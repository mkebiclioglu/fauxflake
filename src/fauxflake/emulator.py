"""Local Snowflake endpoint backed by DuckDB.

The real `dbt-snowflake` adapter connects here over the Snowflake wire protocol
(served by fakesnow, which transpiles Snowflake SQL to DuckDB with sqlglot).
The generator shares the same DuckDB instance in-process, so synthetic source
data is written at native DuckDB speed without going through the protocol.
"""

from __future__ import annotations

import collections
import contextlib
import gzip
import json
import logging
import os
import re
import shutil
import tempfile
import socket
import sys
import threading
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import duckdb
from sqlglot import exp

from . import account, constraints, history, lengths, objects, relations, semantics, staged, zoned

# Statements the emulator cannot run as-is, rewritten before they are parsed.
# Every entry is a known difference from Snowflake: keep the list short and
# documented, because each one is a place where CI and prod are not identical.
REWRITES: list[tuple[re.Pattern[str], str, str]] = [
    (
        # dbt's incremental materializations build `db.schema.model__dbt_tmp` as a
        # TEMPORARY relation. DuckDB only allows temp relations in its `temp`
        # catalog, so they become ordinary relations (dbt replaces them each run).
        re.compile(
            r"\bcreate(\s+or\s+replace)?(\s+secure)?\s+(?:temporary|temp)\s+(table|view)\s+(?=[\w\"$]+\.[\w\"$]+\.[\w\"$]+)",
            re.IGNORECASE,
        ),
        r"create\1\2 \3 ",
        "qualified temporary relation -> regular relation",
    ),
    (
        # Snowpark's `CREATE SCOPED TEMPORARY FILE FORMAT / STAGE / TABLE`: temporary for the session that made it, as
        # SCOPED is for the procedure; here a session's temporary object
        re.compile(r"(?is)^((?:\s|--[^\n]*\n|/\*.*?\*/)*create(?:\s+or\s+replace)?)\s+scoped\s+(temp|temporary)\b"),
        r"\1 \2",
        "SCOPED TEMPORARY -> TEMPORARY",
    ),
    (
        # dbt reads the session's query tag before setting its own (`query_tag` config)
        re.compile(r"(?s)^(?:\s|--[^\n]*\n|/\*.*?\*/)*show\s+parameters\s+like\s+'(\w+)'\s+in\s+session\s*;?(?:\s|/\*.*?\*/)*$", re.IGNORECASE),
        r"""select upper('\1') as "key", '' as "value", '' as "default", 'SESSION' as "level", '' as "description", 'STRING' as "type" """,
        "session parameters are empty",
    ),
    (
        # COPY TAGS keeps the tags of the relation a CREATE OR REPLACE replaces: they are kept here by name (objects.py)
        # whether the clause is there or not, and sqlglot does not read it
        re.compile(r"(?is)^((?:\s|--[^\n]*\n|/\*.*?\*/)*create\b(?:(?!\bas\b).)*?)\bcopy\s+tags\b"),
        r"\1",
        "COPY TAGS: tags are kept by name",
    ),
    (re.compile(r"\bdrop\s+dynamic\s+table\b", re.IGNORECASE), "drop table", "dynamic table -> table"),
    (re.compile(r"\balter\s+dynamic\s+table\s+(\S+)\s+(rename|swap)\b", re.IGNORECASE), r"alter table \1 \2", "dynamic table -> table"),
]


_MERGE = re.compile(r"(?:\s|--[^\n]*\n|/\*.*?\*/)*merge\s+into\b", re.IGNORECASE | re.DOTALL)


def _qualify_merge_insert(sql: str) -> str:
    """Snowflake resolves bare columns in `WHEN NOT MATCHED THEN INSERT ... VALUES (a, b)`
    against the USING source. dbt relies on that; the emulator needs it spelled out."""
    import sqlglot
    from sqlglot import exp

    merge = sqlglot.parse_one(sql, read="snowflake")
    if not isinstance(merge, exp.Merge):
        return sql
    source = merge.args["using"]
    alias = source.alias_or_name
    changed = False
    if isinstance(source, exp.Table) and source.alias and source.alias.upper() != source.name.upper():
        # `using src as s`: the clauses name the source by its alias. A subquery carries one the same way
        plain = source.copy()
        plain.set("alias", None)
        merge.set("using", exp.select("*").from_(plain).subquery(source.alias))
        changed = True
    for when in merge.find_all(exp.When):
        then = when.args.get("then")
        if when.args.get("matched") or not isinstance(then, exp.Insert) or then.expression is None:
            continue
        for column in then.expression.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(alias))
                changed = True
    return merge.sql(dialect="snowflake") if changed else sql


# Statements acknowledged without being executed (transactions are handled in `_transaction` first).
_LEAD = r"(?s)^(\s|--[^\n]*\n|/\*.*?\*/)*"
_TXN_BEGIN = re.compile(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(?:begin(?:\s+(?:work|transaction))?(?:\s+name\s+\S+)?|start\s+transaction(?:\s+name\s+\S+)?)\s*;?\s*$")
_TXN_END = re.compile(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(commit|rollback)(?:\s+work)?\s*;?\s*$")
_TXN_KEEPS = re.compile(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(insert|update|delete|merge|select|with|call|show|desc|describe|set|unset|use|copy|execute|explain|\()\b")
_TWO_DATABASES = "a single transaction can only write to a single attached database"
_NO_SUCH_COLUMN = re.compile(r'Binder Error: (?:Values list "[^"]*" does not have a column named|Referenced column|Table "[^"]*" does not have a column named) "([^"]+)"')
# failures that are not of the data a statement reads (whose answer an empty side of a join could change)
_NOT_DATA_ERRORS = ("Binder Error", "Parser Error", "Catalog Error", "does not exist", "syntax error", "Not implemented",
                    "SQL compilation error", "INTERRUPT", "Interrupt", "timed out", "Duplicate row detected", _TWO_DATABASES)
# DuckDB's word for text that is no number, and Snowflake's (fakesnow answers 100035, Snowflake's for a timestamp)
_NOT_A_TIME = re.compile(r'invalid (date|timestamp) field format: "([^"]*)"')
_NOT_NULL = re.compile(r"NOT NULL constraint failed: (?:[\w$\"]+\.)*?([\w$\"]+)\.([\w$\"]+)")
_MANY_CLAUSES = re.compile(r"Found multiple '(\w+)' clauses\. Line (\d+), Col: (\d+)")
_NOT_A_NUMBER = re.compile(r"Could not convert string (['\"])(.*?)\1 to (?:U?(?:TINY|SMALL|BIG|HUGE)?INT(?:EGER|8|16|32|64|128)?|DECIMAL[^ ]*|DOUBLE|FLOAT)\b", re.S)


def _transaction(cursor, command: str, original):  # noqa: ANN001, ANN202
    """BEGIN, COMMIT and ROLLBACK, as a DuckDB transaction on the session. As in Snowflake, a DDL
    statement commits the transaction that is open. Returns the reply, or None for other statements."""
    conn = cursor._conn
    is_open = getattr(conn, "_fauxflake_txn", False)
    trailing = objects._TRAILING_COMMENT.sub("", command)
    if _TXN_BEGIN.match(trailing):
        if is_open:  # one left open (a failed run that never said ROLLBACK): Snowflake would carry on in it
            _end_transaction(cursor)
        if getattr(conn, "_fauxflake_undo", None) is not None:
            _undo(cursor, restore=False)
        cursor._duck_conn.execute("begin transaction")
        conn._fauxflake_txn, conn._fauxflake_journal = True, []
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    end = _TXN_END.match(trailing)
    if end and getattr(conn, "_fauxflake_undo", None) is not None:  # (a transaction across databases: see _undo)
        _undo(cursor, restore=end.group(1).lower() == "rollback")
        _drop_scratch(cursor)
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    if end:
        _drop_scratch(cursor)
        if is_open:
            conn._fauxflake_txn, conn._fauxflake_journal = False, []
            try:
                cursor._duck_conn.execute(end.group(1))
            except Exception:  # noqa: BLE001 - DuckDB already ended it (a failed statement aborts its transaction)
                if end.group(1).lower() == "commit":
                    raise
        # (after the transaction: the registry is in another database, which a transaction that wrote here cannot touch)
        (relations.committed if end.group(1).lower() == "commit" else relations.rolled_back)(cursor)
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    if is_open and not _TXN_KEEPS.match(trailing):
        _end_transaction(cursor)  # DDL commits
    if getattr(conn, "_fauxflake_undo", None) is not None and not _TXN_KEEPS.match(trailing):
        _undo(cursor, restore=False)
    return None


UNDO = "_fs_global._fx_undo"


def _undo_copy(cursor, tree) -> None:  # noqa: ANN001
    """Before a statement of a transaction that spans databases writes to a table: its rows as they were, once."""
    from . import history

    undo = getattr(cursor._conn, "_fauxflake_undo", None)
    target = history._target(tree) if undo is not None and isinstance(tree, exp.Expression) else None
    if target is None:
        return
    conn = cursor._conn
    fq = ((target.catalog or conn.database or "").upper(), (target.db or conn.schema or "").upper(), target.name.upper())
    if fq in undo:
        return
    source = ".".join('"' + p.replace('"', '""') + '"' for p in fq)
    copy = '"' + ".".join(fq).replace('"', '""') + f'#{len(undo)}"'
    try:
        cursor._duck_conn.execute(f"create schema if not exists {UNDO}")
        cursor._duck_conn.execute(f"create or replace table {UNDO}.{copy} as select * from {source}")
        undo[fq] = copy
    except Exception:  # noqa: BLE001 - not there yet: ROLLBACK drops it
        undo[fq] = None


def _undo(cursor, restore: bool) -> None:  # noqa: ANN001, FBT001
    """The end of a transaction that spans databases: COMMIT keeps what it did, ROLLBACK puts each table back."""
    conn, duck = cursor._conn, cursor._duck_conn
    undo, conn._fauxflake_undo = conn._fauxflake_undo or {}, None
    for fq, copy in undo.items():
        source = ".".join('"' + p.replace('"', '""') + '"' for p in fq)
        try:
            if restore and copy is None:
                duck.execute(f"drop table if exists {source}")
            elif restore:
                exists = duck.execute("select count(*) from duckdb_tables() where upper(database_name) = ? and upper(schema_name) = ? "
                                      "and upper(table_name) = ?", list(fq)).fetchone()[0]
                if exists:
                    duck.execute(f"delete from {source}")
                    duck.execute(f"insert into {source} select * from {UNDO}.{copy}")
                else:
                    duck.execute(f"create table {source} as select * from {UNDO}.{copy}")
            if copy is not None:
                duck.execute(f"drop table if exists {UNDO}.{copy}")
        except Exception as e:  # noqa: BLE001
            print(f"fauxflake: could not put {'.'.join(fq)} back as the transaction found it: {e}", file=sys.stderr)


EMPTY_LOOKUPS: list[str] = []  # INFORMATION_SCHEMA queries that found nothing: what a project's macros looked for
_LOOKUP = re.compile(r"(?i)information_schema\s*\.\s*(tables|columns)\b")


def _note_empty_lookup(cursor, command: str) -> None:  # noqa: ANN001
    """Projects find their inputs by asking the warehouse (`get_relations_by_pattern` and the like).
    When such a query comes back empty, `fauxflake init` wants to know what it asked for."""
    if '"node_id": "model.' not in command or not _LOOKUP.search(command) or len(EMPTY_LOOKUPS) >= 20000:
        return  # dbt's own bookkeeping queries carry no model
    try:
        table = getattr(cursor, "_arrow_table", None)
        if table is not None and table.num_rows == 0 and command not in EMPTY_LOOKUPS:
            EMPTY_LOOKUPS.append(command)
    except Exception:  # noqa: BLE001, S110 - bookkeeping only
        pass


def _drop_scratch(cursor) -> None:  # noqa: ANN001
    scratch = getattr(cursor._conn, "_fauxflake_scratch", None)
    if scratch:
        cursor._conn._fauxflake_scratch = None
        try:
            cursor._duck_conn.execute("drop table if exists " + ".".join(f'"{part}"' for part in scratch))
        except Exception:  # noqa: BLE001, S110 - the transaction is already gone, and the table with it
            pass


def _end_transaction(cursor) -> None:  # noqa: ANN001
    cursor._conn._fauxflake_txn, cursor._conn._fauxflake_journal = False, []
    _drop_scratch(cursor)
    try:
        cursor._duck_conn.execute("commit")
    except Exception:  # noqa: BLE001, S110 - nothing open any more
        pass
    relations.committed(cursor)
NOPS = [
    _LEAD + r"(begin|commit|rollback|start\s+transaction)\b",
    # documentation, access control and warehouse settings have nothing to act on locally
    _LEAD + r"comment\s+(if\s+exists\s+)?on\s",
    _LEAD + r"alter\s+(table|view|materialized\s+view|dynamic\s+table)\s+\S+\s+(alter|modify)\s.*\bcomment\b",
    _LEAD + r"alter\s+\w+(\s+\w+)?\s+(if\s+exists\s+)?\S+\s+(un)?set\s+(tag|comment|data_retention_time_in_days|change_tracking|query_tag)\b",
    _LEAD + r"alter\s+(table|materialized\s+view|dynamic\s+table)\s+\S+\s+(cluster\s+by|drop\s+clustering|resume|suspend)\b",
    _LEAD + r"(grant|revoke)\s",
    _LEAD + r"alter\s+dynamic\s+table\s+\S+\s+(?!rename|swap)",
    _LEAD + r"alter\s+(session|warehouse|account)\s",
    _LEAD + r"use\s+(warehouse|secondary\s+roles)\s",
]


# Simulated "now". When set, Snowflake's clock functions return it, so models
# that filter on current_date see the same point in time as the synthetic data.
FROZEN_NOW = None
_CLOCK = re.compile(
    r"\b(?:current_timestamp|sysdate|getdate|localtimestamp|current_date)\b(?:\s*\(\s*\))?(?!\s*\()", re.IGNORECASE
)


_ZONE_SET = re.compile(r"(?is)^\s*alter\s+session\s+(?:set\s+.*?\btimezone\s*=\s*'([^']*)'|unset\s+.*?\btimezone\b)")
_ZONES: dict[int, str] = {}  # DuckDB connection -> the TimeZone it was last set to


_USE_WAREHOUSE = re.compile(r'(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*use\s+warehouse\s+("(?:[^"]|"")+"|[\w$]+)\s*;?\s*(?:/\*.*\*/\s*)?$')


def _session_warehouse(cursor, command, original):  # noqa: ANN001, ANN202
    """USE WAREHOUSE: the session's, which CURRENT_WAREHOUSE() gives (dbt switches to a model's `snowflake_warehouse` so).
    Returns the reply to a USE, None otherwise."""
    m = _USE_WAREHOUSE.match(command) if isinstance(command, str) else None
    if m:
        name = m.group(1)
        account.use_warehouse(cursor, name[1:-1].replace('""', '"') if name.startswith('"') else name.upper())
    semantics.SESSION["CurrentWarehouse"] = getattr(cursor._conn, "_fx_warehouse", None) or "LOCAL"
    return original(cursor, "select 'Statement executed successfully.' as \"status\"") if m else None


_STATS_VERB = re.compile(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(insert|update|delete|merge|truncate|create)\b")
_CTAS = re.compile(r'(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*create\s+(?:or\s+replace\s+)?(?:(?:local|global|transient|temporary|temp)\s+)*table\s+'
                   r'(?:if\s+not\s+exists\s+)?((?:"(?:[^"]|"")+"|[\w$]+)(?:\.(?:"(?:[^"]|"")+"|[\w$]+)){0,2})(?:(?!\bas\b).)*\bas\b')
_TRUNCATE = re.compile(r'(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*truncate\s+(?:table\s+)?(?:if\s+exists\s+)?((?:"(?:[^"]|"")+"|[\w$]+)(?:\.(?:"(?:[^"]|"")+"|[\w$]+)){0,2})')


def _rows_now(cursor, name: str) -> int:  # noqa: ANN001
    try:
        return cursor._duck_conn.execute(f"select count(*) from {relations._sql_name(relations.qualified(cursor, name))}").fetchone()[0]
    except Exception:  # noqa: BLE001
        return 0


def _note_stats(cursor, command, truncated: int | None) -> None:  # noqa: ANN001
    """What a DML statement did, as Snowflake reports it with the result (dbt reads rows_inserted and the like from it;
    measured: an INSERT, UPDATE, DELETE, MERGE, a CREATE TABLE AS (its rows inserted) and a TRUNCATE (its rows deleted))."""
    conn = cursor._conn
    conn._fx_stats = None
    m = _STATS_VERB.match(command) if isinstance(command, str) else None
    if not m:
        return
    verb = m.group(1).lower()
    stats = {"numRowsInserted": 0, "numRowsUpdated": 0, "numRowsDeleted": 0, "numDmlDuplicates": 0}
    if verb == "create":
        made = _CTAS.match(command)
        if not made:
            return
        stats["numRowsInserted"] = _rows_now(cursor, made.group(1))
    elif verb == "truncate":
        stats["numRowsDeleted"] = truncated or 0
    else:
        table = getattr(cursor, "_arrow_table", None)
        if table is None or table.num_rows == 0:
            return
        for name in table.column_names:
            key = {"number of rows inserted": "numRowsInserted", "number of rows updated": "numRowsUpdated",
                   "number of rows deleted": "numRowsDeleted"}.get(name.lower())
            if key:
                stats[key] += sum(int(v or 0) for v in table.column(name).to_pylist())
    conn._fx_stats = stats


def _session_zone(cursor, command) -> None:  # noqa: ANN001
    """ALTER SESSION SET TIMEZONE: the session's clock, for TIMESTAMP_LTZ and for a TIMESTAMP_TZ made without an offset.
    Kept on the connection and given to DuckDB before each of its statements."""
    if isinstance(command, str):
        m = _ZONE_SET.match(command)
        if m:
            zone = m.group(1) or "UTC"
            try:
                cursor._duck_conn.execute("select current_setting('TimeZone')")  # (still usable)
                cursor._duck_conn.execute(f"set TimeZone = '{zone}'")
            except Exception as e:  # noqa: BLE001
                from snowflake.connector.errors import ProgrammingError
                raise ProgrammingError(msg=f"Invalid value [{zone}] for parameter 'TIMEZONE'", errno=1008, sqlstate="22023") from e
            cursor._conn._fx_timezone = zone
    zone = getattr(cursor._conn, "_fx_timezone", "UTC")
    semantics.SESSION_ZONE[0] = zone
    if _ZONES.get(id(cursor._duck_conn), "UTC") != zone:
        try:
            cursor._duck_conn.execute(f"set TimeZone = '{zone}'")
            _ZONES[id(cursor._duck_conn)] = zone
        except Exception:  # noqa: BLE001, S110
            pass


def _geo_out(table):  # noqa: ANN001, ANN202
    """A GEOGRAPHY or GEOMETRY (geo.py: a struct holding the document) as Snowflake sends one: GeoJSON text."""
    import pyarrow as pa

    from . import geo

    for i, field in enumerate(table.schema):
        if pa.types.is_struct(field.type) and {f.name for f in field.type} in ({"G", "GEOGRAPHY"}, {"G", "GEOMETRY"}):
            shown = pa.array([None if v is None or v.get("G") is None else geo.shown(v["G"]) for v in table.column(i).to_pylist()], pa.string())
            table = table.set_column(i, pa.field(field.name, pa.string()), shown)
    return table


def _local_out(table):  # noqa: ANN001, ANN202
    """Outside UTC, each TIMESTAMP_LTZ as Snowflake's client shows it: the instant at the session's offset."""
    import pyarrow as pa
    import pyarrow.compute as pc

    zone = semantics.SESSION_ZONE[0]
    if zone.upper() in ("UTC", "ETC/UTC", "GMT", "Z"):
        return table
    for i, field in enumerate(table.schema):
        if not (pa.types.is_timestamp(field.type) and field.type.tz):
            continue
        col = table.column(i).combine_chunks()
        utc = pc.cast(col, pa.timestamp("us", tz="UTC"))
        local = pc.local_timestamp(pc.cast(col, pa.timestamp("us", tz=zone)))
        minutes = pc.divide(pc.subtract(local.cast(pa.int64()), utc.cast(pa.timestamp("us")).cast(pa.int64())), 60_000_000)
        seconds = pc.floor_temporal(utc, unit="second")
        epoch = pc.divide(seconds.cast(pa.int64()), 1_000_000)
        fraction = pc.multiply(pc.subtract(utc.cast(pa.int64()), seconds.cast(pa.int64())), 1000).cast(pa.int32())
        offset = pc.add(minutes.cast(pa.int32()), pa.scalar(1440, pa.int32()))
        value = pa.StructArray.from_arrays(
            [pc.fill_null(epoch, 0), pc.fill_null(fraction, 0), pc.fill_null(offset, 1440)],
            fields=[pa.field("epoch", pa.int64(), nullable=False), pa.field("fraction", pa.int32(), nullable=False),
                    pa.field("timezone", pa.int32(), nullable=False)], mask=pc.is_null(col))
        table = table.set_column(i, pa.field(field.name, value.type), value)
    return table


def _zoned_out(table):  # noqa: ANN001, ANN202
    """Each (instant, offset) column as Snowflake sends a TIMESTAMP_TZ: seconds, nanoseconds, and the offset in
    minutes plus 1440."""
    import pyarrow as pa
    import pyarrow.compute as pc

    for i, field in enumerate(table.schema):
        t = field.type
        if not (pa.types.is_struct(t) and t.num_fields == 2 and t.field(0).name.lower() == "i" and t.field(1).name.lower() == "o"
                and pa.types.is_timestamp(t.field(0).type)):
            continue
        col = table.column(i).combine_chunks()
        instant = col.field(0)
        seconds = pc.floor_temporal(instant, unit="second")
        epoch = pc.divide(seconds.cast(pa.int64()), 1_000_000)
        fraction = pc.multiply(pc.subtract(instant.cast(pa.int64()), seconds.cast(pa.int64())), 1000).cast(pa.int32())
        offset = pc.add(col.field(1).cast(pa.int32()), pa.scalar(1440, pa.int32()))
        mask = pc.is_null(col)
        value = pa.StructArray.from_arrays(
            [pc.fill_null(epoch, 0), pc.fill_null(fraction, 0), pc.fill_null(offset, 1440)],
            fields=[pa.field("epoch", pa.int64(), nullable=False), pa.field("fraction", pa.int32(), nullable=False),
                    pa.field("timezone", pa.int32(), nullable=False)], mask=mask)
        table = table.set_column(i, pa.field(field.name, value.type), value)
    return table


_QUOTED_TEXT = re.compile(r"""('(?:[^'\\]|''|\\.)*'|"(?:[^"]|"")*"|\$\$.*?\$\$)""", re.DOTALL)


def _freeze_clock(sql: str) -> str:
    def sub(m: re.Match[str]) -> str:
        if m.group(0).lower().startswith("current_date"):  # (the date in the session's time zone)
            return f"cast('{FROZEN_NOW:%Y-%m-%d %H:%M:%S} +00:00'::timestamp_ltz as date)"
        if m.group(0).lower().startswith("sysdate"):
            return f"'{FROZEN_NOW:%Y-%m-%d %H:%M:%S}'::timestamp"  # (SYSDATE is a TIMESTAMP_NTZ, in UTC)
        # CURRENT_TIMESTAMP() is an instant (TIMESTAMP_LTZ), as the live clock: CONVERT_TIMEZONE('America/Los_Angeles',
        # CURRENT_TIMESTAMP()) gives that zone's wall clock, not the frozen time read as one of that zone's
        return f"'{FROZEN_NOW:%Y-%m-%d %H:%M:%S} +00:00'::timestamp_ltz"

    if "'" not in sql and '"' not in sql and "$$" not in sql:
        return _CLOCK.sub(sub, sql)
    # only outside string literals, quoted names and $$ bodies: 'macro.dbt.current_timestamp' is text
    parts = _QUOTED_TEXT.split(sql)
    return "".join(part if i % 2 else _CLOCK.sub(sub, part) for i, part in enumerate(parts))


MAX_STATEMENT = 1_048_576  # Snowflake accepts 1 MB of SQL text


_ROWS_WRITTEN_OUT = re.compile(r"\s*(?:/\*.*?\*/\s*|--[^\n]*\n\s*)*insert\s+(?:overwrite\s+)?into\s", re.I | re.S)
# (dbt writes a comment first, and reloads a seed that is already there with INSERT OVERWRITE)


_LEADING = r"(?s)^(?:\s|--[^\n]*\n|/\*.*?\*/)*"
_TRAILING = r"\s*;?(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*$"  # (dbt appends its query comment)
_TAG_SET = re.compile(_LEADING + r"alter\s+session\s+set\s+query_tag\s*=\s*'((?:[^'\\]|''|\\.)*)'" + _TRAILING, re.IGNORECASE)
_TAG_UNSET = re.compile(_LEADING + r"alter\s+session\s+unset\s+query_tag" + _TRAILING, re.IGNORECASE)
_TAG_SHOWN = re.compile(_LEADING + r"show\s+parameters\s+like\s+'query_tag'\s+in\s+session\s*;?(?:\s|/\*.*?\*/)*$", re.IGNORECASE)
_ESCAPES = {"\\": "\\", "'": "'", '"': '"', "n": "\n", "t": "\t", "r": "\r", "0": "\0"}


_SESSION_SET = re.compile(r"(?is)^\s*alter\s+session\s+set\s+(.+?)\s*;?\s*$")
_SESSION_UNSET = re.compile(r"(?is)^\s*alter\s+session\s+unset\s+(.+?)\s*;?\s*$")
_SETTING = re.compile(r"""(?is)\s*([\w$]+)\s*=\s*('(?:[^'\\]|''|\\.)*'|"[^"]*"|[^,\s]+)\s*(?:,|$)""")


def session_value(value) -> str:  # noqa: ANN001
    """A parameter's value as SHOW PARAMETERS gives it: text without its quotes, TRUE as true."""
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip()
    if len(text) >= 2 and text[0] == text[-1] == "'":
        return re.sub(r"''|\\(.)", lambda e: "'" if e.group(0) == "''" else _ESCAPES.get(e.group(1), e.group(1)), text[1:-1])
    return text.lower() if text.lower() in ("true", "false") else text


def _session_parameters(cursor, command) -> None:  # noqa: ANN001
    """ALTER SESSION SET / UNSET: the values SHOW PARAMETERS ... IN SESSION gives back at level SESSION (measured). The
    statement itself still runs: TIMEZONE and the rest act through their own handlers."""
    if not isinstance(command, str):
        return
    params = cursor._conn.__dict__.setdefault("_fx_session_params", {})
    if (m := _SESSION_SET.match(command)) is not None:
        for key, value in _SETTING.findall(m.group(1)):
            params[key.upper()] = session_value(value)
    elif (m := _SESSION_UNSET.match(command)) is not None:
        for key in re.split(r"\s*,\s*", m.group(1)):
            params.pop(key.strip().upper(), None)


def _query_tag(cursor, command: str, original):  # noqa: ANN001, ANN202
    """The session's QUERY_TAG, kept as Snowflake keeps it: `show parameters` reads it back (dbt and projects nest
    the tag they find in their own), and one longer than 2000 characters is refused. None for any other statement."""
    text = str(command)
    if (m := _TAG_SET.match(text)) is not None:
        value = re.sub(r"''|\\(.)", lambda e: "'" if e.group(0) == "''" else _ESCAPES.get(e.group(1), e.group(1)), m.group(1))
        if len(value) > 2000:
            from snowflake.connector.errors import ProgrammingError

            raise ProgrammingError(msg=f"SQL compilation error:\ninvalid value [{value}] for parameter 'QUERY_TAG'", errno=1008, sqlstate="22023")
        cursor._conn._fx_query_tag = value
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    if _TAG_UNSET.match(text):
        cursor._conn._fx_query_tag = ""
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    if _TAG_SHOWN.match(text):
        value = getattr(cursor._conn, "_fx_query_tag", "").replace("'", "''")
        return original(cursor, f"""select 'QUERY_TAG' as "key", '{value}' as "value", '' as "default", 'SESSION' as "level", """
                                """'' as "description", 'STRING' as "type" """)
    return None


def rewrite(sql: str) -> str:
    # a seed's rows arrive written out in the INSERT: data, which real projects load in batches larger than this
    if len(sql) > MAX_STATEMENT and not _ROWS_WRITTEN_OUT.match(sql):
        from snowflake.connector.errors import ProgrammingError

        # a pivot with one column per row of made-up text, usually: refused at once, as Snowflake would,
        # rather than parsed for a quarter of an hour
        raise ProgrammingError(msg=f"SQL statement is {len(sql) / 1_048_576:.1f} MB; Snowflake accepts at most 1 MB of SQL text.",
                               errno=1003, sqlstate="42000")
    frozen = _freeze_clock(sql) if FROZEN_NOW is not None else sql
    done = _rewritten(frozen)
    if frozen != sql:
        # `select current_date` is still named CURRENT_DATE, not after the frozen time put in its place
        if len(_NAMED_AS) > 64:
            _NAMED_AS.clear()
        _NAMED_AS[done] = _rewritten(sql)
    return done


_NAMED_AS: dict[str, str] = {}  # statement with the clock frozen -> the same statement as written, for its column names


def _rewritten(sql: str) -> str:
    for pattern, replacement, _why in REWRITES:
        sql = pattern.sub(replacement, sql, count=1)
    if _MERGE.match(sql):
        sql = _qualify_merge_insert(sql)
    return sql


# `create temporary table db.schema.name`: an ordinary relation here (see REWRITES), that goes when the
# session that made it ends, and that another session does not see: its own takes the place
_TEMP_RELATION = re.compile(
    r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*create(?:\s+or\s+replace)?(?:\s+secure)?\s+(?:temporary|temp)\s+(?P<kind>table|view)\s+"
    r"(?:if\s+not\s+exists\s+)?(?P<name>(?:[\w$]+|\"[^\"]+\")(?:\.(?:[\w$]+|\"[^\"]+\")){2})")
TEMPORARY: dict[str, tuple[int, str, str]] = {}  # DB.SCHEMA.NAME -> (session, table | view, the name as written)


def _temporary(cursor, command: str) -> str | None:  # noqa: ANN001
    made = _TEMP_RELATION.match(command)
    if made is None:
        return None
    key = made.group("name").replace('"', "").upper()
    held = TEMPORARY.get(key)
    if held and held[0] != id(cursor._conn):
        _drop_temporary(cursor._duck_conn, key)
    return key


def _drop_temporary(duck, key: str) -> None:  # noqa: ANN001
    _, kind, name = TEMPORARY.pop(key)
    try:
        duck.execute(f"drop {kind} if exists {name}")
    except Exception:  # noqa: BLE001, S110 - replaced by something else since
        pass


def end_session(conn=None, duck=None) -> None:  # noqa: ANN001
    """Drop the temporary relations of one session, or of all of them. A session that ends rolls back the transaction
    it left open, as in Snowflake (dbt does not end a transaction whose statement failed): the temporary relations are
    dropped after that, for good, and nothing the session began is left for other sessions to collide with."""
    if conn is not None:
        if getattr(conn, "_fauxflake_undo", None) is not None:
            with contextlib.suppress(Exception):
                _undo(conn.cursor(), restore=True)
        conn._fauxflake_txn, conn._fauxflake_journal = False, []
        with contextlib.suppress(Exception):
            conn._duck_conn.execute("rollback")
    for key in [k for k, held in TEMPORARY.items() if conn is None or held[0] == id(conn)]:
        _drop_temporary(duck if duck is not None else conn._duck_conn, key)


_ALIAS_ERROR = re.compile(r'Alias (?:with name )?"+([^"]+)"+ (?:exists, but aliases cannot be used|referenced(?: in a SELECT clause)? - but the expression has (?:side effects|a subquery))')
_MISSING_COLUMN = re.compile(r'Referenced column "([^"]+)" not found in FROM clause')
_AMBIGUOUS_ERROR = re.compile(r"""Ambiguous reference to column name "+([^"]+)"+ \(use: '"?([^'".]+)"?\.""")
_patched = False
FAILED_STATEMENTS: dict[str, str] = {}  # error -> the last statement that raised it (debug mode only)

# Relations that stand for what production holds before the run (the spec's `existing:`). Queries read
# them; dbt does not see them, so a model whose own relation has a stand-in is built as on a first run
# (an incremental model that found an empty table would filter on MAX() of nothing and keep no rows).
STANDINS: set[str] = set()
_STANDINS_TABLE = "_fs_global.main._fx_standins"


_SEMI_TABLE = "_fs_global.main._fx_semi"  # (c, s, t, col, kind): the columns that are ARRAY or OBJECT rather than VARIANT


def _describe_semi_types(duck) -> None:  # noqa: ANN001
    """INFORMATION_SCHEMA.COLUMNS, DESCRIBE and SHOW COLUMNS say ARRAY and OBJECT where Snowflake would."""
    from fakesnow import info_schema

    duck.execute(f"create or replace table {_SEMI_TABLE} (c varchar, s varchar, t varchar, col varchar, kind varchar)")
    view = info_schema.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW
    # a column's collation as its Snowflake spec (semantics.collation_spec: a Python function, so the view is made again
    # here, once it is registered)
    view = view.replace("COLLATION_NAME, IS_IDENTITY", "case when columns.data_type = 'VARCHAR' then _fxpy_collation_spec("
                        "(select t.sql from duckdb_tables() t where t.database_name = columns.table_catalog and t.schema_name = "
                        "columns.table_schema and t.table_name = columns.table_name), columns.column_name) end as COLLATION_NAME, IS_IDENTITY")
    typed = view.replace("when columns.data_type='JSON' then 'VARIANT'", "when columns.data_type='JSON' then coalesce(fxk.kind, 'VARIANT')")
    joined = typed.replace("where schema_name != '_fs_information_schema'",
                           f"LEFT JOIN {_SEMI_TABLE} fxk ON fxk.c = columns.table_catalog AND fxk.s = columns.table_schema "
                           "AND fxk.t = columns.table_name AND fxk.col = columns.column_name\n"
                           "where schema_name != '_fs_information_schema'")
    if typed == view or joined == typed:
        return  # another fakesnow than the one this was written against: columns stay VARIANT
    duck.execute(joined.replace("create view if not exists", "create or replace view"))


def _target(cursor, tree: exp.Create) -> tuple[str, str, str] | None:  # noqa: ANN001
    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
    if not isinstance(target, exp.Table) or str(tree.args.get("kind") or "").upper() not in ("TABLE", "VIEW"):
        return None
    conn = cursor._conn
    return (target.catalog or conn.database or "").upper(), (target.db or conn.schema or "").upper(), target.name.upper()


def _note_semi_types(cursor, tree: exp.Create) -> None:  # noqa: ANN001
    """Before the statement is rewritten for DuckDB: which of the columns it makes are ARRAY or OBJECT."""
    cursor._fauxflake_semi = None
    where = _target(cursor, tree)
    if where is None:
        return
    duck, conn = cursor._duck_conn, cursor._conn

    def known(table: exp.Table) -> dict[str, str]:
        place = [(table.catalog or conn.database or "").upper(), (table.db or conn.schema or "").upper(), table.name.upper()]
        return dict(duck.execute(f"select col, kind from {_SEMI_TABLE} where c = ? and s = ? and t = ?", place).fetchall())

    try:
        # a copy: the statement is rewritten in place afterwards, and the columns are asked for once it has run
        cursor._fauxflake_semi = (where, semantics.semi_kinds(tree.copy(), known))
    except Exception:  # noqa: BLE001 - typing is a description, never a reason to fail the statement
        cursor._fauxflake_semi = (where, {})


_RESULT = type("Result", (), {"kinds": {}})()  # what the statement last answered says of its columns (shared across the server's threads): {NAME: 'ARRAY' | 'OBJECT'}


def _note_result_types(cursor, tree: exp.Expression) -> None:  # noqa: ANN001
    """A query's ARRAY and OBJECT columns, described to the client as such (Snowflake's type codes 10 and 9)."""
    if not isinstance(tree, exp.Query):
        return  # (the DESCRIBE of the statement answered runs through here too: it keeps what that one said)
    duck, conn = cursor._duck_conn, cursor._conn

    def known(table: exp.Table) -> dict[str, str]:
        place = [(table.catalog or conn.database or "").upper(), (table.db or conn.schema or "").upper(), table.name.upper()]
        return dict(duck.execute(f"select col, kind from {_SEMI_TABLE} where c = ? and s = ? and t = ?", place).fetchall())

    try:
        _RESULT.kinds = semantics.semi_kinds(exp.Create(this=exp.to_table("_fx_result"), kind="TABLE", expression=tree.copy()), known)
    except Exception:  # noqa: BLE001 - a description, never a reason to fail the statement
        _RESULT.kinds = {}


def _record_semi_types(cursor) -> None:  # noqa: ANN001
    noted = getattr(cursor, "_fauxflake_semi", None)
    if not noted:
        return
    cursor._fauxflake_semi = None
    (db, schema, name), kinds = noted
    duck = cursor._duck_conn
    try:
        duck.execute(f"delete from {_SEMI_TABLE} where c = ? and s = ? and t = ?", [db, schema, name])
        columns = [r[0] for r in duck.execute("select column_name from information_schema.columns where table_catalog = ? and "
                                              "table_schema = ? and table_name = ? and data_type = 'JSON'", [db, schema, name]).fetchall()]
        rows = [(db, schema, name, col, kind) for col in columns for kind in [kinds.get(col.upper()) or (
            kinds[col.upper()] if not isinstance(kinds, dict) or type(kinds) is not dict else None)] if kind]
        if rows:
            duck.executemany(f"insert into {_SEMI_TABLE} values (?, ?, ?, ?, ?)", rows)
    except Exception:  # noqa: BLE001, S110
        pass


KEPT: dict[str, dict[str, str]] = {}  # relation -> the columns (and types) it keeps whatever builds it


def stand_in(duck, name: str, statement: str, keeps: dict[str, str] | None = None) -> None:  # noqa: ANN001
    """Create a stand-in with `statement` and keep it out of SHOW OBJECTS / TABLES / VIEWS. `keeps`: columns the
    relation has in production from an earlier life (a snapshot whose model stopped writing them); the project's
    own build of it gets them back, empty, since dbt cannot see the stand-in and builds the relation anew."""
    duck.execute(statement)
    name = name.replace('"', "").upper()
    if keeps:
        KEPT[name] = dict(keeps)
    else:
        KEPT.pop(name, None)
    if name not in STANDINS:
        STANDINS.add(name)
        duck.execute(f"insert into {_STANDINS_TABLE} values (?)", [name])


def _hide_stand_ins(duck) -> None:  # noqa: ANN001
    from fakesnow.transforms import show

    STANDINS.clear()
    duck.execute(f"create or replace table {_STANDINS_TABLE} (name varchar)")
    relations.ensure(duck)
    for view in (relations.SHOW_OBJECTS, relations.SHOW_TABLES, relations.SHOW_VIEWS):  # (they leave stand-ins out themselves)
        duck.execute(view)


def _over_a_stand_in(cursor, tree: exp.Create):  # noqa: ANN001, ANN202
    """dbt builds a relation that has a stand-in: the stand-in makes way. Returns how to put it back
    should the build fail (the models that read it by name still need something there)."""
    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
    kind = str(tree.args.get("kind") or "").upper()
    if not isinstance(target, exp.Table) or kind not in ("TABLE", "VIEW"):
        return None
    conn = cursor._conn
    db, schema, name = (target.catalog or conn.database or "").upper(), (target.db or conn.schema or "").upper(), target.name.upper()
    fq = f"{db}.{schema}.{name}"
    if fq not in STANDINS:
        return None
    duck = cursor._duck_conn
    there = duck.execute("select table_type from information_schema.tables where table_catalog = ? and table_schema = ? "
                         "and table_name = ?", [db, schema, name]).fetchone()
    restore = None
    reads_itself = any(t is not target and t.name.upper() == name and (t.db or conn.schema or "").upper() == schema
                       and (t.catalog or conn.database or "").upper() == db for t in tree.find_all(exp.Table))
    if there and there[0] == "VIEW" and kind == "TABLE" and tree.args.get("replace") and reads_itself:
        # `create or replace table x as select ... from x`: a model that reads what its last run left. The empty
        # stand-in becomes an empty table of the same shape, which the statement reads and then replaces.
        columns = duck.execute(f'describe "{db}"."{schema}"."{name}"').fetchall()
        shape = ", ".join(f'null::{c[1]} as "{c[0]}"' for c in columns)
        restore = f'create view if not exists "{db}"."{schema}"."{name}" as select {shape} where false'
        duck.execute(f'drop view "{db}"."{schema}"."{name}"')
        duck.execute(f'create table "{db}"."{schema}"."{name}" as select {shape} where false')
        return fq, restore
    if there and (("VIEW" if there[0] == "VIEW" else "TABLE") != kind or not tree.args.get("replace")):
        was = "view" if there[0] == "VIEW" else "table"
        columns = duck.execute(f'describe "{db}"."{schema}"."{name}"').fetchall()
        shape = ", ".join(f'null::{c[1]} as "{c[0]}"' for c in columns)
        restore = f'create view if not exists "{db}"."{schema}"."{name}" as select {shape} where false'
        duck.execute(f'drop {was} "{db}"."{schema}"."{name}"')
    return fq, restore


def _variant_takes_text(cursor, tree: exp.Expression) -> None:  # noqa: ANN001
    """INSERT or MERGE from one table into another: where the target's column is a VARIANT and the source's is text,
    Snowflake stores the text as a string value. DuckDB would parse it as JSON and refuse `abc`. The source (dbt's
    temporary table, in an incremental model) has that column turned into JSON strings first."""
    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
    source = tree.args.get("using") if isinstance(tree, exp.Merge) else tree.expression
    if isinstance(tree, exp.Update):  # `update t set ... from candidates`: a MERGE's matched rows
        source = tree.args.get("from_").this if tree.args.get("from_") is not None else None
    if isinstance(source, (exp.Subquery, exp.Select)):  # `(select ... from the_one_table)`
        inner = source.unnest()
        tables = list(inner.find_all(exp.Table)) if isinstance(inner, exp.Select) else []
        source = tables[0] if len(tables) == 1 and not inner.args.get("joins") else None
    if not isinstance(target, exp.Table) or not isinstance(source, exp.Table):
        return
    conn, duck = cursor._conn, cursor._duck_conn

    def columns(table: exp.Table) -> tuple[str, dict[str, str]]:
        """Where the table is (a temporary one lives in DuckDB's `temp` catalog) and its columns' types."""
        named = [(table.catalog or conn.database or "").upper(), (table.db or conn.schema or "").upper(), table.name.upper()]
        for where in (named, ["TEMP", "MAIN", named[2]]):
            found = duck.execute("select table_catalog, table_schema, column_name, data_type from information_schema.columns "
                                 "where upper(table_catalog) = ? and upper(table_schema) = ? and upper(table_name) = ?", where).fetchall()
            if found:
                return f'"{found[0][0]}"."{found[0][1]}"."{named[2]}"', {row[2].upper(): row[3] for row in found}
        return "", {}

    try:
        (_, wanted), (place, given) = columns(target), columns(source)
        for name in [n for n, kind in wanted.items() if kind == "JSON" and given.get(n) == "VARCHAR"]:
            duck.execute(f'alter table {place} alter column "{name}" type json using to_json("{name}")')
    except Exception:  # noqa: BLE001, S110 - the statement then says what is wrong, as before
        pass


def _built(cursor, fq: str) -> None:  # noqa: ANN001
    STANDINS.discard(fq)
    cursor._duck_conn.execute(f"delete from {_STANDINS_TABLE} where name = ?", [fq])
    for column, kind in KEPT.get(fq, {}).items():
        with contextlib.suppress(duckdb.Error):  # (a view, or the column is written again: nothing to add)
            cursor._duck_conn.execute('alter table ' + ".".join(f'"{part}"' for part in fq.split(".")) +
                                      f' add column if not exists "{column}" {kind}')


ADJUSTED: dict[str, tuple[str, exp.Expression]] = {}  # a statement as written -> (the types it read, how it was adjusted)


def _types_read(cursor, tree: exp.Expression) -> str:  # noqa: ANN001
    """The columns and types of every relation a statement names, as one text: what its adjustments depend on."""
    names = sorted({t.name.upper() for t in tree.find_all(exp.Table) if t.name})
    if not names:
        return ""
    listed = ", ".join("'" + n.replace("'", "''") + "'" for n in names)
    rows = cursor._duck_conn.execute(
        "select database_name, schema_name, table_name, column_name, data_type from duckdb_columns() "
        f"where upper(table_name) in ({listed}) order by all").fetchall()
    return str(hash(tuple(rows)))


def run_guarded(cursor, work, limit: float | None = None) -> None:  # noqa: ANN001
    """Run one statement with a time limit, as STATEMENT_TIMEOUT_IN_SECONDS does on Snowflake.

    A query can run away on invented data (a date spine between two random dates), and DuckDB's
    planner can take unbounded time on a few shapes. Execution is interrupted; planning cannot be,
    so a statement that will not stop is left behind on its own DuckDB connection and the session
    carries on with a fresh one."""
    from snowflake.connector.errors import ProgrammingError

    limit = float(os.environ.get("FAUXFLAKE_STATEMENT_TIMEOUT", "1800")) if limit is None else limit
    outcome: dict = {}

    def target() -> None:
        try:
            work()
        except BaseException as e:  # noqa: BLE001 - re-raised in the calling thread
            outcome["error"] = e

    worker = threading.Thread(target=target, name="fauxflake-statement", daemon=True)
    worker.start()
    # (a statement that multiplies rows can also fill the disk well inside its time: it is stopped while there is
    # still room to work in, whatever it is writing)
    began, full, room = time.monotonic(), False, None
    while worker.is_alive() and time.monotonic() - began < limit:
        worker.join(min(2.0, max(0.05, limit - (time.monotonic() - began))))
        if worker.is_alive() and time.monotonic() - began > 5:
            try:
                where = getattr(CURRENT, "db_path", None) or tempfile.gettempdir()
                free = shutil.disk_usage(where).free
                room = free if room is None else room  # (what was free when the statement was first looked at)
                # (short of room, and this statement is what is taking it: one that merely runs on a full disk goes on)
                full = free < float(os.environ.get("FAUXFLAKE_MIN_FREE_GB", "3")) * 2**30 and (
                    room - free > 2**29 or free < 2**29 or "FAUXFLAKE_MIN_FREE_GB" in os.environ)
            except Exception:  # noqa: BLE001
                full = False
            if full:
                break
    if worker.is_alive():
        duck = cursor._duck_conn
        duck.interrupt()
        worker.join(10)
        if worker.is_alive() and CURRENT is not None:
            fresh = CURRENT.duck.cursor()
            conn = cursor._conn
            if conn.database and conn.schema:
                fresh.execute(f"SET schema='{conn.database}.{conn.schema}'")
            conn._duck_conn = cursor._duck_conn = fresh
        if full:
            raise ProgrammingError(
                msg=f"fauxflake emulator: statement stopped after {time.monotonic() - began:.0f}s: the disk is nearly full "
                    "(FAUXFLAKE_MIN_FREE_GB). On generated data this is usually a join or a date spine that explodes; "
                    "look at the values the model reads.", errno=604, sqlstate="57014")
        raise ProgrammingError(
            msg=f"fauxflake emulator: statement stopped after {limit:.0f}s (FAUXFLAKE_STATEMENT_TIMEOUT). On generated data "
                "this is usually a join or a date spine that explodes; look at the values the model reads.",
            errno=604, sqlstate="57014")
    if "error" in outcome:
        raise outcome["error"]


def _merge_check(cursor, create: exp.Create) -> tuple[list[int], list[int]] | None:  # noqa: ANN001
    """The table of a MERGE's candidates, about to be made: it also gets the target's row id, and the WHEN MATCHED
    clauses that update and that delete are read off the MERGE as written (by their place, as fakesnow numbers them)."""
    command = getattr(cursor, "_fx_command", None)
    select = create.expression if isinstance(create.expression, exp.Select) else None
    if not command or select is None or select.args.get("from_") is None:
        return None
    import sqlglot

    try:
        merge = sqlglot.parse_one(command, read="snowflake")
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(merge, exp.Merge):
        return None
    updates, deletes = [], []
    for i, when in enumerate(merge.args.get("whens") or []):
        if isinstance(when, exp.When) and when.args.get("matched"):
            then = when.args.get("then")
            (updates if isinstance(then, exp.Update) else deletes).append(i)
    if not updates:
        return None
    target = select.args["from_"].this
    select.append("expressions", exp.alias_(exp.column("rowid", table=target.alias_or_name), "_FX_TARGET_ROW"))
    return updates, deletes


def _refuse_nondeterministic_merge(cursor, ops: tuple[list[int], list[int]], table: str) -> None:  # noqa: ANN001
    """Snowflake refuses a MERGE that would update one target row from two source rows, or update and delete it
    (ERROR_ON_NONDETERMINISTIC_MERGE, on by default). DuckDB takes one of them."""
    updates, deletes = ops
    listed = lambda xs: ", ".join(str(x) for x in xs) or "-1"  # noqa: E731
    (twice,) = cursor._duck_conn.execute(
        f"select count(*) from (select _FX_TARGET_ROW, count(*) filter (where MERGE_OP in ({listed(updates)})) as u, "
        f"count(*) filter (where MERGE_OP in ({listed(deletes)})) as d from {table} where _FX_TARGET_ROW is not null "
        "group by 1) where u > 1 or (u >= 1 and d >= 1)").fetchone()
    if twice:
        from snowflake.connector.errors import ProgrammingError

        raise ProgrammingError(msg="Duplicate row detected during DML action\nRow Values: [redacted]", errno=100090, sqlstate="42P18")


_TO_TIMESTAMP = """
CREATE OR REPLACE MACRO ${catalog}._fs_to_timestamp(val, scale) AS (
    CASE
        -- a number is seconds since 1970 (or the unit `scale` says), its fraction kept
        WHEN typeof(val) LIKE 'DECIMAL%' OR typeof(val) IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT',
                                                            'USMALLINT', 'UINTEGER', 'UBIGINT', 'DOUBLE', 'FLOAT')
            THEN make_timestamp(round(try_cast(val AS DECIMAL(38, 9)) * power(10, 6 - scale)::DECIMAL(38, 9))::BIGINT)
        -- text of digits: seconds, milliseconds, microseconds or nanoseconds, by its size (unless `scale` says)
        WHEN regexp_full_match(trim(val::VARCHAR), '-?[0-9]+')
            THEN make_timestamp((CASE
                WHEN scale <> 0 THEN try_cast(trim(val::VARCHAR) AS HUGEINT) * power(10, 6 - scale)::DECIMAL(38, 9)
                WHEN abs(try_cast(trim(val::VARCHAR) AS HUGEINT)) < 31536000000 THEN try_cast(trim(val::VARCHAR) AS HUGEINT) * 1000000
                WHEN abs(try_cast(trim(val::VARCHAR) AS HUGEINT)) < 31536000000000 THEN try_cast(trim(val::VARCHAR) AS HUGEINT) * 1000
                WHEN abs(try_cast(trim(val::VARCHAR) AS HUGEINT)) < 31536000000000000 THEN try_cast(trim(val::VARCHAR) AS HUGEINT)
                ELSE try_cast(trim(val::VARCHAR) AS HUGEINT) // 1000 END)::BIGINT)
        -- other text as Snowflake's AUTO reads it: an offset is dropped from the wall clock, US dates are dates
        WHEN typeof(val) = 'VARCHAR' THEN coalesce(try_cast(_fs_global.main._tz_wall_in(val) AS TIMESTAMP),
            try_strptime(trim(val::VARCHAR), ['%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y']), CAST(val AS TIMESTAMP))
        ELSE CAST(val AS TIMESTAMP)
    END
);
"""


def _patch_information_schema() -> None:
    """INFORMATION_SCHEMA.TABLE_CONSTRAINTS, REFERENTIAL_CONSTRAINTS and TABLE_PRIVILEGES (constraints.py), and SHOW
    COLUMNS as Snowflake prints it: a NOT NULL column's `null?` is NOT_NULL (measured), its type says nullable false, and
    there is a `write_default` column."""
    import fakesnow.transforms as _transforms
    import sqlglot
    from fakesnow.transforms import show as _show

    upstream = _transforms.information_schema_fs

    def redirected(expression):  # noqa: ANN001, ANN202
        if isinstance(expression, exp.Table) and str(expression.db).upper() == "INFORMATION_SCHEMA":
            body = constraints.views(expression.name, expression.catalog or None)
            if body is not None:
                alias = expression.args.get("alias") or exp.TableAlias(this=exp.to_identifier(expression.name.upper()))
                return exp.Subquery(this=sqlglot.parse_one(body, read="duckdb"), alias=alias)
        return upstream(expression)

    _transforms.information_schema_fs = redirected

    # a table's comment is written into fakesnow's own table as a literal: a quote in it broke the CREATE
    import fakesnow.info_schema as _info

    if not hasattr(_info.insert_table_comment_sql, "__wrapped__"):
        upstream_comment = _info.insert_table_comment_sql

        def insert_table_comment_sql(catalog, schema, table, comment):  # noqa: ANN001, ANN202
            quote = lambda v: str(v).replace("'", "''")  # noqa: E731
            return upstream_comment(quote(catalog), quote(schema), quote(table), quote(comment))

        insert_table_comment_sql.__wrapped__ = upstream_comment
        _info.insert_table_comment_sql = insert_table_comment_sql

    # a VECTOR is a DuckDB FLOAT[n] / INTEGER[n] (semantics._vector_type), not an ARRAY to be held as JSON
    semi = getattr(_transforms.semi_structured_types, "__wrapped__", _transforms.semi_structured_types)

    def semi_structured_types(expression):  # noqa: ANN001, ANN202
        if isinstance(expression, exp.DataType) and expression.meta.get("fx_vector"):
            return expression
        return semi(expression)

    semi_structured_types.__wrapped__ = semi
    _transforms.semi_structured_types = semi_structured_types

    # SHOW TABLES / OBJECTS / VIEWS in name order, with STARTS WITH and LIMIT .. FROM (dbt pages through SHOW OBJECTS so)
    listing = getattr(_transforms.show_tables_etc, "__wrapped__", _transforms.show_tables_etc)

    def show_tables_etc(expression, current_database=None, current_schema=None):  # noqa: ANN001, ANN202
        done = listing(expression, current_database, current_schema)
        if done is expression or not isinstance(done, exp.Select):
            return done
        conditions = []
        starts = expression.args.get("starts_with")
        if isinstance(starts, exp.Expression):
            conditions.append(exp.Anonymous(this="starts_with", expressions=[exp.column("name"), starts.copy()]))
        after = expression.args.get("from_")
        if isinstance(after, exp.Expression):
            conditions.append(exp.GT(this=exp.column("name"), expression=after.copy()))
        for condition in conditions:
            done = done.where(condition, copy=False)
        return done.order_by(exp.column("name"), copy=False) if not done.args.get("order") else done

    show_tables_etc.__wrapped__ = listing
    _transforms.show_tables_etc = show_tables_etc
    view = _show.SQL_CREATE_VIEW_SHOW_COLUMNS
    if "write_default" not in view:
        view = view.replace("""CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'false' END as "null?",""",
                            """CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'NOT_NULL' END as "null?",""")
        view = view.replace('"nullable":true', '"nullable":' + "' || CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'false' END || '")
        view = view.replace('NULL as "schema_evolution_record"', 'NULL as "schema_evolution_record", NULL as "write_default"')
        # (a VARCHAR's byteLength is four bytes a character, at most 128 MB, as Snowflake reports it)
        bytes_case = '''CASE
                WHEN character_maximum_length = 16777216 THEN 16777216
                ELSE coalesce(character_maximum_length*4,16777216)
            END'''
        assert bytes_case in view
        view = view.replace(bytes_case, "least(coalesce(character_maximum_length, 16777216) * 4, 134217728)")
        assert ',"fixed":false}' in view
        view = view.replace(',"fixed":false}\'', ',"fixed":false\' || coalesce(\',"collation":"\' || collation_name || \'"\', \'\') || \'}\'')
        _show.SQL_CREATE_VIEW_SHOW_COLUMNS = view


def _vectors_described(describe):  # noqa: ANN001, ANN202
    """fakesnow's describe_as_rowtype, also for a VECTOR: a FLOAT[n] or INTEGER[n] column (see semantics._vector_type),
    described as Snowflake describes one: type vector, its dimension, and one field for the element."""

    def described_with_vectors(described):  # noqa: ANN001, ANN202
        vectors = {i: semantics.VECTOR_KIND.fullmatch(str(row[1]).upper()) for i, row in enumerate(described)}
        vectors = {i: m for i, m in vectors.items() if m and m[2]}
        geos = {i: semantics.GEO_KINDS[str(row[1]).upper()] for i, row in enumerate(described) if str(row[1]).upper() in semantics.GEO_KINDS}
        if not vectors and not geos:
            return describe(described)
        info = describe([(row[0], "BLOB" if i in vectors else "VARCHAR", *row[2:]) if i in vectors or i in geos else row
                         for i, row in enumerate(described)])
        for i, kind in geos.items():  # (sent as GeoJSON text: see _geo_out)
            info[i].update(type=kind, byteLength=None, length=None)
        for i, m in vectors.items():
            element = {"type": "real", "precision": None, "scale": None} if m[1] == "FLOAT" else \
                {"type": "fixed", "precision": 38, "scale": 0}
            info[i].update(type="vector", byteLength=None, length=None, precision=None, scale=None,
                           vectorDimension=int(m[2]), fields=[{"name": None, "nullable": False, "byteLength": None,
                                                                "length": None, "collation": None, **element}])
        return info

    described_with_vectors.__wrapped__ = describe
    return described_with_vectors


def _patch_fakesnow() -> None:
    global _patched
    if _patched:
        return
    from string import Template as _Template

    import fakesnow.macros as _macros

    _patch_information_schema()
    _macros.FS_TO_TIMESTAMP = _Template(_TO_TIMESTAMP)  # (fakesnow's casts a number to BIGINT first: 0.023 s became 0)
    # FLATTEN numbers each element by its own place in the list (the lambda's second argument): fakesnow's
    # row_number() OVER () keeps no order, and over a large table run in parallel an element took another's INDEX
    # (Flipside's receipt ids `shard-INDEX` paired with other receipts). An array's `undefined` elements are skipped,
    # and the others keep their indexes: the rows are numbered, then left out
    head = _macros.FS_FLATTEN.template.partition(" AS TABLE")[0]
    if "_fx_undefined" not in _macros.FS_FLATTEN.template:
        _macros.FS_FLATTEN = _Template(f"""{head} AS TABLE
    SELECT * FROM (
        SELECT hash(TO_JSON(input))::UBIGINT AS SEQ, e.k AS KEY,
               COALESCE(e.k, '[' || (e.i - 1) || ']') AS PATH,
               CASE WHEN e.k IS NOT NULL THEN NULL ELSE (e.i - 1)::BIGINT END AS INDEX,
               e.v AS VALUE, _fs_global.main._sf_doc(input) AS THIS
        FROM (SELECT UNNEST(CASE WHEN json_type(TO_JSON(input)) = 'OBJECT'
                 THEN list_transform(list_zip(json_keys(TO_JSON(input)), CASE WHEN _fs_global.main._sf_floats(input)
                          THEN _fxpy_elements(input::VARCHAR)::JSON[] ELSE CAST(json_extract(TO_JSON(input), '$$.*') AS JSON[]) END),
                        lambda p, i: struct_pack(k := p[1], v := CAST(p[2] AS JSON), i := i))
                 ELSE list_transform(_fs_global.main._sf_list(input), lambda x, i: struct_pack(k := NULL::VARCHAR, v := x, i := i))
                 END, recursive := true)) AS e(k, v, i)
        ORDER BY e.i
    ) AS _fx_undefined
    WHERE VALUE IS DISTINCT FROM {semantics._UNDEFINED_JSON}""")
    # OBJECT_CONSTRUCT keeps its keys sorted, as Snowflake does (so objects with the same pairs are equal, hash alike, and
    # UNION into one), writes each value's own text (DuckDB's json_group_object writes a FLOAT anew, as a NUMBER)
    # and is an expression, not a subquery: a subquery would take an aggregate among the values (`object_construct('n',
    # count(*))` without GROUP BY) as its own
    _macros.FS_OBJECT_CONSTRUCT = _Template("""CREATE OR REPLACE MACRO ${catalog}._fs_object_construct(keys, vals, keep_nulls) AS (
    ('{' || array_to_string(list_transform(list_sort(list_filter(list_transform(list_zip(keys, vals), lambda p: CASE WHEN p[1] IS NOT NULL
        AND (keep_nulls OR p[2] IS NOT NULL) THEN struct_pack(k := p[1]::VARCHAR, e := to_json(p[1])::VARCHAR || ':' || COALESCE(p[2]::VARCHAR, 'null')) END),
        lambda e: e IS NOT NULL)), lambda e: e.e), ',') || '}')::JSON
);
""")
    _macros.FS_OBJECT_CONSTRUCT_STAR = _Template(_macros.FS_OBJECT_CONSTRUCT_STAR.template.replace(
        "COALESCE(json_group_object(key, value::JSON), '{}'::JSON)",
        "COALESCE(('{' || string_agg(to_json(key)::VARCHAR || ':' || value::VARCHAR, ',') || '}')::JSON, '{}'::JSON)").replace(
        "SELECT TO_JSON(COLUMNS(*))", "SELECT _fs_global.main._sf_jv(COLUMNS(*))"))
    # generated SQL can nest thousands deep (a pivot over many values, a long OR chain): give the
    # parser and the rewrites room, in the threads started from here on
    sys.setrecursionlimit(max(sys.getrecursionlimit(), 50_000))
    try:
        threading.stack_size(512 * 1024 * 1024)
    except (ValueError, RuntimeError):
        pass
    from fakesnow.cursor import FakeSnowflakeCursor
    from snowflake.connector.errors import ProgrammingError

    original = FakeSnowflakeCursor.execute

    def execute(self, command, *args, **kwargs):  # noqa: ANN001
        if os.environ.get("FAUXFLAKE_EMULATOR_TIMING"):  # for working on the emulator: where a statement's time goes
            self._fx_clock = {"start": time.monotonic(), "transform": 0.0, "transforms": 0, "run": 0.0, "runs": 0, "adjust": 0.0}
            try:
                return timed_execute(self, command, *args, **kwargs)
            finally:
                c = self._fx_clock
                total = time.monotonic() - c["start"]
                if total >= float(os.environ["FAUXFLAKE_EMULATOR_TIMING"]):
                    print(f"fauxflake timing: {total:.1f}s ({c['transforms']} rewrites {c['transform']:.1f}s, adjustments {c['adjust']:.1f}s, "
                          f"{c['runs']} runs {c['run']:.1f}s) " + ", ".join(f"{k} {v:.1f}s" for k, v in c.items() if k not in (
                              "start", "transform", "transforms", "run", "runs", "adjust")) + f" {' '.join(str(command).split())[:90]}", file=sys.stderr)
        return timed_execute(self, command, *args, **kwargs)

    def timed_execute(self, command, *args, **kwargs):  # noqa: ANN001
        semantics.reset_seeded()
        _session_zone(self, command)
        self._fx_history_seen = False
        try:
            self._fauxflake_outer = semantics.outer_flattens(command)
            self._fauxflake_recursive = semantics.outer_flattens(command, "recursive")
            self._fauxflake_paths = semantics.flatten_paths(command)
            self._fx_command = command if isinstance(command, str) else None
            self._fauxflake_layer, self._fauxflake_inline, self._fauxflake_owners = False, set(), {}
            self._fauxflake_qualify = False
            command = objects.session_functions(self, command)
            command = staged.rewrite(self, command)
            handled = _query_tag(self, command, original)
            if handled is not None:
                return handled
            _session_parameters(self, command)
            handled = _session_warehouse(self, command, original)
            if handled is not None:
                return handled
            handled = _transaction(self, command, original)
            if handled is not None:
                return handled
            handled = relations.intercept(self, command, original) or account.intercept(self, command, original) \
                or constraints.intercept(self, command, original) or objects.intercept(self, command, original)
            if handled is not None:  # (warehouses, monitors, parameters; functions, procedures, streams, tasks, policies)
                objects.remember(self)
                return handled
            temporary = _temporary(self, command)
            cut = _TRUNCATE.match(command) if isinstance(command, str) else None
            truncated = _rows_now(self, cut.group(1)) if cut else None
            for _ in range(8):
                try:
                    done = original(self, rewrite(command), *args, **kwargs)
                    if temporary:
                        made = _TEMP_RELATION.match(command)
                        TEMPORARY[temporary] = (id(self._conn), made.group("kind"), made.group("name"))
                    objects.consume_streams(self, command)
                    objects.remember(self)
                    objects.note_secure_view(self, command)
                    _note_stats(self, command, truncated)
                    relations.note(self, command)
                    lengths.note(self, command)
                    _note_empty_lookup(self, command)
                    return done
                except ProgrammingError as e:
                    # constructs DuckDB will not plan as written: say how to rewrite them, then run again
                    message = str(e)
                    alias = _ALIAS_ERROR.search(message)
                    if not alias:  # a SELECT alias used in a JOIN condition reads as a missing column
                        missing = _MISSING_COLUMN.search(message)
                        if missing and re.search(r"(?i)\bas\s+\"?" + re.escape(missing.group(1)) + r"\b", command) \
                                and re.search(r"(?i)\bjoin\b", command):
                            alias = missing
                    ambiguous = _AMBIGUOUS_ERROR.search(message)
                    if "Ambiguous reference to catalog or schema" in message and not self._fauxflake_qualify:
                        self._fauxflake_qualify = True  # schema.table when a database has the schema's name
                    elif "window function calls cannot be nested" in message and not self._fauxflake_layer:
                        self._fauxflake_layer = True  # an aliased window used inside another window
                    elif alias and alias.group(1).upper() not in self._fauxflake_inline:
                        self._fauxflake_inline.add(alias.group(1).upper())  # an alias built from another alias
                    elif "has multiple overloads that match the supplied arguments" in message and not self._fauxflake_layer \
                            and re.search(r"(?i)\bover\s*\(", command):
                        self._fauxflake_layer = "everywhere"  # a window given to an overloaded macro: computed in a layer below
                    elif "has multiple overloads that match the supplied arguments" in message \
                            and (reused := semantics.lateral_aliases(command) - self._fauxflake_inline):
                        # an overloaded macro given a SELECT alias of the same SELECT: DuckDB picks the overload
                        # before it knows the alias's type. Written out, the expression's type is known
                        self._fauxflake_inline |= reused
                    elif ambiguous and ambiguous.group(1).upper() not in self._fauxflake_owners and " using" in command.lower():
                        self._fauxflake_owners[ambiguous.group(1).upper()] = ambiguous.group(2)  # shared column of a USING join
                    else:
                        raise
            done = original(self, rewrite(command), *args, **kwargs)
            objects.consume_streams(self, command)
            objects.remember(self)
            objects.note_secure_view(self, command)
            _note_stats(self, command, truncated)
            relations.note(self, command)
            lengths.note(self, command)
            return done
        except ProgrammingError as e:
            if "_fx_refused_conversion" in str(e.msg):  # (the check of what a TRY_ function is given, in a window)
                self._sqlstate = "22023"
                raise ProgrammingError(msg="SQL compilation error:\nFunction TRY_CAST cannot be used with arguments of these types",
                                       errno=1065, sqlstate="22023") from e
            if e.errno == 2003 and "does not exist" in str(e.msg) and "not authorized" not in str(e.msg):
                # Snowflake's wording, which clients look for: dbt reads "no such table" as "no columns yet"
                self._sqlstate = "42S02"
                raise ProgrammingError(msg=f"{e.raw_msg or e.msg}\nObject does not exist or not authorized.",
                                       errno=2003, sqlstate="42S02") from e
            said = re.search(r"\x1ffxerr:(\d+):(\w+):(.*?)\x1f", str(e.msg), re.S)
            if said:  # an error of Snowflake's, raised by one of the emulator's functions through a failing cast
                self._sqlstate = said[2]
                raise ProgrammingError(msg=said[3], errno=int(said[1]), sqlstate=said[2]) from e
            text = _NOT_A_NUMBER.search(str(e.msg)) if e.errno == 100035 else None
            if text:
                self._sqlstate = "22018"
                # (DuckDB's words kept after Snowflake's: the fitter reads the value and the type from them)
                raise ProgrammingError(msg=f"Numeric value '{text[2]}' is not recognized\n{e.msg}", errno=100038, sqlstate="22018") from e
            when = _NOT_A_TIME.search(str(e.msg)) if e.errno == 100035 else None
            if when:  # Snowflake's words (measured: a date is 100040, a timestamp 100035), DuckDB's after them for the fitter
                self._sqlstate = "22007"
                kind = "Date" if when[1] == "date" else "Timestamp"
                raise ProgrammingError(msg=f"{kind} '{when[2]}' is not recognized\n{e.msg}", errno=100040 if kind == "Date" else 100035,
                                       sqlstate="22007") from e
            null = _NOT_NULL.search(str(e.msg))
            if null:  # Snowflake's words for a NULL in a NOT NULL column (measured: 100072)
                self._sqlstate = "22000"
                raise ProgrammingError(msg=f"DML operation to table {null[1]} failed on column {null[2]} with error: NULL result in a "
                                           "non-nullable column", errno=100072, sqlstate="22000") from e
            many = _MANY_CLAUSES.search(str(e.msg)) if e.errno == 1003 else None
            if many:  # Snowflake's parser stops at the second keyword: its line and 0-based position
                self._sqlstate = "42000"
                raise ProgrammingError(msg=f"SQL compilation error:\nsyntax error line {many[2]} at position "
                                           f"{max(0, int(many[3]) - len(many[1]))} unexpected '{many[1].lower()}'.", errno=1003, sqlstate="42000") from e
            raise
        except Exception as e:  # noqa: BLE001
            tried = re.search(r"Function TRY_CAST cannot be used with arguments of type [^'\n]*", str(e))
            if tried:  # (the emulator's check of what a TRY_ function is given)
                self._sqlstate = "22023"
                raise ProgrammingError(msg=f"SQL compilation error:\n{tried[0]}", errno=1065, sqlstate="22023") from e
            null = _NOT_NULL.search(str(e))
            if null:  # Snowflake's words for a NULL in a NOT NULL column (measured: 100072)
                self._sqlstate = "22000"
                raise ProgrammingError(msg=f"DML operation to table {null[1]} failed on column {null[2]} with error: NULL result in a "
                                           "non-nullable column", errno=100072, sqlstate="22000") from e
            said = re.search(r"\x1ffxerr:(\d+):(\w+):(.*?)\x1f", str(e), re.S)
            if said:  # an error of Snowflake's, raised by one of the emulator's functions
                self._sqlstate = said[2]
                raise ProgrammingError(msg=said[3], errno=int(said[1]), sqlstate=said[2]) from e
            # Anything else would surface as an HTTP 500, which the Snowflake
            # connector retries for minutes. Fail the statement immediately instead.
            self._sqlstate = "XX000"
            raise ProgrammingError(msg=f"fauxflake emulator: {type(e).__name__}: {e}", errno=9998, sqlstate="XX000") from e

    FakeSnowflakeCursor.execute = execute

    # Failures outside execute() (for example describing an unsupported result type)
    # must also come back as SQL errors, not HTTP 500s the connector retries forever.
    import fakesnow.server as server

    class Sessions(dict):
        def __delitem__(self, token) -> None:  # noqa: ANN001
            if token in self:
                end_session(self[token])
            super().__delitem__(token)

    server.sessions = Sessions(server.sessions)

    # the connector ends its session on close() only when it was given a master token at login
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    for i, route in enumerate(server.app.router.routes):
        if getattr(route, "path", "") == "/session/v1/login-request":
            login = route.endpoint

            async def login_request(request, login=login):  # noqa: ANN001, ANN202
                response = await login(request)
                try:
                    body = json.loads(response.body)
                    body["data"].setdefault("masterToken", body["data"]["token"])
                except Exception:  # noqa: BLE001 - a refused login: as it is
                    return response
                warehouse = request.query_params.get("warehouse")
                session = server.sessions.get(body["data"].get("token")) if warehouse else None
                if session is not None:  # (the warehouse the client logs in with: CURRENT_WAREHOUSE() until it USEs another)
                    session._fx_warehouse = warehouse.strip('"') if warehouse.startswith('"') else warehouse.upper()
                if session is None:
                    session = server.sessions.get(body["data"].get("token"))
                with contextlib.suppress(Exception):  # the parameters the client logs in with are the session's (SHOW PARAMETERS)
                    sent = json.loads(gzip.decompress(await request.body()) if request.headers.get("Content-Encoding") == "gzip"
                                      else await request.body())["data"].get("SESSION_PARAMETERS") or {}
                    if session is not None:
                        session.__dict__.setdefault("_fx_session_params", {}).update(
                            {k.upper(): session_value(v) for k, v in sent.items() if not k.upper().startswith("FAKESNOW") and k != "nop_regexes"})
                role = request.query_params.get("roleName")
                if role and server.shared_fs is not None:
                    # the role the client logs in with is the session's (dbt's profile names one): CURRENT_ROLE(), the owner
                    # of what it makes, the grantor of what it grants
                    with contextlib.suppress(Exception):
                        objects.set_role(server.shared_fs.duck_conn, role.strip('"') if role.startswith('"') else role)
                        body["data"].setdefault("sessionInfo", {})["roleName"] = role.strip('"').upper()
                return JSONResponse(body, status_code=response.status_code)

            server.app.router.routes[i] = Route(route.path, login_request, methods=list(route.methods or ["POST"]))
    from starlette.responses import JSONResponse

    import fakesnow.rowtype as rowtype

    if not hasattr(rowtype.describe_as_rowtype, "__wrapped__"):  # (in-process cursors describe through this one)
        rowtype.describe_as_rowtype = _vectors_described(rowtype.describe_as_rowtype)
    describe = _vectors_described(getattr(server.describe_as_rowtype, "__wrapped__", server.describe_as_rowtype))

    def describe_as_rowtype(described):  # noqa: ANN001
        try:
            # a TIMESTAMP_TZ is held as (instant, offset): described as one (see zoned.py and _zoned_out)
            zoned_at = {i for i, row in enumerate(described) if str(row[1]).upper() == zoned.SHOWN.upper()}
            # DuckDB's own TIMESTAMPTZ is a TIMESTAMP_LTZ here (a TIMESTAMP_TZ is the struct of zoned.py): described as one
            local_at = {i for i, row in enumerate(described) if str(row[1]).upper() == "TIMESTAMP WITH TIME ZONE"}
            if zoned_at:
                described = [(row[0], "TIMESTAMP WITH TIME ZONE", *row[2:]) if i in zoned_at else row for i, row in enumerate(described)]
            info = describe(described)
            for i in local_at:
                info[i]["type"] = "timestamp_ltz"
            kinds = getattr(_RESULT, "kinds", None)
            kinds = {} if kinds is None else kinds  # (an empty Lazily still answers when asked)
            for row, column in zip(described, info):
                if str(row[1]).upper() == "JSON":
                    try:
                        kind = kinds[str(row[0]).upper()]  # (looked up when asked: see semi_kinds)
                    except Exception:  # noqa: BLE001
                        kind = None
                    if kind in ("ARRAY", "OBJECT"):
                        column["type"] = kind.lower()
            return info
        except Exception as e:  # noqa: BLE001
            raise ProgrammingError(msg=f"fauxflake emulator: unsupported result type: {e}",
                                   errno=9997, sqlstate="XX000") from e

    server.describe_as_rowtype = describe_as_rowtype
    upstream_to_sf = server.to_sf

    def to_sf(table, rowtype):  # noqa: ANN001, ANN202
        done = upstream_to_sf(_geo_out(_local_out(_zoned_out(table))), rowtype)
        if any(c.get("type") == "timestamp_ltz" for c in rowtype):
            # a TIMESTAMP_LTZ travels as (epoch, fraction), as Snowflake sends one: only a TIMESTAMP_TZ carries its offset
            import pyarrow as pa

            arrays, fields = list(done.columns), list(done.schema)
            for i, c in enumerate(rowtype):
                if c.get("type") == "timestamp_ltz" and pa.types.is_struct(fields[i].type) and fields[i].type.num_fields == 3:
                    col = pa.concat_arrays([chunk for chunk in arrays[i].chunks]) if arrays[i].num_chunks else \
                        pa.array([], fields[i].type)
                    two = pa.StructArray.from_arrays([col.field(0), col.field(1)], fields=[fields[i].type.field(0), fields[i].type.field(1)],
                                                     mask=col.is_null())
                    arrays[i], fields[i] = two, fields[i].with_type(two.type)
            done = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        if any(c.get("type") in ("geography", "geometry") for c in rowtype):
            # the column is described as GEOGRAPHY / GEOMETRY; its values travel as text, as Snowflake sends GeoJSON (the
            # connector reads Arrow by `logicalType`, and has no reader of its own for these)
            import pyarrow as pa

            fields = [f.with_metadata({**(f.metadata or {}), b"logicalType": b"TEXT"}) if c.get("type") in ("geography", "geometry") else f
                      for f, c in zip(done.schema, rowtype)]
            done = pa.Table.from_arrays(done.columns, schema=pa.schema(fields))
        return done

    server.to_sf = to_sf

    # The endpoint parses a statement to classify it before running it. Statements handled in
    # objects.py (anonymous procedures, Snowflake Scripting blocks) are not SQL sqlglot reads:
    # classify those as commands and let execute() decide.
    strict_parse = server.parse_one

    def parse_one(sql):  # noqa: ANN001, ANN202
        try:
            return strict_parse(sql)
        except Exception:  # noqa: BLE001
            from sqlglot import exp

            return exp.Command(this="CALL", expression=exp.Literal.string(sql))

    server.parse_one = parse_one

    async def server_error(request, exc):  # noqa: ANN001
        return JSONResponse({"data": {"errorCode": "009996", "sqlState": "XX000"}, "code": "009996",
                             "message": f"fauxflake emulator: {exc.message}"[:2000], "success": False})

    server.app.exception_handlers[server.ServerError] = server_error
    server.app.middleware_stack = None

    # The connector gives up waiting on a statement after about a minute and sends the same request
    # again, with the same requestId. Snowflake answers the repeat with the first run's result. Do
    # the same: a second execution would collide with the first on the connection, or run an INSERT twice.
    import asyncio

    from starlette.routing import Route, request_response

    in_flight: dict[tuple[str, str], asyncio.Future] = {}
    handle = server.query_request

    def _with_stats(request, response):  # noqa: ANN001, ANN202
        """A DML statement's counts, in the reply as Snowflake gives them (`stats`: the connector's cursor.stats)."""
        try:
            conn = server.sessions.get(server.to_token(request))
            stats, zone = getattr(conn, "_fx_stats", None), getattr(conn, "_fx_timezone", None)
            if not stats and not zone:
                return response
            body = json.loads(response.body)
            if not body.get("success") or not isinstance(body.get("data"), dict):
                return response
            if stats:
                body["data"]["stats"] = stats
                conn._fx_stats = None
            if zone:  # the session's time zone, as Snowflake returns it once set: the client shows a TIMESTAMP_LTZ in it
                body["data"].setdefault("parameters", []).append({"name": "TIMEZONE", "value": zone})
            return JSONResponse(body, status_code=response.status_code)
        except Exception:  # noqa: BLE001 - the reply as it was
            return response

    async def query_request(request):  # noqa: ANN001, ANN202
        request_id = request.query_params.get("requestId")
        key = (request.headers.get("Authorization", ""), request_id or "")
        if not request_id:
            return await handle(request)
        running = in_flight.get(key)
        if running is not None:
            return await asyncio.shield(running)
        loop = asyncio.get_running_loop()
        future = in_flight[key] = loop.create_future()
        try:
            response = await handle(request)
            response = _with_stats(request, response)
        except server.ServerError:
            raise
        except Exception as e:  # noqa: BLE001
            # a fault of the emulator itself (encoding a result, say): answered as a failed statement, so the client
            # reports it at once instead of retrying the request for minutes on an HTTP 500
            response = JSONResponse({"data": {"errorCode": "009997", "sqlState": "XX000"}, "code": "009997",
                                     "message": f"fauxflake emulator: internal error: {type(e).__name__}: {e}"[:2000],
                                     "success": False})
            future.set_result(response)
            return response
        except BaseException as e:
            future.set_exception(e)
            future.exception()  # marked as retrieved: a repeat may never come
            raise
        else:
            future.set_result(response)
            return response
        finally:
            loop.call_later(600, in_flight.pop, key, None)  # long enough for every repeat of this request

    server.query_request = query_request
    for route in server.app.routes:
        if isinstance(route, Route) and route.path == "/queries/v1/query-request":
            route.endpoint = query_request
            route.app = request_response(query_request)

    # a SUM over integers is a HUGEINT in DuckDB and a NUMBER in Snowflake: describe it as one
    from string import Template

    import fakesnow.info_schema as info

    whole = "('BIGINT', 'HUGEINT', 'INTEGER', 'SMALLINT', 'TINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT', 'UHUGEINT')"
    info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW = info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW \
        .replace("when columns.data_type='TIMESTAMP WITH TIME ZONE' then 'TIMESTAMP_TZ'",
                 f"when columns.data_type='TIMESTAMP WITH TIME ZONE' then 'TIMESTAMP_LTZ' when columns.data_type='{zoned.SHOWN}' then 'TIMESTAMP_TZ'") \
        .replace("columns.data_type like 'TIME%'", f"(columns.data_type like 'TIME%' or columns.data_type = '{zoned.SHOWN}')") \
        .replace("columns.data_type='BIGINT'", f"columns.data_type in {whole}").replace("columns.data_type='DOUBLE'", "columns.data_type in ('DOUBLE', 'FLOAT')") \
        .replace("then NULL else columns.numeric_scale end", f"then NULL when columns.data_type in {whole} then 0 else columns.numeric_scale end")
    # a VECTOR (semantics._vector_type) is listed as VECTOR; DESCRIBE gives its element type and dimension
    if "'VECTOR'" not in info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW:
        info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW = info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW.replace(
            "when columns.data_type='BLOB' then 'BINARY'",
            "when columns.data_type='BLOB' then 'BINARY' when regexp_full_match(columns.data_type, '(FLOAT|INTEGER)\\[\\d+\\]') then 'VECTOR' "
            "when columns.data_type = 'STRUCT(G VARCHAR, GEOGRAPHY INTEGER)' then 'GEOGRAPHY' "
            "when columns.data_type = 'STRUCT(G VARCHAR, GEOMETRY INTEGER)' then 'GEOMETRY'")

    # the columns Snowflake's views have beyond those: precision of times, and the rest as Snowflake
    # reports them for an ordinary column
    nulls = ["CHARACTER_SET_CATALOG", "CHARACTER_SET_SCHEMA", "CHARACTER_SET_NAME", "COLLATION_CATALOG", "COLLATION_SCHEMA",
             "DOMAIN_CATALOG", "DOMAIN_SCHEMA", "DOMAIN_NAME", "UDT_CATALOG", "UDT_SCHEMA", "UDT_NAME", "SCOPE_CATALOG",
             "SCOPE_SCHEMA", "SCOPE_NAME", "MAXIMUM_CARDINALITY", "DTD_IDENTIFIER", "INTERVAL_TYPE", "INTERVAL_PRECISION",
             "IDENTITY_MAXIMUM", "IDENTITY_MINIMUM", "EXPRESSION", "SCHEMA_EVOLUTION_RECORD"]
    more = (f",\n    case when columns.data_type like 'TIME%' then least(coalesce(columns.datetime_precision, 9), 9) when columns.data_type = '{zoned.SHOWN}' then 9 end as DATETIME_PRECISION,"
            "\n    'COLUMN' as KIND, 'NO' as IS_SELF_REFERENCING, " + ", ".join(f"null::VARCHAR as {c}" for c in nulls))
    if "as DATETIME_PRECISION" not in info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW:
        info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW = info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW.replace(
            "null::VARCHAR as DATA_TYPE_ALIAS", "null::VARCHAR as DATA_TYPE_ALIAS" + more)
    info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = Template(relations.INFORMATION_SCHEMA_TABLES)
    tables_view = info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW.template
    if '"ERROR_LOGGING"' not in tables_view:
        info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = Template(tables_view.replace(
            "'NO' AS \"IS_HYBRID\"", "'NO' AS \"IS_HYBRID\", 'NO' AS \"ERROR_LOGGING\", 'NO' AS \"IS_INTERACTIVE\", 'NO' AS \"ROW_TIMESTAMP_ON\""))
    tables_view = info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW.template
    if "'VIEW' AS kind" not in tables_view and "_fx_relations" not in tables_view:
        # Snowflake lists views here too (TABLE_TYPE 'VIEW', no row count, size or retention): a project that reads a
        # model's columns through it (GitLab's masking post-hook, on every view model) finds them
        for old, new in [
            ("from duckdb_tables tables", "from (select database_name, schema_name, table_name, estimated_size, 'BASE TABLE' AS kind "
                                          "from duckdb_tables union all select database_name, schema_name, view_name, NULL, 'VIEW' AS kind "
                                          "from duckdb_views where not internal) tables"),
            ("'BASE TABLE' AS \"TABLE_TYPE\"", "tables.kind AS \"TABLE_TYPE\""),
            ("'NO' AS \"IS_TRANSIENT\"", "case when tables.kind = 'VIEW' then NULL else 'NO' end AS \"IS_TRANSIENT\""),
            ("0 AS \"BYTES\"", "case when tables.kind = 'VIEW' then NULL else 0 end AS \"BYTES\""),
            ("1 AS \"RETENTION_TIME\"", "case when tables.kind = 'VIEW' then NULL else 1 end AS \"RETENTION_TIME\""),
        ]:
            assert old in tables_view, old
            tables_view = tables_view.replace(old, new)
        info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = Template(tables_view)
    databases_view = info.SQL_CREATE_INFORMATION_SCHEMA_DATABASES_VIEW.template
    if "OWNER_ROLE_TYPE" not in databases_view:
        info.SQL_CREATE_INFORMATION_SCHEMA_DATABASES_VIEW = Template(databases_view.replace(
            "'STANDARD' as TYPE", "'STANDARD' as TYPE, 'ROLE' as OWNER_ROLE_TYPE, 'NO' as REPLICABLE_WITH_FAILOVER_GROUPS"))

    # DESCRIBE TABLE reads the current database's column view, so a table in another database (a
    # source in RAW, described while connected to ANALYTICS) came back with no columns. dbt_utils.star
    # on such a source then expanded to nothing. Read the view that covers every database.
    import fakesnow.transforms.transforms as upstream

    if "_fs_global._fs_information_schema._fs_columns" not in upstream.SQL_DESCRIBE_TABLE.template:
        upstream.SQL_DESCRIBE_TABLE = Template(upstream.SQL_DESCRIBE_TABLE.template.replace(
            "FROM _fs_information_schema._fs_columns", "FROM _fs_global._fs_information_schema._fs_columns"))
    if '"write default"' not in upstream.SQL_DESCRIBE_TABLE.template:
        # DESCRIBE as Snowflake's (measured): the column's comment, whether a primary or unique key names it, `write default`
        def key(kind: str) -> str:
            return ("CASE WHEN EXISTS (SELECT 1 FROM _fs_global.main._fx_live_constraints k WHERE k.database_name = _fs_columns.table_catalog "
                    "AND k.schema_name = _fs_columns.table_schema AND k.table_name = _fs_columns.table_name "
                    f"AND k.kind = '{kind}' AND list_contains(k.columns, _fs_columns.column_name)) THEN 'Y' ELSE 'N' END")
        template = upstream.SQL_DESCRIBE_TABLE.template
        for old, new in [("""NULL::VARCHAR AS "comment",""", """_fs_columns.comment AS "comment","""),
                         ("""'N' AS "primary key",""", f"""{key('PRIMARY KEY')} AS "primary key","""),
                         ("""'N' AS "unique key",""", f"""{key('UNIQUE')} AS "unique key","""),
                         ("""NULL::JSON AS "privacy domain",""", """NULL::JSON AS "privacy domain", NULL::VARCHAR AS "write default",""")]:
            assert old in template, old
            template = template.replace(old, new)
        upstream.SQL_DESCRIBE_TABLE = Template(template)
    if "COLLATE" not in upstream.SQL_DESCRIBE_TABLE.template:  # (a column's collation, as Snowflake writes it)
        text = "WHEN data_type = 'TEXT' THEN 'VARCHAR(' || coalesce(character_maximum_length,16777216)  || ')'"
        assert text in upstream.SQL_DESCRIBE_TABLE.template
        upstream.SQL_DESCRIBE_TABLE = Template(upstream.SQL_DESCRIBE_TABLE.template.replace(
            text, text + " || coalesce(' COLLATE ''' || collation_name || '''', '')"))
    if "VECTOR(" not in upstream.SQL_DESCRIBE_TABLE.template:
        upstream.SQL_DESCRIBE_TABLE = Template(upstream.SQL_DESCRIBE_TABLE.template.replace(
            "        ELSE data_type END AS \"type\",",
            "         WHEN data_type = 'VECTOR' THEN (SELECT 'VECTOR(' || CASE WHEN d.data_type LIKE 'FLOAT%' THEN 'FLOAT' ELSE 'INT' END"
            " || ', ' || regexp_extract(d.data_type, '\\[(\\d+)\\]', 1) || ')' FROM duckdb_columns() d"
            " WHERE d.database_name = _fs_columns.table_catalog AND d.schema_name = _fs_columns.table_schema"
            " AND d.table_name = _fs_columns.table_name AND d.column_name = _fs_columns.column_name)\n"
            "        ELSE data_type END AS \"type\","))

    semantics.patch_upstream()
    import fakesnow.cursor as cursor_module

    upstream_parse = cursor_module.parse_one

    def parse_named(command):  # noqa: ANN001, ANN202
        return semantics.name_expressions(upstream_parse(command), _NAMED_AS.get(command, command))

    cursor_module.parse_one = parse_named
    original_transform = FakeSnowflakeCursor._transform

    def _transform(self, expression, params):  # noqa: ANN001
        clock, began = getattr(self, "_fx_clock", None), time.monotonic()
        try:
            return _transform_once(self, expression, params)
        finally:
            if clock is not None:
                clock["transform"] += time.monotonic() - began
                clock["transforms"] += 1

    def _transform_once(self, expression, params):  # noqa: ANN001
        if not getattr(self, "_fx_history_seen", False):  # (once a statement: a retry transforms it again)
            self._fx_history_seen = True
            history.before(self, expression)
        expression = history.rewrite(expression, self)
        expression = objects.apply_policies(objects.tag_scans(objects.result_scans(expression, self), self), self)
        if getattr(self, "_fauxflake_qualify", False) and self._conn.database:
            expression = semantics.qualify_schemas(expression, self._conn.database)
        if getattr(self, "_fauxflake_owners", None):
            expression = semantics.qualify_using(expression, self._fauxflake_owners)
        if getattr(self, "_fauxflake_inline", None):
            expression = semantics.inline_aliases(expression, self._fauxflake_inline)
        if getattr(self, "_fauxflake_layer", False):
            expression = semantics.layer_windows(expression, everywhere=self._fauxflake_layer == "everywhere")
        elif semantics.reads_window_aliases(expression):
            expression = semantics.layer_windows(expression, everywhere=True)
        if isinstance(expression, exp.Create):
            _note_semi_types(self, expression)
        _note_result_types(self, expression)
        try:
            expression = zoned.rewrite(expression, self._duck_conn, self._conn.database, self._conn.schema)
        except Exception as e:  # noqa: BLE001 - the statement goes on as written
            if os.environ.get("FAUXFLAKE_DEBUG_ZONED"):
                raise
            print(f"fauxflake: TIMESTAMP_TZ rewrite skipped: {e}", file=sys.stderr) if os.environ.get("FAUXFLAKE_SHOW_SQL") else None
        out = original_transform(self, semantics.rewrite(expression), params)
        out = semantics.finish(semantics.parenthesize_arrows(out))
        out = semantics.keep_outer_rows(out, getattr(self, "_fauxflake_outer", []), getattr(self, "_fauxflake_recursive", []),
                                        getattr(self, "_fauxflake_paths", []))
        if os.environ.get("FAUXFLAKE_SHOW_SQL"):  # what DuckDB is actually asked to run
            print(out.sql(dialect="duckdb"), file=sys.stderr)
        return out

    FakeSnowflakeCursor._transform = _transform

    original_explode = FakeSnowflakeCursor._transform_explode

    def _transform_explode(self, expression):  # noqa: ANN001
        if isinstance(expression, exp.Merge):
            _variant_takes_text(self, expression)  # before the MERGE is taken apart into the statements that carry it out
        # ALTER TABLE t DROP COLUMN a, b (dbt's on_schema_change: sync_all_columns): one column at a time
        actions = expression.args.get("actions") or [] if isinstance(expression, exp.Alter) else []
        dropped = actions[0].args.get("tables") or [] if len(actions) == 1 and isinstance(actions[0], exp.Drop) else []
        if len(dropped) > 1:
            parts = []
            for column in dropped:
                part = expression.copy()
                part.args["actions"][0].set("tables", [column.copy()])
                parts.append(part)
            return parts
        return original_explode(self, expression)

    FakeSnowflakeCursor._transform_explode = _transform_explode

    original_execute = FakeSnowflakeCursor._execute

    def _heal(self, force: bool = False) -> None:  # noqa: ANN001, FBT001, FBT002
        """In Snowflake a statement that fails leaves its transaction open. DuckDB aborts the whole
        transaction, so start another and repeat what the transaction had done so far. With `force`, also when DuckDB
        kept it (a binder error): what the failed statement began (the emulator's own steps for it) is undone, as on
        Snowflake, where a failed statement leaves nothing behind for other sessions to wait on."""
        conn = self._conn
        if not getattr(conn, "_fauxflake_txn", False) or getattr(conn, "_fauxflake_healing", False):
            return
        if not force:
            try:
                self._duck_conn.execute("select 1")
                return
            except Exception:  # noqa: BLE001, S110 - aborted
                pass
        conn._fauxflake_healing = True
        try:
            self._duck_conn.execute("rollback")
            self._duck_conn.execute("begin transaction")
            for tree, values in list(getattr(conn, "_fauxflake_journal", [])):
                original_execute(self, tree.copy(), values)
        except Exception:  # noqa: BLE001 - what it had done cannot be repeated: the transaction is over, rolled back
            conn._fauxflake_txn, conn._fauxflake_journal = False, []
            try:
                self._duck_conn.execute("rollback")
            except Exception:  # noqa: BLE001, S110
                pass
        finally:
            conn._fauxflake_healing = False

    def _outside_transaction(self, sql: str) -> None:  # noqa: ANN001
        """Run a statement of the emulator's own (a view defined again) so that it stays whatever becomes of the
        session's transaction: the transaction is set aside, the statement runs, and what the transaction had
        done so far is repeated in a new one."""
        conn = self._conn
        if not getattr(conn, "_fauxflake_txn", False) or getattr(conn, "_fauxflake_healing", False):
            self._duck_conn.execute(sql)
            return
        conn._fauxflake_healing = True
        try:
            self._duck_conn.execute("rollback")
            self._duck_conn.execute(sql)
            self._duck_conn.execute("begin transaction")
            for tree, values in list(getattr(conn, "_fauxflake_journal", [])):
                original_execute(self, tree.copy(), values)
        except Exception:  # noqa: BLE001 - what it had done cannot be repeated: the transaction is over, rolled back
            conn._fauxflake_txn, conn._fauxflake_journal = False, []
            with contextlib.suppress(Exception):
                self._duck_conn.execute("rollback")
        finally:
            conn._fauxflake_healing = False

    def _execute(self, transformed, params=None):  # noqa: ANN001
        def run(tree) -> None:  # noqa: ANN001
            conn = self._conn
            # MERGE works through a temporary table, which DuckDB counts as a second database. Inside a
            # transaction it is an ordinary table next to the target, dropped when the transaction ends.
            scratch = [t for t in tree.find_all(exp.Table) if t.name.upper() == "MERGE_CANDIDATES" and not t.db]
            merging = None
            if scratch and isinstance(tree, exp.Create):
                merging = _merge_check(self, tree)
                _drop_scratch(self)  # of an earlier MERGE
                source = tree.find(exp.From)
                target = source.this if source is not None and isinstance(source.this, exp.Table) else None
                home = ((target.catalog if target is not None else "") or conn.database,
                        (target.db if target is not None else "") or conn.schema)
                if getattr(conn, "_fauxflake_txn", False) and all(home):
                    conn._fauxflake_scratch = (home[0], home[1], f"__FX_MERGE_{id(conn):X}")
            where = getattr(conn, "_fauxflake_scratch", None)
            if scratch and where:
                for t in scratch:
                    t.set("this", exp.to_identifier(where[2], quoted=True))
                    t.set("db", exp.to_identifier(where[1], quoted=True))
                    t.set("catalog", exp.to_identifier(where[0], quoted=True))
                if isinstance(tree, exp.Create):
                    properties = tree.args.get("properties")
                    if properties:
                        properties.set("expressions", [p for p in properties.expressions if not isinstance(p, exp.TemporaryProperty)])
                    # straight to DuckDB: fakesnow's bookkeeping for a new table lives in another database
                    self._duck_conn.execute(tree.sql(dialect="duckdb"))
                    if merging:
                        _refuse_nondeterministic_merge(self, merging, ".".join(f'"{p}"' for p in where))
                    if not getattr(conn, "_fauxflake_healing", False):
                        conn.__dict__.setdefault("_fauxflake_journal", []).append((tree.copy(), params))
                    return
            journaled = getattr(self._conn, "_fauxflake_txn", False) and not isinstance(tree, (exp.Query, exp.Describe, exp.Show))
            kept = tree.copy() if journaled else None
            replaced = _over_a_stand_in(self, tree) if STANDINS and isinstance(tree, exp.Create) else None
            if isinstance(tree, (exp.Insert, exp.Update)):
                _variant_takes_text(self, tree)
            if getattr(self._conn, "_fauxflake_undo", None) is not None:
                _undo_copy(self, tree)
            try:
                original_execute(self, tree, params)
                if merging:
                    _refuse_nondeterministic_merge(self, merging, "merge_candidates")
                if replaced:
                    _built(self, replaced[0])
                if isinstance(tree, exp.Create):
                    _record_semi_types(self)
            except Exception as e:
                if replaced and replaced[1]:
                    with contextlib.suppress(Exception):
                        self._duck_conn.execute(replaced[1])
                if kept is not None and _TWO_DATABASES in str(e) and not getattr(self._conn, "_fauxflake_healing", False):
                    # DuckDB keeps a transaction to one database, and this statement needs a second
                    # (MERGE works through a temporary table): what the transaction did so far is
                    # committed here, and the rest of it runs in autocommit
                    # (MERGE works through a temporary table): the transaction goes on in autocommit, each table it
                    # writes copied first, so that ROLLBACK can put them back (see _undo)
                    conn = self._conn
                    conn._fauxflake_txn, conn._fauxflake_healing = False, True
                    conn._fauxflake_undo = {}
                    try:
                        self._duck_conn.execute("rollback")
                        for done, values in list(getattr(conn, "_fauxflake_journal", [])):
                            _undo_copy(self, done)
                            original_execute(self, done.copy(), values)
                    finally:
                        conn._fauxflake_healing = False
                    conn._fauxflake_journal = []
                    _undo_copy(self, kept)
                    original_execute(self, kept, params)
                    return
                _heal(self)
                raise
            conn = self._conn
            if kept is not None and getattr(conn, "_fauxflake_txn", False) and not getattr(conn, "_fauxflake_healing", False):
                conn.__dict__.setdefault("_fauxflake_journal", []).append((kept, params))

        def attempt() -> None:
            nonlocal transformed
            clock, began = getattr(self, "_fx_clock", None), time.monotonic()
            # the adjustments ask DuckDB for the types a statement binds to, which on a model of many CTEs takes as
            # long as running it. The same statement over relations of the same types (the next round of a fitting,
            # the build after the one that prepared it) is adjusted the same way: that answer is kept
            written = transformed.sql(dialect="duckdb") if isinstance(transformed, exp.Expression) else None
            kept = ADJUSTED.get(written) if written is not None else None
            types = _types_read(self, transformed) if kept is not None else None
            if kept is not None and kept[0] == types:
                transformed = kept[1].copy()
            else:
                for adjust in (semantics.coerce_union_literals, semantics.resolve_equalities, semantics.mixed_kinds, semantics.union_text_kinds,
                               semantics.days_added_to_aggregates, semantics.rounded_divisions, semantics.shared_aliases,
                               semantics.variants_by_value, semantics.kept_floats):
                    if adjust in (semantics.mixed_kinds, semantics.union_text_kinds, semantics.days_added_to_aggregates,
                                  semantics.rounded_divisions, semantics.variants_by_value) and (
                            getattr(self._conn, "_fauxflake_txn", False) or not isinstance(
                            transformed, (exp.Select, exp.Create, exp.Insert, exp.Union))):
                        continue  # (a probe that fails inside a transaction would abort it; MERGE and the like are left alone)
                    one = time.monotonic()
                    try:
                        transformed = adjust(transformed, self._duck_conn)
                    except Exception:  # noqa: BLE001, S110 - an optional adjustment: the statement runs as written
                        pass
                    if clock is not None:
                        clock[adjust.__name__] = clock.get(adjust.__name__, 0.0) + time.monotonic() - one
                if written is not None and time.monotonic() - began > 0.25 and len(ADJUSTED) < 5000:
                    try:
                        ADJUSTED[written] = (_types_read(self, transformed), transformed.copy())
                    except Exception:  # noqa: BLE001, S110
                        pass
            if clock is not None:
                clock["adjust"] += time.monotonic() - began
            _heal(self)  # a probe that failed inside a transaction aborted it
            if isinstance(transformed, (exp.Select, exp.Create, exp.Insert, exp.Union)):
                try:
                    if not getattr(self._conn, "_fx_names_learned", False):
                        semantics.learn_names(transformed, self._duck_conn)
                        self._conn._fx_names_learned = True
                    misnamed = semantics.misnamed_column(transformed, self._duck_conn,
                                                         probe=not getattr(self._conn, "_fauxflake_txn", False))
                except Exception:  # noqa: BLE001 - a check of its own that fails leaves the statement to run
                    misnamed = None
                refused = semantics.zoned_argument(transformed)
                if refused is None and not getattr(self._conn, "_fauxflake_txn", False):
                    try:
                        refused = semantics.refused_conversions(transformed, self._duck_conn)
                    except Exception:  # noqa: BLE001 - a check of its own that fails leaves the statement to run
                        refused = None
                if refused:
                    raise ProgrammingError(msg=refused[1], errno=refused[0], sqlstate="42P13" if refused[0] == 1044 else "22023")
                if misnamed:
                    errno, state = (2028, "42601") if "ambiguous" in misnamed else (2025, "42S21") if "duplicate column" in misnamed \
                        else (904, "42000")
                    raise ProgrammingError(msg=misnamed, errno=errno, sqlstate=state)
            semantics.learn_names(transformed)
            remedies = {
                "times": lambda e: semantics.numbers_as_times(transformed, e) or semantics.offsets_as_written(transformed, e),
                "by_name": lambda e: semantics.view_past_a_schema_named_like_a_database(transformed, e),
                "grouped": lambda e: semantics.group_by_all_written_out(transformed) if "Cannot mix aggregates" in e else None,
                "unshadowed": lambda e: semantics.aliases_over_tables(transformed) if "STRUCT(" in e else None,
                "variants": lambda e: (semantics.text_beside_variant(transformed, self._duck_conn)
                                       or semantics.text_beside_variant_in_views(transformed, self._duck_conn,
                                                                                 lambda sql: _outside_transaction(self, sql)))
                if "Malformed JSON" in e else None,
                "zones": lambda e: semantics.dates_in_time_zones(transformed, self._duck_conn) if "TIMESTAMPTZ_NS" in e else None,
                "bytes": lambda e: semantics.shifted_bytes(transformed, self._duck_conn, e),
                "pruned": lambda e: semantics.pruned_nulls(transformed, self._duck_conn)
                if "Conversion Error" in e or "is not recognized" in e or "Could not convert" in e else None,
                "types": lambda e: semantics.repair_types(transformed, self._duck_conn, params)
                if "No function matches" in e or "Could not choose a best candidate" in e else None,
                "empty_sides": lambda e: semantics.empty_join_sides(transformed, self._duck_conn)
                if not any(w in e for w in _NOT_DATA_ERRORS) else None,
            }
            used: collections.Counter = collections.Counter()
            while True:
                ran = time.monotonic()
                try:
                    run(transformed)
                    return
                except (ProgrammingError, duckdb.Error) as e:
                    if clock is not None:
                        clock["run"] += time.monotonic() - ran
                        clock["runs"] += 1
                        ran = None
                    # one repair can uncover the next (an implicit conversion, then an alias named like a table)
                    for name, remedy in remedies.items():
                        if used[name] >= 2:
                            continue
                        repaired = remedy(str(e))
                        _heal(self)
                        if repaired is not None:
                            used[name] += 1  # only a repair that was made counts against its two tries
                            transformed = repaired
                            break
                    else:
                        raise

        try:
            run_guarded(self, attempt)
        except Exception as e:
            if os.environ.get("FAUXFLAKE_FIT_DEBUG"):  # for working on the emulator: what DuckDB refused
                if len(FAILED_STATEMENTS) < 500:
                    FAILED_STATEMENTS[str(e)[:160]] = transformed.sql(dialect="duckdb")[:4000]
            _heal(self, force=True)
            missing = _NO_SUCH_COLUMN.search(str(e))
            if missing and getattr(e, "errno", None) in (None, 9998, 2043, 1003):
                # Snowflake's words for a name that is no column (dbt shows them; tests look for the name in them)
                # (DuckDB's words kept after Snowflake's: the retries above read them, an alias used in a JOIN for one)
                raise ProgrammingError(msg=f"SQL compilation error: error line 1 at position 0\ninvalid identifier '{missing.group(1)}'"
                                           f"\n{getattr(e, 'msg', None) or e}", errno=904, sqlstate="42000") from e
            raise
        semantics.coerce_result(self)

    FakeSnowflakeCursor._execute = _execute
    _patched = True


@dataclass
class Emulator:
    port: int
    duck: duckdb.DuckDBPyConnection
    _fs: object
    db_path: object = None  # where the databases are kept on disk, if anywhere

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A native DuckDB cursor on the emulated warehouse."""
        return self.duck.cursor()

    def ensure_database(self, database: str, schema: str) -> None:
        self._fs.connect(database=database.upper(), schema=schema.upper()).close()

    def connect_kwargs(self, **overrides) -> dict:
        return dict(
            user="ci", password="ci", account="fauxflake", host="127.0.0.1",
            port=self.port, protocol="http", session_parameters={"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False},
        ) | overrides


_SPEC_UDFS: set[str] = set()  # the stand-ins currently defined from a spec


def define_udfs(emu: "Emulator", udfs: dict[str, str]) -> None:
    """Stand-ins for functions that exist only in your Snowflake account, from the spec's `udfs:`.
    Each is `name: "(args) as <DuckDB SQL>"` and is callable with or without qualification."""
    # only the stand-ins of an earlier spec go: the functions and policies the project itself created
    # (in hooks that later runs may not repeat) stay known
    for name in _SPEC_UDFS:
        if semantics.UDFS.get(name) == f"_udf_{name}":
            semantics.UDFS.pop(name, None)
    _SPEC_UDFS.clear()
    # a placeholder written by `init` gives way to the project's own CREATE FUNCTION; a real stand-in does not
    objects.pin(emu, [n for n, body in udfs.items() if not body.rstrip().endswith("as null::varchar")])
    created = objects.state_of(emu._fs).functions if hasattr(emu, "_fs") else {}
    for name, body in udfs.items():
        if name in created and body.rstrip().endswith("as null::varchar"):
            continue  # the project has since created the real one
        emu.duck.execute(f"create or replace macro _fs_global.main._udf_{name}{body}")
        semantics.UDFS[name] = f"_udf_{name}"
        _SPEC_UDFS.add(name)


CURRENT: "Emulator | None" = None  # the emulator whose Snowflake endpoint is being served


def free_port() -> int:
    with contextlib.closing(socket.socket(type=socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


TEMP_DIRS: list[str] = []  # removed when the process is told to stop and cannot leave normally


_OWNER = ".fauxflake-pid"


def _mark(path: str | Path) -> None:
    """Say which process a temporary directory belongs to, so that a later run can tell when it was left behind."""
    with contextlib.suppress(OSError):
        (Path(path) / _OWNER).write_text(str(os.getpid()))


def sweep_stale(root: str | Path | None = None, older_than: float = 12 * 3600) -> list[str]:
    """Remove the temporary directories of runs that were killed before they could clean up (a CI job cancelled, a
    laptop out of memory): `fauxflake-*` in the system's temporary directory whose process is gone, or, where no
    process is named, that nothing has touched for half a day. Returns what was removed."""
    import tempfile

    removed = []
    for path in Path(root or tempfile.gettempdir()).glob("fauxflake-*"):
        try:
            if not path.is_dir() or path.is_symlink():
                continue
            marker = path / _OWNER
            if marker.is_file():
                pid = int(marker.read_text().strip() or 0)
                try:
                    os.kill(pid, 0)
                    continue  # its process is alive
                except ProcessLookupError:
                    pass
                except OSError:
                    continue  # there, but not ours to ask
            elif time.time() - path.stat().st_mtime < older_than:
                continue
            shutil.rmtree(path, ignore_errors=True)
            removed.append(str(path))
        except (OSError, ValueError):
            continue
    return removed


def _spill_directory(duck: duckdb.DuckDBPyConnection) -> str:
    """Where DuckDB writes what does not fit in memory. Its default is `.tmp` in the working directory,
    which is the user's dbt repository: use a private directory outside it, removed on exit."""
    import tempfile

    path = tempfile.mkdtemp(prefix="fauxflake-spill-")
    _mark(path)
    TEMP_DIRS.append(path)
    duck.execute("set temp_directory = '" + path.replace("'", "''") + "'")
    # A query that explodes on generated data (a join on a key that is always the same) would
    # otherwise write until the disk is full. Stop it at a bound, and never past a quarter of what is free.
    limit = os.environ.get("FAUXFLAKE_MAX_SPILL")
    if not limit:
        free = shutil.disk_usage(path).free
        limit = f"{max(256, min(8192, free // 4 // 2**20))}MiB"
    duck.execute(f"set max_temp_directory_size = '{limit}'")
    _bound_memory(duck)
    import atexit

    atexit.register(shutil.rmtree, path, ignore_errors=True)  # also when the run is interrupted
    return path


def _bound_memory(duck: duckdb.DuckDBPyConnection) -> None:
    """DuckDB takes up to 80% of the machine's memory before it spills to disk. dbt, the generator and
    the rest of the machine live in what is left: on an 8 GB laptop or a small CI runner that starves
    the system (the run gets killed) well before DuckDB would have spilled. Take three quarters of
    DuckDB's own figure, which already accounts for a container's limit. `FAUXFLAKE_MEMORY=4GB` sets it."""
    limit = os.environ.get("FAUXFLAKE_MEMORY")
    if not limit:
        try:
            (default,) = duck.execute("select current_setting('memory_limit')").fetchone()
            number, unit = re.fullmatch(r"\s*([\d.]+)\s*([KMGT]i?B)\s*", default).groups()
            mib = float(number) * {"K": 2**-10, "M": 1, "G": 2**10, "T": 2**20}[unit[0]]
            limit = f"{max(512, int(mib * 0.75))}MiB"
        except Exception:  # noqa: BLE001 - a setting we cannot read is left as it is
            return
    duck.execute(f"set memory_limit = '{limit}'")


@contextlib.contextmanager
def engine() -> Iterator[Emulator]:
    """Only the DuckDB side, with no Snowflake endpoint: enough to generate and inspect data."""
    from fakesnow.instance import FakeSnow

    _patch_fakesnow()
    fs = FakeSnow(nop_regexes=NOPS)
    semantics.python_functions(fs.duck_conn)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
    semantics.reserved_words(fs.duck_conn)
    _hide_stand_ins(fs.duck_conn)
    _describe_semi_types(fs.duck_conn)
    fs.duck_conn.execute("set global old_implicit_casting = true")  # text compares with numbers and dates, as in Snowflake
    spill = _spill_directory(fs.duck_conn)
    semantics.UDFS.clear()  # functions belong to the emulator that defined them
    semantics.UDTFS.clear()
    _SPEC_UDFS.clear()
    fs.results_cache.setdefault(objects._KEY, objects.State()).root = fs.duck_conn
    try:
        emu = Emulator(port=0, duck=fs.duck_conn, _fs=fs)
        yield emu
    finally:
        objects.close(emu)
        fs.duck_conn.close()
        shutil.rmtree(spill, ignore_errors=True)


@contextlib.contextmanager
def serve(port: int | None = None, db_path: str | Path | None = None, verbose: bool = False,
          keep: bool = False) -> Iterator[Emulator]:
    """Start the emulator. With `db_path`, each database persists as `<db_path>/<NAME>.db`.

    The Snowflake endpoint of the most recently started emulator wins: run one per process
    when dbt is involved.
    """
    global FROZEN_NOW
    FROZEN_NOW = None
    if not verbose:
        warnings.filterwarnings("ignore", category=UserWarning, module="fakesnow")
        logging.getLogger("sqlglot").setLevel(logging.ERROR)
    import fakesnow.server
    import uvicorn
    from fakesnow.instance import FakeSnow

    _patch_fakesnow()
    if not verbose:
        logging.getLogger("fakesnow.server").setLevel(logging.WARNING)
    sweep_stale()
    if db_path and Path(db_path).name.startswith("fauxflake-"):
        _mark(db_path)
    if db_path and not keep:
        Path(db_path).mkdir(parents=True, exist_ok=True)
        for stale in [*Path(db_path).glob("*.db"), *Path(db_path).glob("*.db.wal"), *Path(db_path).glob(objects.FUNCTIONS_FILE)]:
            stale.unlink()  # every run starts from an empty warehouse
    fs = FakeSnow(db_path=db_path, nop_regexes=NOPS)
    semantics.python_functions(fs.duck_conn)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
    semantics.reserved_words(fs.duck_conn)
    _hide_stand_ins(fs.duck_conn)
    _describe_semi_types(fs.duck_conn)
    fs.duck_conn.execute("set global old_implicit_casting = true")  # text compares with numbers and dates, as in Snowflake
    spill = _spill_directory(fs.duck_conn)
    semantics.UDFS.clear()  # functions belong to the emulator that defined them
    semantics.UDTFS.clear()
    _SPEC_UDFS.clear()
    state = fs.results_cache.setdefault(objects._KEY, objects.State())
    state.root, state.home = fs.duck_conn, db_path
    previous = fakesnow.server.shared_fs
    fakesnow.server.shared_fs = fs

    port = port or free_port()
    server = uvicorn.Server(
        uvicorn.Config(fakesnow.server.app, host="127.0.0.1", port=port, log_level="info" if verbose else "warning")
    )
    thread = threading.Thread(target=server.run, name="fauxflake-emulator", daemon=True)
    thread.start()
    while thread.is_alive() and not server.started:
        time.sleep(0.02)
    if not server.started:
        raise RuntimeError(f"emulator failed to start on port {port}")
    global CURRENT
    emu = Emulator(port=port, duck=fs.duck_conn, _fs=fs, db_path=db_path)
    outer, CURRENT = CURRENT, emu
    try:
        if db_path and keep:
            objects.restore_functions(emu)
        yield emu
    finally:
        CURRENT = outer
        end_session(duck=fs.duck_conn)
        objects.close(emu)
        server.should_exit = True
        thread.join(timeout=5)
        fakesnow.server.shared_fs = previous
        fs.duck_conn.close()
        shutil.rmtree(spill, ignore_errors=True)
