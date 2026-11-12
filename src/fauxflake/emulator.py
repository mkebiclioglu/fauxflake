"""Local Snowflake endpoint backed by DuckDB.

The real `dbt-snowflake` adapter connects here over the Snowflake wire protocol
(served by fakesnow, which transpiles Snowflake SQL to DuckDB with sqlglot).
The generator shares the same DuckDB instance in-process, so synthetic source
data is written at native DuckDB speed without going through the protocol.
"""

from __future__ import annotations

import collections
import contextlib
import json
import logging
import os
import re
import shutil
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

from . import objects, semantics

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
        # dbt reads the session's query tag before setting its own (`query_tag` config)
        re.compile(r"(?s)^(?:\s|--[^\n]*\n|/\*.*?\*/)*show\s+parameters\s+like\s+'(\w+)'\s+in\s+session\s*;?(?:\s|/\*.*?\*/)*$", re.IGNORECASE),
        r"""select upper('\1') as "key", '' as "value", '' as "default", 'SESSION' as "level", '' as "description", 'STRING' as "type" """,
        "session parameters are empty",
    ),
    (
        # dbt reads existing grants before applying `grants:` / `copy_grants`: there are none locally
        re.compile(r"(?s)^(?:\s|--[^\n]*\n|/\*.*?\*/)*show\s+grants\s+on\s.*$", re.IGNORECASE),
        """select null as "created_on", null as "privilege", null as "granted_on", null as "name", null as "granted_to", """
        """null as "grantee_name", null as "grant_option", null as "granted_by" where false""",
        "no grants exist locally",
    ),
    (
        # A dynamic table is a query Snowflake keeps fresh by itself. Locally it is a table that
        # every run rebuilds: the lag, warehouse and refresh settings have nothing to act on.
        re.compile(r"(?s)\bcreate\s+(?:or\s+replace\s+)?(?:transient\s+)?dynamic\s+table\s+(?:if\s+not\s+exists\s+)?(\S+)\s.*?\bas\s*(?=\(|select\b|with\b)",
                   re.IGNORECASE),
        r"create or replace table \1 as ",
        "dynamic table -> table",
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


def _transaction(cursor, command: str, original):  # noqa: ANN001, ANN202
    """BEGIN, COMMIT and ROLLBACK, as a DuckDB transaction on the session. As in Snowflake, a DDL
    statement commits the transaction that is open. Returns the reply, or None for other statements."""
    conn = cursor._conn
    is_open = getattr(conn, "_fauxflake_txn", False)
    trailing = objects._TRAILING_COMMENT.sub("", command)
    if _TXN_BEGIN.match(trailing):
        if is_open:  # one left open (a failed run that never said ROLLBACK): Snowflake would carry on in it
            _end_transaction(cursor)
        cursor._duck_conn.execute("begin transaction")
        conn._fauxflake_txn, conn._fauxflake_journal = True, []
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    end = _TXN_END.match(trailing)
    if end:
        _drop_scratch(cursor)
        if is_open:
            conn._fauxflake_txn, conn._fauxflake_journal = False, []
            try:
                cursor._duck_conn.execute(end.group(1))
            except Exception:  # noqa: BLE001 - DuckDB already ended it (a failed statement aborts its transaction)
                if end.group(1).lower() == "commit":
                    raise
        return original(cursor, "select 'Statement executed successfully.' as \"status\"")
    if is_open and not _TXN_KEEPS.match(trailing):
        _end_transaction(cursor)  # DDL commits
    return None


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


_QUOTED_TEXT = re.compile(r"""('(?:[^'\\]|''|\\.)*'|"(?:[^"]|"")*"|\$\$.*?\$\$)""", re.DOTALL)


def _freeze_clock(sql: str) -> str:
    def sub(m: re.Match[str]) -> str:
        if m.group(0).lower().startswith("current_date"):
            return f"'{FROZEN_NOW:%Y-%m-%d}'::date"
        return f"'{FROZEN_NOW:%Y-%m-%d %H:%M:%S}'::timestamp"

    if "'" not in sql and '"' not in sql and "$$" not in sql:
        return _CLOCK.sub(sub, sql)
    # only outside string literals, quoted names and $$ bodies: 'macro.dbt.current_timestamp' is text
    parts = _QUOTED_TEXT.split(sql)
    return "".join(part if i % 2 else _CLOCK.sub(sub, part) for i, part in enumerate(parts))


def rewrite(sql: str) -> str:
    if FROZEN_NOW is not None:
        sql = _freeze_clock(sql)
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
    """Drop the temporary relations of one session, or of all of them."""
    for key in [k for k, held in TEMPORARY.items() if conn is None or held[0] == id(conn)]:
        _drop_temporary(duck if duck is not None else conn._duck_conn, key)


_ALIAS_ERROR = re.compile(r'Alias (?:with name )?"+([^"]+)"+ (?:exists, but aliases cannot be used|referenced(?: in a SELECT clause)? - but the expression has (?:side effects|a subquery))')
_MISSING_COLUMN = re.compile(r'Referenced column "([^"]+)" not found in FROM clause')
_AMBIGUOUS_ERROR = re.compile(r"""Ambiguous reference to column name "+([^"]+)"+ \(use: '"?([^'".]+)"?\.""")
_patched = False
FAILED_STATEMENTS: dict[str, str] = {}  # error -> the last statement that raised it (debug mode only)


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
    worker.join(limit)
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
        raise ProgrammingError(
            msg=f"fauxflake emulator: statement stopped after {limit:.0f}s (FAUXFLAKE_STATEMENT_TIMEOUT). On generated data "
                "this is usually a join or a date spine that explodes; look at the values the model reads.",
            errno=604, sqlstate="57014")
    if "error" in outcome:
        raise outcome["error"]


def _patch_fakesnow() -> None:
    global _patched
    if _patched:
        return
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
        try:
            self._fauxflake_outer = semantics.outer_flattens(command)
            self._fauxflake_recursive = semantics.outer_flattens(command, "recursive")
            self._fauxflake_layer, self._fauxflake_inline, self._fauxflake_owners = False, set(), {}
            self._fauxflake_qualify = False
            command = objects.session_functions(self, command)
            handled = _transaction(self, command, original)
            if handled is not None:
                return handled
            handled = objects.intercept(self, command, original)  # functions, procedures, streams, tasks, policies
            if handled is not None:
                return handled
            temporary = _temporary(self, command)
            for _ in range(8):
                try:
                    done = original(self, rewrite(command), *args, **kwargs)
                    if temporary:
                        made = _TEMP_RELATION.match(command)
                        TEMPORARY[temporary] = (id(self._conn), made.group("kind"), made.group("name"))
                    objects.consume_streams(self, command)
                    objects.remember(self)
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
                    elif ambiguous and ambiguous.group(1).upper() not in self._fauxflake_owners and " using" in command.lower():
                        self._fauxflake_owners[ambiguous.group(1).upper()] = ambiguous.group(2)  # shared column of a USING join
                    else:
                        raise
            done = original(self, rewrite(command), *args, **kwargs)
            objects.consume_streams(self, command)
            objects.remember(self)
            return done
        except ProgrammingError as e:
            if e.errno == 2003 and "does not exist" in str(e.msg) and "not authorized" not in str(e.msg):
                # Snowflake's wording, which clients look for: dbt reads "no such table" as "no columns yet"
                self._sqlstate = "42S02"
                raise ProgrammingError(msg=f"{e.raw_msg or e.msg}\nObject does not exist or not authorized.",
                                       errno=2003, sqlstate="42S02") from e
            raise
        except Exception as e:  # noqa: BLE001
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
                return JSONResponse(body, status_code=response.status_code)

            server.app.router.routes[i] = Route(route.path, login_request, methods=list(route.methods or ["POST"]))
    from starlette.responses import JSONResponse

    describe = server.describe_as_rowtype

    def describe_as_rowtype(described):  # noqa: ANN001
        try:
            return describe(described)
        except Exception as e:  # noqa: BLE001
            raise ProgrammingError(msg=f"fauxflake emulator: unsupported result type: {e}",
                                   errno=9997, sqlstate="XX000") from e

    server.describe_as_rowtype = describe_as_rowtype

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
        .replace("columns.data_type='BIGINT'", f"columns.data_type in {whole}").replace("columns.data_type='DOUBLE'", "columns.data_type in ('DOUBLE', 'FLOAT')") \
        .replace("then NULL else columns.numeric_scale end", f"then NULL when columns.data_type in {whole} then 0 else columns.numeric_scale end")

    # the columns Snowflake's views have beyond those: precision of times, and the rest as Snowflake
    # reports them for an ordinary column
    nulls = ["CHARACTER_SET_CATALOG", "CHARACTER_SET_SCHEMA", "CHARACTER_SET_NAME", "COLLATION_CATALOG", "COLLATION_SCHEMA",
             "DOMAIN_CATALOG", "DOMAIN_SCHEMA", "DOMAIN_NAME", "UDT_CATALOG", "UDT_SCHEMA", "UDT_NAME", "SCOPE_CATALOG",
             "SCOPE_SCHEMA", "SCOPE_NAME", "MAXIMUM_CARDINALITY", "DTD_IDENTIFIER", "INTERVAL_TYPE", "INTERVAL_PRECISION",
             "IDENTITY_MAXIMUM", "IDENTITY_MINIMUM", "EXPRESSION", "SCHEMA_EVOLUTION_RECORD"]
    more = (",\n    case when columns.data_type like 'TIME%' then least(coalesce(columns.datetime_precision, 9), 9) end as DATETIME_PRECISION,"
            "\n    'COLUMN' as KIND, 'NO' as IS_SELF_REFERENCING, " + ", ".join(f"null::VARCHAR as {c}" for c in nulls))
    if "as DATETIME_PRECISION" not in info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW:
        info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW = info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW.replace(
            "null::VARCHAR as DATA_TYPE_ALIAS", "null::VARCHAR as DATA_TYPE_ALIAS" + more)
    tables_view = info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW.template
    if '"ERROR_LOGGING"' not in tables_view:
        info.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = Template(tables_view.replace(
            "'NO' AS \"IS_HYBRID\"", "'NO' AS \"IS_HYBRID\", 'NO' AS \"ERROR_LOGGING\", 'NO' AS \"IS_INTERACTIVE\", 'NO' AS \"ROW_TIMESTAMP_ON\""))
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

    semantics.patch_upstream()
    import fakesnow.cursor as cursor_module

    upstream_parse = cursor_module.parse_one

    def parse_named(command):  # noqa: ANN001, ANN202
        return semantics.name_expressions(upstream_parse(command), command)

    cursor_module.parse_one = parse_named
    original_transform = FakeSnowflakeCursor._transform

    def _transform(self, expression, params):  # noqa: ANN001
        expression = objects.apply_policies(objects.tag_scans(objects.result_scans(expression, self), self), self)
        if getattr(self, "_fauxflake_qualify", False) and self._conn.database:
            expression = semantics.qualify_schemas(expression, self._conn.database)
        if getattr(self, "_fauxflake_owners", None):
            expression = semantics.qualify_using(expression, self._fauxflake_owners)
        if getattr(self, "_fauxflake_inline", None):
            expression = semantics.inline_aliases(expression, self._fauxflake_inline)
        if getattr(self, "_fauxflake_layer", False):
            expression = semantics.layer_windows(expression)
        out = original_transform(self, semantics.rewrite(expression), params)
        out = semantics.finish(semantics.parenthesize_arrows(out))
        out = semantics.keep_outer_rows(out, getattr(self, "_fauxflake_outer", []), getattr(self, "_fauxflake_recursive", []))
        if os.environ.get("FAUXFLAKE_SHOW_SQL"):  # what DuckDB is actually asked to run
            print(out.sql(dialect="duckdb"), file=sys.stderr)
        return out

    FakeSnowflakeCursor._transform = _transform

    original_explode = FakeSnowflakeCursor._transform_explode

    def _transform_explode(self, expression):  # noqa: ANN001
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

    def _heal(self) -> None:  # noqa: ANN001
        """In Snowflake a statement that fails leaves its transaction open. DuckDB aborts the whole
        transaction, so start another and repeat what the transaction had done so far."""
        conn = self._conn
        if not getattr(conn, "_fauxflake_txn", False) or getattr(conn, "_fauxflake_healing", False):
            return
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

    def _execute(self, transformed, params=None):  # noqa: ANN001
        def run(tree) -> None:  # noqa: ANN001
            conn = self._conn
            # MERGE works through a temporary table, which DuckDB counts as a second database. Inside a
            # transaction it is an ordinary table next to the target, dropped when the transaction ends.
            scratch = [t for t in tree.find_all(exp.Table) if t.name.upper() == "MERGE_CANDIDATES" and not t.db]
            if scratch and isinstance(tree, exp.Create):
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
                    if not getattr(conn, "_fauxflake_healing", False):
                        conn.__dict__.setdefault("_fauxflake_journal", []).append((tree.copy(), params))
                    return
            journaled = getattr(self._conn, "_fauxflake_txn", False) and not isinstance(tree, (exp.Query, exp.Describe, exp.Show))
            kept = tree.copy() if journaled else None
            try:
                original_execute(self, tree, params)
            except Exception as e:
                if kept is not None and _TWO_DATABASES in str(e) and not getattr(self._conn, "_fauxflake_healing", False):
                    # DuckDB keeps a transaction to one database, and this statement needs a second
                    # (MERGE works through a temporary table): what the transaction did so far is
                    # committed here, and the rest of it runs in autocommit
                    conn = self._conn
                    print("fauxflake: a transaction wrote to a second database; it was committed at that point and "
                          "the rest of it runs in autocommit.", file=sys.stderr)
                    conn._fauxflake_txn, conn._fauxflake_healing = False, True
                    try:
                        self._duck_conn.execute("rollback")
                        for done, values in list(getattr(conn, "_fauxflake_journal", [])):
                            original_execute(self, done.copy(), values)
                    finally:
                        conn._fauxflake_healing = False
                    conn._fauxflake_journal = []
                    original_execute(self, kept, params)
                    return
                _heal(self)
                raise
            conn = self._conn
            if kept is not None and getattr(conn, "_fauxflake_txn", False) and not getattr(conn, "_fauxflake_healing", False):
                conn.__dict__.setdefault("_fauxflake_journal", []).append((kept, params))

        def attempt() -> None:
            nonlocal transformed
            for adjust in (semantics.coerce_union_literals, semantics.resolve_equalities, semantics.shared_aliases):
                try:
                    transformed = adjust(transformed, self._duck_conn)
                except Exception:  # noqa: BLE001, S110 - an optional adjustment: the statement runs as written
                    pass
            _heal(self)  # a probe that failed inside a transaction aborted it
            remedies = {
                "times": lambda e: semantics.numbers_as_times(transformed, e) or semantics.offsets_as_written(transformed, e),
                "by_name": lambda e: semantics.view_past_a_schema_named_like_a_database(transformed, e),
                "grouped": lambda e: semantics.group_by_all_written_out(transformed) if "Cannot mix aggregates" in e else None,
                "unshadowed": lambda e: semantics.aliases_over_tables(transformed) if "STRUCT(" in e else None,
                "types": lambda e: semantics.repair_types(transformed, self._duck_conn, params)
                if "No function matches" in e or "Could not choose a best candidate" in e else None,
            }
            used: collections.Counter = collections.Counter()
            while True:
                try:
                    run(transformed)
                    return
                except ProgrammingError as e:
                    # one repair can uncover the next (an implicit conversion, then an alias named like a table)
                    for name, remedy in remedies.items():
                        if used[name] >= 2:
                            continue
                        used[name] += 1
                        repaired = remedy(str(e))
                        _heal(self)
                        if repaired is not None:
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


def _spill_directory(duck: duckdb.DuckDBPyConnection) -> str:
    """Where DuckDB writes what does not fit in memory. Its default is `.tmp` in the working directory,
    which is the user's dbt repository: use a private directory outside it, removed on exit."""
    import tempfile

    path = tempfile.mkdtemp(prefix="fauxflake-spill-")
    TEMP_DIRS.append(path)
    duck.execute("set temp_directory = '" + path.replace("'", "''") + "'")
    # A query that explodes on generated data (a join on a key that is always the same) would
    # otherwise write until the disk is full. Stop it at a bound, and never past a quarter of what is free.
    limit = os.environ.get("FAUXFLAKE_MAX_SPILL")
    if not limit:
        free = shutil.disk_usage(path).free
        limit = f"{max(256, min(8192, free // 4 // 2**20))}MiB"
    duck.execute(f"set max_temp_directory_size = '{limit}'")
    import atexit

    atexit.register(shutil.rmtree, path, ignore_errors=True)  # also when the run is interrupted
    return path


@contextlib.contextmanager
def engine() -> Iterator[Emulator]:
    """Only the DuckDB side, with no Snowflake endpoint: enough to generate and inspect data."""
    from fakesnow.instance import FakeSnow

    _patch_fakesnow()
    fs = FakeSnow(nop_regexes=NOPS)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
    semantics.reserved_words(fs.duck_conn)
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
    if db_path and not keep:
        Path(db_path).mkdir(parents=True, exist_ok=True)
        for stale in [*Path(db_path).glob("*.db"), *Path(db_path).glob("*.db.wal"), *Path(db_path).glob(objects.FUNCTIONS_FILE)]:
            stale.unlink()  # every run starts from an empty warehouse
    fs = FakeSnow(db_path=db_path, nop_regexes=NOPS)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
    semantics.reserved_words(fs.duck_conn)
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
