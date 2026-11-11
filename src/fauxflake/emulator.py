"""Local Snowflake endpoint backed by DuckDB.

The real `dbt-snowflake` adapter connects here over the Snowflake wire protocol
(served by fakesnow, which transpiles Snowflake SQL to DuckDB with sqlglot).
The generator shares the same DuckDB instance in-process, so synthetic source
data is written at native DuckDB speed without going through the protocol.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import socket
import sys
import threading
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import duckdb

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
            r"\bcreate(\s+or\s+replace)?\s+(?:temporary|temp)\s+(table|view)\s+(?=[\w\"$]+\.[\w\"$]+\.[\w\"$]+)",
            re.IGNORECASE,
        ),
        r"create\1 \2 ",
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
    for when in merge.find_all(exp.When):
        then = when.args.get("then")
        if when.args.get("matched") or not isinstance(then, exp.Insert) or then.expression is None:
            continue
        for column in then.expression.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(alias))
                changed = True
    return merge.sql(dialect="snowflake") if changed else sql


# Statements acknowledged without being executed. The emulator keeps catalog
# metadata in a separate DuckDB database, and DuckDB cannot write to two
# databases in one transaction, so explicit transactions run as autocommit.
_LEAD = r"(?s)^(\s|--[^\n]*\n|/\*.*?\*/)*"
NOPS = [
    _LEAD + r"(begin|commit)\b",
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


def _freeze_clock(sql: str) -> str:
    def sub(m: re.Match[str]) -> str:
        if m.group(0).lower().startswith("current_date"):
            return f"'{FROZEN_NOW:%Y-%m-%d}'::date"
        return f"'{FROZEN_NOW:%Y-%m-%d %H:%M:%S}'::timestamp"

    return _CLOCK.sub(sub, sql)


def rewrite(sql: str) -> str:
    if FROZEN_NOW is not None:
        sql = _freeze_clock(sql)
    for pattern, replacement, _why in REWRITES:
        sql = pattern.sub(replacement, sql, count=1)
    if _MERGE.match(sql):
        sql = _qualify_merge_insert(sql)
    return sql


_ALIAS_ERROR = re.compile(r'Alias (?:with name )?"+([^"]+)"+ (?:exists, but aliases cannot be used|referenced(?: in a SELECT clause)? - but the expression has (?:side effects|a subquery))')
_MISSING_COLUMN = re.compile(r'Referenced column "([^"]+)" not found in FROM clause')
_AMBIGUOUS_ERROR = re.compile(r"""Ambiguous reference to column name "+([^"]+)"+ \(use: '"?([^'".]+)"?\.""")
_patched = False


def _patch_fakesnow() -> None:
    global _patched
    if _patched:
        return
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
            handled = objects.intercept(self, command, original)  # functions, procedures, streams, tasks, policies
            if handled is not None:
                return handled
            for _ in range(8):
                try:
                    done = original(self, rewrite(command), *args, **kwargs)
                    objects.consume_streams(self, command)
                    objects.remember(self)
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
        except ProgrammingError:
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

    # a SUM over integers is a HUGEINT in DuckDB and a NUMBER in Snowflake: describe it as one
    import fakesnow.info_schema as info

    whole = "('BIGINT', 'HUGEINT', 'INTEGER', 'SMALLINT', 'TINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT', 'UHUGEINT')"
    info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW = info.SQL_CREATE_GLOBAL_INFORMATION_SCHEMA_COLUMNS_VIEW \
        .replace("columns.data_type='BIGINT'", f"columns.data_type in {whole}").replace("columns.data_type='DOUBLE'", "columns.data_type in ('DOUBLE', 'FLOAT')") \
        .replace("then NULL else columns.numeric_scale end", f"then NULL when columns.data_type in {whole} then 0 else columns.numeric_scale end")

    semantics.patch_upstream()
    original_transform = FakeSnowflakeCursor._transform

    def _transform(self, expression, params):  # noqa: ANN001
        expression = objects.apply_policies(objects.result_scans(expression, self), self)
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

    original_execute = FakeSnowflakeCursor._execute

    def _execute(self, transformed, params=None):  # noqa: ANN001
        try:
            original_execute(self, transformed, params)
        except ProgrammingError as e:
            if "No function matches" not in str(e) and "Could not choose a best candidate" not in str(e):
                raise
            repaired = semantics.repair_types(transformed, self._duck_conn, params)
            if repaired is None:
                raise
            original_execute(self, repaired, params)
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


def define_udfs(emu: "Emulator", udfs: dict[str, str]) -> None:
    """Stand-ins for functions that exist only in your Snowflake account, from the spec's `udfs:`.
    Each is `name: "(args) as <DuckDB SQL>"` and is callable with or without qualification."""
    semantics.UDFS.clear()
    semantics.UDTFS.clear()
    # a placeholder written by `init` gives way to the project's own CREATE FUNCTION; a real stand-in does not
    objects.pin(emu, [n for n, body in udfs.items() if not body.rstrip().endswith("as null::varchar")])
    for name, body in udfs.items():
        emu.duck.execute(f"create or replace macro _fs_global.main._udf_{name}{body}")
        semantics.UDFS[name] = f"_udf_{name}"


CURRENT: "Emulator | None" = None  # the emulator whose Snowflake endpoint is being served


def free_port() -> int:
    with contextlib.closing(socket.socket(type=socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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
    try:
        emu = Emulator(port=0, duck=fs.duck_conn, _fs=fs)
        yield emu
    finally:
        objects.close(emu)
        fs.duck_conn.close()


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
        for stale in [*Path(db_path).glob("*.db"), *Path(db_path).glob("*.db.wal")]:
            stale.unlink()  # every run starts from an empty warehouse
    fs = FakeSnow(db_path=db_path, nop_regexes=NOPS)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
    semantics.reserved_words(fs.duck_conn)
    fs.duck_conn.execute("set global old_implicit_casting = true")  # text compares with numbers and dates, as in Snowflake
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
        yield emu
    finally:
        CURRENT = outer
        objects.close(emu)
        server.should_exit = True
        thread.join(timeout=5)
        fakesnow.server.shared_fs = previous
        fs.duck_conn.close()
