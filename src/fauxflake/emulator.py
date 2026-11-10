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

from . import semantics

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
]


_MERGE = re.compile(r"\s*(?:/\*.*?\*/\s*)*merge\s+into\b", re.IGNORECASE | re.DOTALL)


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
NOPS = [r"(?s)^(\s|--[^\n]*\n|/\*.*?\*/)*(begin|commit)\b"]


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
            return original(self, rewrite(command), *args, **kwargs)
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

    async def server_error(request, exc):  # noqa: ANN001
        return JSONResponse({"data": {"errorCode": "009996", "sqlState": "XX000"}, "code": "009996",
                             "message": f"fauxflake emulator: {exc.message}"[:2000], "success": False})

    server.app.exception_handlers[server.ServerError] = server_error
    server.app.middleware_stack = None

    semantics.patch_upstream()
    original_transform = FakeSnowflakeCursor._transform

    def _transform(self, expression, params):  # noqa: ANN001
        out = original_transform(self, semantics.rewrite(expression), params)
        out = semantics.parenthesize_arrows(out)
        out = semantics.keep_outer_rows(out, getattr(self, "_fauxflake_outer", []), getattr(self, "_fauxflake_recursive", []))
        if os.environ.get("FAUXFLAKE_SHOW_SQL"):  # what DuckDB is actually asked to run
            print(out.sql(dialect="duckdb"), file=sys.stderr)
        return out

    FakeSnowflakeCursor._transform = _transform

    original_execute = FakeSnowflakeCursor._execute

    def _execute(self, transformed, params=None):  # noqa: ANN001
        original_execute(self, transformed, params)
        semantics.coerce_result(self)

    FakeSnowflakeCursor._execute = _execute
    _patched = True


@dataclass
class Emulator:
    port: int
    duck: duckdb.DuckDBPyConnection
    _fs: object

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
    try:
        yield Emulator(port=0, duck=fs.duck_conn, _fs=fs)
    finally:
        fs.duck_conn.close()


@contextlib.contextmanager
def serve(port: int | None = None, db_path: str | Path | None = None, verbose: bool = False) -> Iterator[Emulator]:
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
    if db_path:
        Path(db_path).mkdir(parents=True, exist_ok=True)
        for stale in [*Path(db_path).glob("*.db"), *Path(db_path).glob("*.db.wal")]:
            stale.unlink()  # every run starts from an empty warehouse
    fs = FakeSnow(db_path=db_path, nop_regexes=NOPS)
    for macro in semantics.MACROS:
        fs.duck_conn.execute(macro)
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
    emu = Emulator(port=port, duck=fs.duck_conn, _fs=fs)
    outer, CURRENT = CURRENT, emu
    try:
        yield emu
    finally:
        CURRENT = outer
        server.should_exit = True
        thread.join(timeout=5)
        fakesnow.server.shared_fs = previous
        fs.duck_conn.close()
