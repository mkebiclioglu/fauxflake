"""Measure, rather than assume, that the emulator agrees with Snowflake.

The sources are synthetic, so they are safe to load into a real account. The
same rows go to both engines, the same dbt project builds on both, simulated
days pass on both, and every model is compared row for row.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .dbt import DbtProject
from .generate import Warehouse

TYPES = {"BIGINT": "NUMBER(38,0)", "INTEGER": "NUMBER(38,0)", "HUGEINT": "NUMBER(38,0)", "UBIGINT": "NUMBER(38,0)",
         "SMALLINT": "NUMBER(38,0)", "TINYINT": "NUMBER(38,0)", "DOUBLE": "FLOAT", "FLOAT": "FLOAT", "VARCHAR": "VARCHAR",
         "TIMESTAMP": "TIMESTAMP_NTZ", "TIMESTAMP_NS": "TIMESTAMP_NTZ", "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
         "DATE": "DATE", "TIME": "TIME", "BOOLEAN": "BOOLEAN", "JSON": "VARIANT", "BLOB": "BINARY"}


def _snowflake_type(kind: str) -> str:
    if kind.endswith("[]") or kind.startswith(("STRUCT", "MAP")):
        return "VARIANT"
    return TYPES.get(kind, kind.replace("DECIMAL", "NUMBER"))


def seed(conn, wh: Warehouse, after: datetime | None = None, log=print, workers: int = 8) -> None:
    """Copy the emulator's source tables (or the rows loaded since `after`) into Snowflake. Each table is written
    to a Parquet file here, one after the other (DuckDB is one connection), and loaded into Snowflake by a few
    threads at once (PUT and COPY wait on the network, not on this machine)."""
    from concurrent.futures import ThreadPoolExecutor

    with tempfile.TemporaryDirectory() as tmp:
        work = []
        for t in wh.spec.ordered():
            db, schema, table = t.name.split(".")
            loaded = t.role("loaded_at")
            since = None if t.raw.get("mode") == "upsert" or not loaded else after  # (rows that change in place: replaced)
            where = f"where {loaded} > '{since}'" if since else ""
            path = Path(tmp) / f"{db}.{schema}.{table}.parquet"
            n = wh.cur.execute(f"copy (select * from {t.name} {where}) to '{path}' (format parquet)").fetchone()[0]
            described = [(c, _snowflake_type(ty)) for c, ty, *_ in wh.cur.execute(f"describe {t.name}").fetchall()]
            work.append((t.name, db, schema, table, path, n, described, since is None))
        cur = conn.cursor()
        for db, schema in sorted({(w[1], w[2]) for w in work if w[7]}):
            try:
                cur.execute(f"create database if not exists {db}")
            except Exception:  # noqa: BLE001, S110 - a role that may not create databases: the schema says so if it is missing
                pass
            cur.execute(f"create schema if not exists {db}.{schema}")

        def load(item) -> str:  # noqa: ANN001
            name, db, schema, table, path, n, described, fresh = item
            c = conn.cursor()
            if fresh:
                c.execute(f"create or replace table {name} ({', '.join(f'{chr(34)}{col}{chr(34)} {ty}' for col, ty in described)})")
            if n:
                c.execute(f"put file://{path} @{db}.{schema}.%{table} auto_compress=false overwrite=true")
                # (a document travels as text and is parsed on arrival: loaded by name it would stay a string)
                picked = ", ".join(f'parse_json($1:"{col}"::varchar)' if ty == "VARIANT" else f'$1:"{col}"::{ty}' for col, ty in described)
                c.execute(f"copy into {name} from (select {picked} from @{db}.{schema}.%{table}) "
                          "file_format=(type=parquet use_logical_type=true) purge=true")
            c.close()
            return f"  seeded {name}: {n:,} rows"

        with ThreadPoolExecutor(workers) as pool:
            for line in pool.map(load, work):
                log(line)


def _fetch(conn, sql: str):
    """Query Snowflake into an Arrow table (None when empty), without needing pandas."""
    import pyarrow as pa

    cur = conn.cursor()
    rows = cur.execute(sql).fetchall()
    if not rows:
        return None
    names = [d[0] for d in cur.description]
    return pa.table({n: [r[i] for r in rows] for i, n in enumerate(names)})


@dataclass
class Parity:
    model: str
    emulator_rows: int
    snowflake_rows: int
    only_emulator: int
    only_snowflake: int
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.only_emulator == 0 and self.only_snowflake == 0 and not self.note


def _comparable(column: str, kind: str, source: str) -> str:
    """One column in a form both engines agree on: Snowflake's values arrive as Python objects
    (pretty-printed JSON text, floats that differ in the last bit)."""
    quoted = f'"{column}"'
    if kind == "JSON":
        return f"{quoted}::varchar as {quoted}"  # canonical text: see _canonical_columns
    if kind in ("DOUBLE", "FLOAT"):
        return f"format('{{:.10g}}', {quoted}::double) as {quoted}"
    return f"{quoted}::{kind} as {quoted}" if source == "theirs" else quoted


def _canonical(text: str | None) -> str | None:
    """JSON as both engines would agree on it: Snowflake stores object keys sorted and prints 1.0."""
    import json

    def tidy(v):
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, dict):
            return {k: tidy(x) for k, x in v.items()}
        if isinstance(v, list):
            return [tidy(x) for x in v]
        return v

    if text is None:
        return None
    try:
        return json.dumps(tidy(json.loads(text)), sort_keys=True, separators=(",", ":"))
    except ValueError:
        return text


def _canonical_columns(table, columns: list[str]):
    """The Arrow table with its JSON columns rewritten as canonical text."""
    import pyarrow as pa

    lookup = {name.upper(): name for name in table.column_names}
    for col in columns:
        name = lookup[col]
        values = [_canonical(None if v is None else str(v)) for v in table.column(name).to_pylist()]
        table = table.set_column(table.column_names.index(name), name, pa.array(values, pa.string()))
    return table


def compare(conn, emu, local: DbtProject, schema: str | None = None, remote_schema: str | None = None,
            only: set[str] | None = None) -> list[Parity]:
    """Each model on the emulator against the same model on Snowflake, row for row. Without
    `schema`, each model is read from the schema dbt built it in (the same on both sides)."""
    duck, out = emu.cursor(), []
    for node in sorted(local.models(), key=lambda n: n["name"]):
        if only is not None and node["unique_id"] not in only:
            continue
        name = node.get("alias") or node["name"]
        try:
            theirs = _fetch(conn, f'select * from {node["database"]}.{remote_schema or node["schema"]}.{name}')
            mine = f'{node["database"]}.{schema or node["schema"]}.{name}'
            types = {c.upper(): ty for c, ty, *_ in duck.execute(f"describe {mine}").fetchall()}
            if theirs is None:  # zero rows
                theirs_n, a, b = 0, duck.execute(f"select count(*) from {mine}").fetchone()[0], 0
            else:
                duck.register("_theirs", theirs)
                # compare in the emulator's types, so NUMBER(38,0) vs BIGINT is not a difference
                missing = set(types) ^ {c.upper() for c in theirs.column_names}
                if missing:
                    out.append(Parity(node["name"], 0, theirs.num_rows, 0, 0, f"columns differ: {sorted(missing)}"))
                    continue
                theirs_n = theirs.num_rows
                source = mine
                documents = [c for c, ty in types.items() if ty == "JSON"]
                if documents:  # compare JSON by content: key order and 1 vs 1.0 are not differences
                    duck.register("_theirs", _canonical_columns(theirs, documents))
                    cols = ", ".join(f'"{c}"::varchar as "{c}"' if c in documents else f'"{c}"' for c in types)
                    duck.register("_mine", _canonical_columns(duck.execute(f"select {cols} from {mine}").to_arrow_table(), documents))
                    source = "_mine"
                a, b = duck.execute(f"""
                    with t as (select {', '.join(_comparable(c, ty, 'theirs') for c, ty in types.items())} from _theirs),
                         m as (select {', '.join(_comparable(c, ty, 'mine') for c, ty in types.items())} from {source})
                    select (select count(*) from (select * from m except all select * from t)),
                           (select count(*) from (select * from t except all select * from m))""").fetchone()
            mine_n = duck.execute(f"select count(*) from {mine}").fetchone()[0]
            out.append(Parity(node["name"], mine_n, theirs_n, a, b))
        except Exception as e:  # noqa: BLE001
            out.append(Parity(node["name"], 0, 0, 0, 0, f"{type(e).__name__}: {str(e)[:160]}"))
    return out


def outcomes(project: DbtProject) -> dict[str, tuple[str, str, object]]:
    """What the last dbt invocation did with each node: (status, first line of the message, failures)."""
    import json

    path = project.target_path / "run_results.json"
    if not path.exists():
        return {}
    out = {}
    for r in json.loads(path.read_text()).get("results") or []:
        message = " ".join(str(r.get("message") or "").split())
        out[r["unique_id"]] = (str(r.get("status")), message[:300], r.get("failures"))
    return out


@dataclass
class Difference:
    node: str
    kind: str  # emulator_only_error | snowflake_only_error | test_result | rows
    detail: str


def differences(local: dict, remote: dict, rows: list[Parity]) -> list[Difference]:
    """Where the two engines did not do the same: a node that failed on one side only, a test whose result or
    failure count differs, a model whose rows differ."""
    out = []
    for uid in sorted(set(local) | set(remote)):
        mine, theirs = local.get(uid), remote.get(uid)
        if mine is None or theirs is None:
            continue
        broke = lambda o: o[0] in ("error", "runtime error")  # noqa: E731
        if broke(mine) and not broke(theirs):
            out.append(Difference(uid, "emulator_only_error", mine[1]))
        elif broke(theirs) and not broke(mine):
            out.append(Difference(uid, "snowflake_only_error", theirs[1]))
        elif uid.startswith("test.") and not broke(mine) and (mine[0], mine[2]) != (theirs[0], theirs[2]):
            out.append(Difference(uid, "test_result", f"emulator {mine[0]} ({mine[2]}), snowflake {theirs[0]} ({theirs[2]})"))
    for r in rows:
        if not r.ok:
            out.append(Difference(r.model, "rows", f"emulator={r.emulator_rows} snowflake={r.snowflake_rows} "
                                                    f"only_emulator={r.only_emulator} only_snowflake={r.only_snowflake} {r.note}"))
    return out


def run(conn, wh: Warehouse, local: DbtProject, remote: DbtProject, start: datetime, days: int,
        remote_schema: str = "FAUXFLAKE_PARITY", log=print) -> tuple[list[Parity], list[Difference]]:
    """The same sources on both engines, the same project built on both (every node, also after one fails), then
    what each did compared: which nodes failed on one side only, which tests answered differently, which models hold
    other rows. Day by day for incremental models."""
    import os

    found: list[Difference] = []
    rows: list[Parity] = []
    for i in range(days + 1):
        now = start + timedelta(days=i)
        prev = wh.now
        wh.advance_to(now)
        log(f"{now:%Y-%m-%d}  seeding Snowflake")
        seed(conn, wh, after=prev, log=log)
        full = ["--full-refresh"] if i == 0 else []
        os.environ["FAUXFLAKE_TESTS_DO_NOT_BLOCK"] = "1"  # (a failing test keeps nothing else from being built and compared)
        try:
            # the same schema name on both sides: some models embed their own relation names
            local.invoke(["build", *full], schema=remote_schema, quiet=True)
            mine = outcomes(local)
            remote.invoke(["build", *full], schema=remote_schema, quiet=True)
            theirs = outcomes(remote)
        finally:
            os.environ.pop("FAUXFLAKE_TESTS_DO_NOT_BLOCK", None)
        log(f"{now:%Y-%m-%d}  built on both engines")
        built = {uid for uid in mine if mine[uid][0] == "success" and theirs.get(uid, ("",))[0] == "success"}
        rows = compare(conn, wh.emu, local, only=built)  # (each model where dbt put it: the same schema on both sides)
        found = differences(mine, theirs, rows)
    return rows, found
