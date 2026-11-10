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

TYPES = {"BIGINT": "NUMBER(38,0)", "INTEGER": "NUMBER(38,0)", "DOUBLE": "FLOAT", "VARCHAR": "VARCHAR",
         "TIMESTAMP": "TIMESTAMP_NTZ", "DATE": "DATE", "BOOLEAN": "BOOLEAN"}


def seed(conn, wh: Warehouse, after: datetime | None = None, log=print) -> None:
    """Copy the emulator's source tables (or the rows loaded since `after`) into Snowflake."""
    cur = conn.cursor()
    with tempfile.TemporaryDirectory() as tmp:
        for t in wh.spec.ordered():
            db, schema, table = t.name.split(".")
            loaded = t.role("loaded_at")
            if t.raw.get("mode") == "upsert" or not loaded:
                after = None  # rows change in place, so the table is replaced rather than appended to
            where = f"where {loaded} > '{after}'" if after else ""
            path = Path(tmp) / f"{table}.parquet"
            n = wh.cur.execute(f"copy (select * from {t.name} {where}) to '{path}' (format parquet)").fetchone()[0]
            if after is None:
                cols = ", ".join(
                    f'"{c}" {TYPES.get(ty, ty.replace("DECIMAL", "NUMBER"))}'
                    for c, ty, *_ in wh.cur.execute(f"describe {t.name}").fetchall())
                cur.execute(f"create schema if not exists {db}.{schema}")
                cur.execute(f"create or replace table {t.name} ({cols})")
            cur.execute(f"put file://{path} @{db}.{schema}.%{table} auto_compress=false overwrite=true")
            cur.execute(f"copy into {t.name} from @{db}.{schema}.%{table} "
                        "file_format=(type=parquet use_logical_type=true) "
                        "match_by_column_name=case_insensitive purge=true")
            log(f"  seeded {t.name}: {n:,} rows")


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


def compare(conn, emu, local: DbtProject, schema: str | None = None, remote_schema: str | None = None) -> list[Parity]:
    """Each model on the emulator against the same model on Snowflake, row for row. Without
    `schema`, each model is read from the schema dbt built it in (the same on both sides)."""
    duck, out = emu.cursor(), []
    for node in sorted(local.models(), key=lambda n: n["name"]):
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


def run(conn, wh: Warehouse, local: DbtProject, remote: DbtProject, start: datetime, days: int,
        remote_schema: str = "FAUXFLAKE_PARITY", log=print) -> list[Parity]:
    for i in range(days + 1):
        now = start + timedelta(days=i)
        prev = wh.now
        wh.advance_to(now)
        log(f"{now:%Y-%m-%d}  seeding Snowflake")
        seed(conn, wh, after=prev, log=log)
        full = ["--full-refresh"] if i == 0 else []
        # the same schema name on both sides: some models embed their own relation names
        if not local.invoke(["build", *full], schema=remote_schema, quiet=True):
            raise SystemExit(f"dbt build failed on the emulator at {now}")
        if not remote.invoke(["build", *full], schema=remote_schema, quiet=True):
            raise SystemExit(f"dbt build failed on Snowflake at {now}")
        log(f"{now:%Y-%m-%d}  built on both engines")
    return compare(conn, wh.emu, local, remote_schema, remote_schema)
