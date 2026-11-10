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


def compare(conn, wh: Warehouse, local: DbtProject, schema: str, remote_schema: str) -> list[Parity]:
    duck, out = wh.emu.cursor(), []
    for node in sorted(local.models(), key=lambda n: n["name"]):
        name = node.get("alias") or node["name"]
        try:
            theirs = _fetch(conn, f'select * from {node["database"]}.{remote_schema}.{name}')
            mine = f'{node["database"]}.{schema}.{name}'
            types = {c.upper(): ty for c, ty, *_ in duck.execute(f"describe {mine}").fetchall()}
            if theirs is None:  # zero rows
                theirs_n, a, b = 0, duck.execute(f"select count(*) from {mine}").fetchone()[0], 0
            else:
                duck.register("_theirs", theirs)
                # compare in the emulator's types, so NUMBER(38,0) vs BIGINT is not a difference
                cast = ", ".join(f'"{c}"::{ty} as "{c}"' for c, ty in types.items())
                missing = set(types) ^ {c.upper() for c in theirs.column_names}
                if missing:
                    out.append(Parity(node["name"], 0, theirs.num_rows, 0, 0, f"columns differ: {sorted(missing)}"))
                    continue
                theirs_n = theirs.num_rows
                a, b = duck.execute(f"""
                    with t as (select {cast} from _theirs), m as (select {', '.join(f'"{c}"' for c in types)} from {mine})
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
        if not local.invoke(["build", *full], quiet=True):
            raise SystemExit(f"dbt build failed on the emulator at {now}")
        if not remote.invoke(["build", *full], schema=remote_schema, quiet=True):
            raise SystemExit(f"dbt build failed on Snowflake at {now}")
        log(f"{now:%Y-%m-%d}  built on both engines")
    return compare(conn, wh, local, "CI", remote_schema)
