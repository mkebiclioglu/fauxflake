"""Let simulated time pass and check that incremental models stay correct.

Run the project day after day while new, late and updated rows keep arriving,
then rebuild everything from scratch and compare. Any difference means an
incremental model has drifted from what a full refresh would produce: the class
of bug that only shows up in production, weeks after the merge.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from .dbt import DbtProject
from .emulator import Emulator
from .generate import Warehouse


@dataclass
class Drift:
    model: str
    incremental_rows: int
    rebuilt_rows: int
    only_incremental: int
    only_rebuilt: int

    @property
    def ok(self) -> bool:
        return self.only_incremental == 0 and self.only_rebuilt == 0


def compare(emu: Emulator, project: DbtProject, schema: str, rebuilt_schema: str) -> list[Drift]:
    cur, out = emu.cursor(), []
    for node in project.incremental_models():
        name = node.get("alias") or node["name"]
        a, b = (f'{node["database"]}.{s}.{name}' for s in (schema, rebuilt_schema))
        row = cur.execute(f"""
            select (select count(*) from {a}), (select count(*) from {b}),
                   (select count(*) from (select * from {a} except all select * from {b})),
                   (select count(*) from (select * from {b} except all select * from {a}))
        """).fetchone()
        out.append(Drift(node["name"], *row))
    return out


def simulate(wh: Warehouse, project: DbtProject, start: datetime, days: int, step_hours: float = 24,
             dbt_args: list[str] | None = None, log=print, failures: list[datetime] | None = None) -> list[Drift]:
    """With `failures`, a failing dbt build is recorded there instead of ending the run."""
    dbt_args = dbt_args or []
    t0 = time.time()
    counts = wh.advance_to(start)
    log(f"{start:%Y-%m-%d %H:%M}  loaded {sum(counts.values()):>12,} rows  ({time.time() - t0:.1f}s)")
    if not project.invoke(["build", *dbt_args]):
        raise SystemExit("dbt build failed on the initial load")

    steps = round(days * 24 / step_hours)
    for i in range(1, steps + 1):
        now = start + timedelta(hours=step_hours * i)
        t0 = time.time()
        counts = wh.advance_to(now)
        t1 = time.time()
        if not project.invoke(["build", *dbt_args], quiet=True):
            if failures is None:
                raise SystemExit(f"dbt build failed at simulated time {now}")
            failures.append(now)
        log(f"{now:%Y-%m-%d %H:%M}  +{sum(counts.values()):>11,} rows  "
            f"(generate {t1 - t0:.1f}s, dbt {time.time() - t1:.1f}s)")

    log("rebuilding from scratch to compare ...")
    if not project.invoke(["run", "--full-refresh", *dbt_args], schema="CI_REBUILD", quiet=True):
        raise SystemExit("dbt full refresh failed")
    return compare(wh.emu, project, "CI", "CI_REBUILD")
