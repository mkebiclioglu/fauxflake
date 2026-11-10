"""dbt features real projects lean on: packages, seeds, snapshots, ephemeral models, source tests."""

from datetime import datetime
from pathlib import Path

from fauxflake.dbt import DbtProject, emulator_output
from fauxflake.generate import Warehouse
from fauxflake.simulate import simulate
from fauxflake.spec import Spec

PROJECT = Path(__file__).parent / "projects" / "kitchen_sink"


def test_dbt_utils_seeds_snapshots_and_ephemerals(emu):
    spec = Spec.load(PROJECT / "fauxflake.yml")
    project = DbtProject(PROJECT, emulator_output(emu.port, "ANALYTICS"))
    if not (PROJECT / "dbt_packages" / "dbt_utils").exists():
        assert project.invoke(["deps"], quiet=True)
    results = simulate(Warehouse(emu, spec, 1.0), project, datetime(2025, 1, 21), days=3, log=lambda *_: None)
    snapshot = next(r for r in results if r.model == "users_snapshot (snapshot)")
    assert snapshot.ok and snapshot.incremental_rows > 400
    cur = emu.cursor()
    assert cur.execute("select count(*), count(distinct PAYMENT_KEY) from ANALYTICS.CI.FCT_PAYMENTS").fetchone()[0] > 6000
    assert cur.execute("select count(*) from ANALYTICS.CI.CALENDAR").fetchone()[0] == 59
    assert {c for c, *_ in cur.execute("describe ANALYTICS.CI.DIM_USERS").fetchall()} >= {"USER_ID", "EMAIL", "PLAN", "usd", "eur"}
