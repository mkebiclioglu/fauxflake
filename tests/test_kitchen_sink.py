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
    assert (PROJECT / "dbt_packages" / "dbt_utils").exists()  # installed on first use
    results = simulate(Warehouse(emu, spec, 1.0), project, datetime(2025, 1, 21), days=3, log=lambda *_: None)
    snapshot = next(r for r in results if r.model == "users_snapshot (snapshot)")
    assert snapshot.ok and snapshot.incremental_rows > 400
    cur = emu.cursor()
    assert cur.execute("select count(*), count(distinct PAYMENT_KEY) from ANALYTICS.CI.FCT_PAYMENTS").fetchone()[0] > 6000
    assert cur.execute("select count(*) from ANALYTICS.CI.CALENDAR").fetchone()[0] == 59
    assert {c for c, *_ in cur.execute("describe ANALYTICS.CI.DIM_USERS").fetchall()} >= {"USER_ID", "EMAIL", "PLAN", "usd", "eur"}
    # materializations that only exist on Snowflake still produce their rows
    payments = cur.execute("select count(*) from ANALYTICS.CI.FCT_PAYMENTS").fetchone()[0]
    assert cur.execute("select count(*) from ANALYTICS.CI.CLUSTERED_PAYMENTS").fetchone()[0] == payments
    assert cur.execute("select sum(USERS) from ANALYTICS.CI.PLAN_SUMMARY").fetchone()[0] == \
        cur.execute("select count(*) from ANALYTICS.CI.DIM_USERS").fetchone()[0]  # a dynamic table
    assert cur.execute("select count(*) from ANALYTICS.CI.SECURE_USERS").fetchone()[0] > 0  # a secure view
    # Python models: one written with Snowpark, one with pandas
    users = cur.execute("select count(*) from ANALYTICS.CI.DIM_USERS").fetchone()[0]
    assert cur.execute("select sum(USERS) from ANALYTICS.CI.PY_PLAN_TOTALS").fetchone()[0] == users
    assert cur.execute("select count(*), count(*) filter (where HAS_EMAIL) from ANALYTICS.CI.PY_USER_FLAGS").fetchone() == (users, users)
