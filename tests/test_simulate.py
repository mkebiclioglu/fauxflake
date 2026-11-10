"""End to end: the real dbt-snowflake adapter, the example project, simulated weeks."""

from datetime import datetime

from fauxflake.dbt import DbtProject
from fauxflake.generate import Warehouse
from fauxflake.simulate import simulate

from conftest import EXAMPLE


def run(emu, spec, dbt_args):
    wh = Warehouse(emu, spec, scale=0.0005)
    project = DbtProject(EXAMPLE, emu.port, "ANALYTICS")
    drifts = simulate(wh, project, datetime(2025, 1, 21), days=4, dbt_args=dbt_args, log=lambda *_: None)
    return {d.model: d for d in drifts}


def test_correct_incremental_models_do_not_drift(emu, spec):
    drifts = run(emu, spec, [])
    assert set(drifts) == {"fct_orders", "fct_daily_events"}
    assert all(d.ok for d in drifts.values()), drifts
    assert drifts["fct_orders"].incremental_rows > 1000


def test_ignoring_late_data_is_caught(emu, spec):
    drifts = run(emu, spec, ["--vars", "{late_window_days: 0}"])
    assert drifts["fct_orders"].ok
    assert not drifts["fct_daily_events"].ok
