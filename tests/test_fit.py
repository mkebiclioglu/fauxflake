"""`init` on a project that tells static analysis almost nothing: the spec is learned by running it."""

from pathlib import Path

import yaml
from click.testing import CliRunner

from fauxflake.cli import main

PROJECT = Path(__file__).parent / "projects" / "fit_me"


def test_init_learns_the_spec_by_running_the_project(tmp_path):
    spec_path = tmp_path / "fauxflake.yml"
    runner = CliRunner()
    out = runner.invoke(main, ["init", "--project-dir", str(PROJECT), "--spec", str(spec_path)])
    assert out.exit_code == 0, out.output
    spec = yaml.safe_load(spec_path.read_text())
    columns = spec["tables"]["RAW.APP.EVENTS"]["columns"]
    # the source is only read with `select *`: its columns come from the models downstream
    assert {"EVENT_ID", "ACCOUNT_ID", "AMOUNT", "CREATED", "PAYLOAD", "PATH", "KIND", "REGION_CODE"} <= set(columns)
    assert columns["KIND"]["gen"] == "categorical" and "purchase" in columns["KIND"]["values"]  # `where kind = 'purchase'`
    assert columns["REGION_CODE"]["v"] == [40, 60]  # `between 40 and 60` keeps rows
    assert columns["PATH"]["template"].startswith("{n}/")  # split(path, '/')[0]::number
    assert columns["CREATED"]["gen"] in ("time_offset", "event_time")  # to_timestamp_ntz(created)
    assert columns["PAYLOAD"]["gen"] == "object"
    # a model that reads its own previous output by name starts from an empty table of the right shape
    existing = spec["existing"]["ANALYTICS.CI.ACCOUNT_WATERMARK"]
    assert existing["as"] == "table" and set(existing["columns"]) == {"ACCOUNT_ID", "LAST_SEEN"}
    # the function is created by the project's own hook, so it needs no placeholder
    assert "udfs" not in spec
    # a test about production data is listed, not hidden
    assert spec["dbt"]["expected_failures"] == ["no_small_totals"]
    assert "4/4 models" in out.output and "expected_failures" in out.output

    built = runner.invoke(main, ["build", "--project-dir", str(PROJECT), "--spec", str(spec_path), "--keep"])
    assert built.exit_code == 0, built.output
    rows = runner.invoke(main, ["query", "--project-dir", str(PROJECT), "--spec", str(spec_path),
                                "select count(*) as n, count(distinct size) as sizes from fct_events"])
    assert rows.exit_code == 0 and int(rows.output.strip().splitlines()[-2].split()[0]) > 0, rows.output
