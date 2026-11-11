from pathlib import Path

from click.testing import CliRunner

from fauxflake.cli import main

EXAMPLE = Path(__file__).resolve().parents[1] / "example"


def test_query_reads_a_kept_warehouse(tmp_path):
    runner = CliRunner()
    args = ["--spec", str(EXAMPLE / "fauxflake.yml"), "--db-path", str(tmp_path / "wh")]
    built = runner.invoke(main, ["build", *args, "--project-dir", str(EXAMPLE), "--scale", "0.0002"])
    assert built.exit_code == 0, built.output
    out = runner.invoke(main, ["query", *args, "select count(*) as n, count(distinct status) as statuses from fct_orders"])
    assert out.exit_code == 0, out.output
    header, _rule, row, footer = out.output.strip().splitlines()[-4:]
    assert header.split() == ["N", "STATUSES"] and int(row.split()[0]) > 0 and footer == "(1 row)"
    # the clock stands where the build left it; a model's compiled SQL can be run by name
    clock = runner.invoke(main, ["query", *args, "select datediff('day', (select max(ordered_at)::date from fct_orders), current_date()) as days"])
    assert clock.exit_code == 0 and clock.output.strip().splitlines()[-2].strip() in ("0", "1"), clock.output
    by_model = runner.invoke(main, ["query", *args, "--project-dir", str(EXAMPLE), "--model", "fct_orders", "--limit", "1"])
    assert by_model.exit_code == 0 and "more not shown" in by_model.output, by_model.output
    # any dbt command runs against the kept warehouse, without regenerating or rebuilding
    one = runner.invoke(main, ["dbt", *args, "--project-dir", str(EXAMPLE), "test", "-s", "fct_orders"])
    assert one.exit_code == 0 and "generated" not in one.output, one.output
    missing = runner.invoke(main, ["query", "--db-path", str(tmp_path / "nothing"), "select 1"])
    assert missing.exit_code != 0 and "no warehouse" in missing.output
