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
    missing = runner.invoke(main, ["query", "--db-path", str(tmp_path / "nothing"), "select 1"])
    assert missing.exit_code != 0 and "no warehouse" in missing.output
