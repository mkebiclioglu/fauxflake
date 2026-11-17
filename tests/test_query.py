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


def test_convert_timezone_with_two_arguments_gives_the_time_of_day_there():
    """Answers recorded on Snowflake in a session whose TIMEZONE is UTC, as the emulator's is."""
    from datetime import date, datetime

    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
        row = conn.cursor().execute("""
            select convert_timezone('America/New_York', '2024-03-10 12:00:00'::timestamp_ntz)::timestamp_ntz,
                   convert_timezone('America/New_York', '2024-07-10 02:00:00'::timestamp_ntz)::date,
                   hour(convert_timezone('America/New_York', '2024-07-10 12:00:00'::timestamp_ntz)),
                   date_trunc('day', convert_timezone('Asia/Tokyo', '2024-07-10 20:00:00'::timestamp_ntz))::date,
                   to_char(convert_timezone('Europe/Berlin', '2024-01-10 23:30:00'::timestamp_ntz), 'YYYY-MM-DD HH24:MI'),
                   convert_timezone('America/New_York', '2024-03-10 12:00:00 +0100'::timestamp_tz)::timestamp_ntz,
                   convert_timezone('America/New_York', '2024-03-10'::date)::timestamp_ntz,
                   convert_timezone('America/New_York', x)::timestamp_ntz,
                   '2024-03-10 12:00:00 +0000'::timestamp_tz::varchar
            from (select '2024-03-10 12:00:00'::varchar x)""").fetchone()
        conn.close()
    assert row == (datetime(2024, 3, 10, 8), date(2024, 7, 9), 8, date(2024, 7, 11), "2024-01-11 00:30", datetime(2024, 3, 10, 7),
                   datetime(2024, 3, 9, 19), datetime(2024, 3, 10, 8), "2024-03-10 12:00:00.000 Z")
