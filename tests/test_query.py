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
    # a file with several statements: each is run and printed
    two = tmp_path / "two.sql"
    two.write_text("select 1 as a; -- first\nselect 'x;y' as b;\n")
    both = runner.invoke(main, ["query", *args, "-f", str(two)])
    assert both.exit_code == 0 and both.output.count("(1 row)") == 2 and "x;y" in both.output, both.output
    # any dbt command runs against the kept warehouse, without regenerating or rebuilding
    one = runner.invoke(main, ["dbt", *args, "--project-dir", str(EXAMPLE), "test", "-s", "fct_orders"])
    assert one.exit_code == 0 and "generated" not in one.output, one.output
    missing = runner.invoke(main, ["query", "--db-path", str(tmp_path / "nothing"), "select 1"])
    assert missing.exit_code != 0 and "no warehouse" in missing.output


def test_a_statement_adjusted_once_is_adjusted_the_same_way_while_the_types_it_reads_stay(monkeypatch):
    from fauxflake import emulator, semantics
    from fauxflake.emulator import serve

    calls = []
    real = semantics.resolve_equalities

    def slow(tree, conn):
        calls.append(1)
        import time
        time.sleep(0.3)
        return real(tree, conn)

    monkeypatch.setattr(semantics, "resolve_equalities", slow)
    emulator.ADJUSTED.clear()
    with serve() as emu:
        conn = emu._fs.connect(database="D", schema="S")
        cur = conn.cursor()
        cur.execute("create database if not exists D")
        cur.execute("create schema if not exists D.S")
        cur.execute("""create or replace table D.S.T as select 1 as a, parse_json('"1"') as v""")
        query = "select t.a from D.S.T t join D.S.T u on t.a::varchar = u.v"
        def asked(run) -> int:
            before = len(calls)
            run()
            return len(calls) - before

        assert asked(lambda: cur.execute(query).fetchall()) == 1
        assert asked(lambda: cur.execute(query).fetchall()) == 0  # not asked again
        cur.execute("create or replace table D.S.T as select 1 as a, '1' as v")
        assert asked(lambda: cur.execute(query).fetchall()) == 1  # the types changed: asked again


def test_nullif_and_in_with_an_empty_string_take_a_variant_column_by_its_value():
    from fauxflake.emulator import serve

    with serve() as emu:
        conn = emu._fs.connect(database="D", schema="S")
        cur = conn.cursor()
        cur.execute("create database if not exists D")
        cur.execute("create schema if not exists D.S")
        cur.execute("""create or replace table D.S.T as select parse_json('{"a":"x"}'):a as j, parse_json('""') as e, 'k' as t, '' as u""")
        assert cur.execute("select nullif(j, ''), nullif(e, ''), nullif(t, ''), nullif(u, '') from D.S.T").fetchall() == [('"x"', None, "k", None)]
        assert cur.execute("select j in ('', 'x'), e in ('', 'x'), t in ('', 'q') from D.S.T").fetchall() == [(True, True, False)]


def test_a_statement_is_stopped_while_there_is_still_room_on_the_disk(monkeypatch):
    import pytest
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    monkeypatch.setenv("FAUXFLAKE_MIN_FREE_GB", "100000000")  # (no disk has this much free: every long statement stops)
    with serve() as emu:
        conn = emu._fs.connect(database="D", schema="S")
        cur = conn.cursor()
        cur.execute("create database if not exists D")
        cur.execute("create schema if not exists D.S")
        assert cur.execute("select 1").fetchall() == [(1,)]  # a quick statement is not looked at
        with pytest.raises(ProgrammingError, match="the disk is nearly full"):
            cur.execute("select count(*) from range(3000000) a, range(3000000) b where a.range + b.range = -1")
        assert cur.execute("select 2").fetchall() == [(2,)]  # and the session carries on


def test_a_result_beyond_a_signed_64_bit_integer_comes_back_as_a_number():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S"))
        assert conn.cursor().execute("select md5_number_lower64('abc'), md5_number_upper64('abc')").fetchall() == [
            (15462616177412505458, 10376663631224000432)]
