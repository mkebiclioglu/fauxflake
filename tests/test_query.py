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


def test_a_fault_of_the_emulator_is_reported_at_once_instead_of_retried(monkeypatch):
    import time

    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    import fakesnow.server
    from fauxflake.emulator import serve

    def broken(*a, **k):
        raise AssertionError("Timezone X not yet supported")

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        monkeypatch.setattr(fakesnow.server, "to_sf", broken)
        started = time.monotonic()
        with pytest.raises(ProgrammingError, match="internal error"):
            cur.execute("select 1").fetchall()
        assert time.monotonic() - started < 20


def test_text_beside_a_number_takes_the_number_also_when_the_select_reads_its_own_alias():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create database if not exists D")
        cur.execute("create schema if not exists D.S")
        cur.execute("create or replace table D.S.T as select '7' as quantity, 3 as seats, '2024-01-02' as started")
        got = cur.execute("""select started::date as began, case when began is not null then seats else quantity end as n,
                                    coalesce(seats, quantity) as m from D.S.T""").fetchall()
        assert [type(v).__name__ for v in got[0][1:]] == ["Decimal", "Decimal"], got


def test_the_session_keeps_its_query_tag_and_refuses_one_longer_than_snowflake_does():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        shown = lambda: cur.execute("show parameters like 'query_tag' in session").fetchall()[0][1]  # noqa: E731
        assert shown() == ""
        cur.execute("""alter session set query_tag = 'it''s {"a": "{\\"b\\": 1}"}'""")  # (read as Snowflake reads the literal)
        assert shown() == """it's {"a": "{"b": 1}"}"""
        cur.execute("alter session set query_tag = '" + "x" * 2000 + "'")
        with pytest.raises(ProgrammingError, match="invalid value"):
            cur.execute("alter session set query_tag = '" + "x" * 2001 + "'")
        assert shown() == "x" * 2000
        cur.execute("alter session unset query_tag")
        assert shown() == ""
        cur.execute('alter session set query_tag = \'t\'\n/* {"app": "dbt", "node_id": "model.p.m"} */')  # (dbt appends a comment)
        assert shown() == "t"


def test_a_quoted_name_is_a_column_only_in_the_case_it_is_written():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t (metric_name varchar, \"Mixed\" int)")
        cur.execute("insert into t values ('a', 1)")
        with pytest.raises(ProgrammingError, match="invalid identifier '\"Metric_Name\"'") as raised:
            cur.execute('select\n  "Metric_Name" from t')
        assert raised.value.errno == 904 and "error line 2 at position 2" in str(raised.value)
        with pytest.raises(ProgrammingError, match="invalid identifier 'MIXED'"):
            cur.execute("select mixed from t")
        assert cur.execute('select "METRIC_NAME", metric_name, "Mixed" from t').fetchall() == [("a", "a", 1)]
        # a name the statement gives itself, in its own case
        assert cur.execute('with c as (select metric_name as "Metric_Name" from t) select "Metric_Name" from c').fetchall() == [("a",)]
        assert cur.execute('select "Metric_Name" from (select metric_name as "Metric_Name" from t)').fetchall() == [("a",)]
        cur.execute('create table u as select metric_name as "start" from t')
        with pytest.raises(ProgrammingError, match="invalid identifier 'START'"):
            cur.execute("select start from u")
        assert cur.execute('select "start" from u').fetchall() == [("a",)]


def test_a_conversion_of_a_column_without_nulls_is_never_null_and_is_not_made():
    """Measured on Snowflake: each micro-partition knows how many NULLs a column holds, so `where n is null` over a
    conversion of a column with none reads nothing, and a value that does not convert is no error."""
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select md5(i::varchar) as x, iff(i = 3, null, md5(i::varchar)) as y, i "
                    "from (select seq4() as i from table(generator(rowcount => 35)))")
        cur.execute("create view v as select x::number as n, y::number as m, upper(x)::number as u, (x::number + 1) as p, "
                    "to_number(x) as tn, x::date as d, coalesce(x, 'a')::number as c, i from t")
        cur.execute("create view w as select n as n2, * from v")
        count = lambda sql: cur.execute(sql).fetchone()[0]  # noqa: E731
        for where in ("n is null", "tn is null", "d is null", "n is null and i > 5", "n is null or i > 50", "i > 50"):
            assert count(f"select count(*) from v where {where}") == 0, where
        assert count("select count(*) from w where n2 is null") == 0
        assert count("select count(*) from (select n from v where n is null) dbt_internal_test") == 0
        assert count("select count(*) from t where x::number is null") == 0
        assert count("with s as (select x::number as n from t) select count(*) from s where n is null") == 0
        assert count("select count(*) from (select distinct x::number as n from t) where n is null") == 0
        assert count("select count(*) from (select a.x::number as n from t a join t b on a.i = b.i) where n is null") == 0
        assert count("select count(*) from (select b.x::number as n from t a left join t b on a.i = b.i + 100) where n is null") == 35
        for where in ("m is null", "u is null", "p is null", "c is null", "n is not null", "n = 5"):
            with pytest.raises(ProgrammingError):
                cur.execute(f"select count(*) from v where {where}").fetchall()
        with pytest.raises(ProgrammingError):
            cur.execute("select count(*) from (select x::number as n from t limit 100) where n is null").fetchall()
        for blocked in ("with s as (select * from t qualify row_number() over (partition by i order by i) = 1), "  # (GitLab's
                        "r as (select distinct x::number as n from s) select count(*) from r where n is null",       # orders view)
                        "select count(*) from (select x::number as n, row_number() over (order by i) r from t) where n is null and r > 0",
                        "select count(*) from (select x::number as n from t group by x) where n is null"):
            with pytest.raises(ProgrammingError):
                cur.execute(blocked).fetchall()
        assert count("with s as (select * from t qualify row_number() over (partition by x order by i) = 1), "
                     "r as (select x::number as n from s) select count(*) from r where n is null") == 0
        # a secure view that is a WITH keeps the filter out (GitLab's PII views); one without a WITH does not, and a
        # view made again without SECURE is an ordinary one
        cur.execute("create secure view sw as with s as (select * from v) select n from s")
        cur.execute("create secure view sp as select * from (select n from v)")
        assert count("select count(*) from sp where n is null") == 0
        with pytest.raises(ProgrammingError):
            cur.execute("select count(*) from sw where n is null").fetchall()
        cur.execute("create or replace view sw as with s as (select * from v) select n from s")
        assert count("select count(*) from sw where n is null") == 0


def test_a_view_that_converts_a_timestamp_to_a_number_is_refused_when_it_is_created():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t (t timestamp_ntz, d date, n number, s varchar, j variant)")  # (no rows: nothing is read)
        for refused, errno in (("t::float", 1007), ("to_numeric(t)", 1007), ("t::number", 1007), ("d::int", 1007),
                               ("t::boolean", 1007), ("dateadd('s', t, '1970-01-01')", 1044), ("upper(s)::number + d::number", 1007),
                               ("try_to_timestamp_ntz(t)::date", 1065), ("try_to_date(t)", 1065), ("try_to_number(d)", 1065)):
            with pytest.raises(ProgrammingError) as raised:
                cur.execute(f"create or replace view v as select {refused} as x from t")
            assert raised.value.errno == errno, refused
        for fine in ("n::float", "s::number", "to_numeric(s)", "j::number", "s::boolean", "n::boolean", "dateadd('s', n, '1970-01-01')",
                     "dateadd('s', s, '1970-01-01')", "date_part(epoch_second, t)::number", "t::date", "t::varchar",
                     "try_to_date(d)", "try_to_timestamp(s)", "try_to_number(s)"):
            cur.execute(f"create or replace view v as select {fine} as x from t")


def test_a_table_or_view_with_a_column_named_twice_is_refused():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select 1 as id, 2 as dbt_scd_id")
        for kind in ("table", "view"):
            with pytest.raises(ProgrammingError, match="duplicate column name 'DBT_SCD_ID'") as raised:
                cur.execute(f"create or replace {kind} u as select *, md5(id) as dbt_scd_id from t")
            assert raised.value.errno == 2025
        with pytest.raises(ProgrammingError, match="duplicate column name 'DBT_SCD_ID'"):  # (as dbt makes a snapshot)
            cur.execute("create or replace table u as (select *, md5(id) as dbt_scd_id from (select * from t) sbq)")
        cur.execute("create or replace view u as select * from t")


def test_an_expression_without_a_name_is_named_as_written_also_in_a_cte_a_table_takes():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table src as select 1 delta, '2025-01-01' pubdate")
        cur.execute("create table t as (with s as (select * from src), r as (select pubdate::date as d, delta::NUMBER, "
                    "coalesce(delta, 0) + 1, upper(pubdate), (delta) from s) select * from r)")
        names = lambda sql: [d[0] for d in cur.execute(sql).description]  # noqa: E731
        assert names("select * from t") == ["D", "DELTA::NUMBER", "COALESCE(DELTA, 0) + 1", "UPPER(PUBDATE)", "DELTA"]
        assert names("select a.* from (select count(*), max(delta) + 1 from src) a") == ["COUNT(*)", "MAX(DELTA) + 1"]
        assert names("select * from (select (select count(*) from src), delta + 1 from src)") == ["(SELECT COUNT(*) FROM SRC)", "DELTA + 1"]


def test_the_frozen_clock_is_an_instant_that_a_time_zone_shows_as_its_wall_clock(monkeypatch):
    from datetime import datetime

    import snowflake.connector

    from fauxflake import emulator
    from fauxflake.emulator import serve

    with serve() as emu:
        monkeypatch.setattr(emulator, "FROZEN_NOW", datetime(2026, 1, 15, 12, 0, 0))  # (serve() starts with a live clock)
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        (la,) = cur.execute("select convert_timezone('America/Los_Angeles', current_timestamp())::timestamp_ntz::varchar").fetchone()
        assert la.startswith("2026-01-15 04:00:00")
        (utc,) = cur.execute("select sysdate()::varchar").fetchone()
        assert utc.startswith("2026-01-15 12:00:00")


def test_a_bare_clock_keyword_is_named_as_written_with_the_clock_live_or_frozen(monkeypatch):
    from datetime import date, datetime

    import snowflake.connector

    from fauxflake import emulator
    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("select current_date, current_timestamp, localtimestamp, 1")  # (DuckDB took CURRENT_DATE for the name)
        assert [d[0] for d in cur.description] == ["CURRENT_DATE", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "1"]
        monkeypatch.setattr(emulator, "FROZEN_NOW", datetime(2026, 1, 15, 12, 0, 0))
        cur.execute("create or replace table t as select 1 as a, current_date, current_timestamp() from (select 1)")
        cur.execute("select * from t")
        assert [d[0] for d in cur.description] == ["A", "CURRENT_DATE", "CURRENT_TIMESTAMP()"]  # (not '2026-01-15'::DATE)
        assert cur.fetchone()[1] == date(2026, 1, 15)


def test_a_converted_time_converts_again_from_its_instant_and_a_zoned_one_is_refused_where_snowflake_refuses_it():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        (la,) = cur.execute("select convert_timezone('America/Los_Angeles', convert_timezone('UTC', "
                            "'2025-01-01 12:00:00 +00:00'::timestamp_tz))::timestamp_ntz::varchar").fetchone()
        assert la.startswith("2025-01-01 04:00:00")
        (ntz,) = cur.execute("select convert_timezone('Asia/Tokyo', '2025-01-01 12:00:00'::timestamp_ntz)::timestamp_ntz::varchar").fetchone()
        assert ntz.startswith("2025-01-01 21:00:00")  # (read in the session's zone, UTC here)
        with pytest.raises(ProgrammingError, match="Invalid argument types for function 'CONVERT_TIMEZONE'") as raised:
            cur.execute("create view v as select convert_timezone('UTC', 'Asia/Tokyo', '2025-01-01 12:00:00 +00:00'::timestamp_tz) t")
        assert raised.value.errno == 1044


def test_information_schema_tables_lists_views_as_snowflake_does():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select 1 a")
        cur.execute("create view v as select a, 'x'::varchar b from t")
        rows = cur.execute("select t.table_name, t.table_type, t.row_count, c.column_name, c.data_type from d.information_schema.tables t "
                           "join d.information_schema.columns c on c.table_schema = t.table_schema and c.table_name = t.table_name "
                           "where t.table_schema = 'S' order by 1, 4").fetchall()
        assert rows == [("T", "BASE TABLE", 1, "A", "NUMBER"), ("V", "VIEW", None, "A", "NUMBER"), ("V", "VIEW", None, "B", "TEXT")]


def test_a_merge_that_updates_a_row_from_two_source_rows_is_refused_as_snowflake_does():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t (id int, v varchar)")
        cur.execute("insert into t values (1, 'a'), (2, 'b')")
        cur.execute("create table s (id int, v varchar)")
        cur.execute("insert into s values (1, 'x'), (1, 'y'), (3, 'z')")
        with pytest.raises(ProgrammingError, match="Duplicate row detected") as raised:
            cur.execute("merge into t using s on t.id = s.id when matched then update set v = s.v when not matched then insert (id, v) values (s.id, s.v)")
        assert raised.value.errno == 100090
        assert cur.execute("select * from t order by id").fetchall() == [(1, "a"), (2, "b")]  # (nothing done)
        # deleting a row matched twice is deterministic
        cur.execute("merge into t using s on t.id = s.id when matched then delete")
        assert cur.execute("select * from t order by id").fetchall() == [(2, "b")]
        cur.execute("merge into t using (select 2 as id, 'q' as v) s on t.id = s.id when matched then update set v = s.v")
        assert cur.execute("select * from t order by id").fetchall() == [(2, "q")]
