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
        # nor does DuckDB's own filter get in: a column that fails on every row read, under a filter on a value
        # Snowflake does not follow into a secure view (LOWER of a column: measured, it fails)
        cur.execute("create secure view sj as with a as (select distinct lower(t.x) as k, t.x::number as m from t) select * from a")
        with pytest.raises(ProgrammingError):
            cur.execute("select count(*) from sj where k is null").fetchall()
        # and the fence keeps every value (it once read RANDOM(), which became Snowflake's 64-bit one: half came out NULL)
        cur.execute("create secure view sk as with s as (select i from t) select i from s")
        assert count("select count(*) from sk where i is null") == 0
        assert count("select count(*) from sk") == count("select count(*) from t") > 1


def test_a_filter_goes_into_a_secure_view_while_what_it_tests_cannot_fail():
    # measured on Snowflake (2,000 rows, one text that is no number): `is null` reaches the table through a secure
    # view that is a WITH when it tests a column, a number made a number or arithmetic of one, and the rows it keeps
    # out are never made; a conversion of text stops it, and the view is read whole
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
        cur = conn.cursor()
        count = lambda sql: cur.execute(sql).fetchone()[0]  # noqa: E731
        cur.execute("create table pt (id number, x varchar, u number, tx varchar) as select seq4(), "
                    "iff(seq4() = 0, 'a', seq4()::varchar), seq4(), seq4()::varchar from table(generator(rowcount => 2000))")
        views = {
            "s_d_id": "with s as (select distinct id::number n, x::number m from pt) select * from s",
            "s_d_txt": "with s as (select distinct tx::number n, x::number m from pt) select * from s",
            "s_d_plain": "with s as (select distinct id n, x::number m from pt) select * from s",
            "s_d_expr": "with s as (select distinct id + 1 n, x::number m from pt) select * from s",
            "s_d_nullif": "with s as (select distinct id n, nullif(u, '') k from pt) select * from s",
            "s_qual": "with s0 as (select * from pt qualify row_number() over (partition by id order by u desc) = 1), "
                      "s as (select distinct id::number n, nullif(u, '') k from s0) select * from s",
            "s_j": "with a as (select t.x as k from pt t join pt u on t.x::number = u.id) select k from a",
            "s_md_x": "with s as (select distinct md5(tx) n, x::number m from pt) select * from s",
            "s_low_x": "with s as (select distinct lower(tx) n, x::number m from pt) select * from s",
            "s_sha": "with s as (select distinct sha2(trim(lower(tx || 'salt'))) n, nullif(u, '') m from pt) select * from s",
            "s_vc": "with s as (select distinct tx::varchar n, x::number m from pt) select * from s",
            "s_vc_id": "with s as (select distinct id::varchar n, x::number m from pt) select * from s",
            "s_gkey": "with a as (select tx::varchar n, sum(x::number) s from pt group by 1) select * from a",
            "s_ugj": "with a as (select t.id n, t.x::number m from pt t join pt u on t.x::number = u.id union select id, u from pt), "
                     "b as (select n, sum(m) s from a group by n) select * from b",
            "s_un_star": "with a as (select id n, x::number m from pt), b as (select u n, x::number m from pt), "
                         "c as (select * from a union select * from b) select * from c",
        }
        cur.execute("create view p_g as select id n, sum(x::number) s from pt group by 1")
        for name, body in views.items():
            cur.execute(f"create secure view {name} as {body}")
        cur.execute("create view p_d_txt as with s as (select distinct tx::number n, x::number m from pt) select * from s")
        for name in ("s_d_id", "s_d_plain", "s_d_expr", "s_d_nullif", "s_qual", "s_j", "s_md_x", "p_d_txt", "s_vc", "s_gkey",
                     "s_ugj", "s_un_star", "p_g"):
            assert count(f"select count(*) from {name} where n is null" if name != "s_j" else
                         "select count(*) from s_j where k is null") == 0, name
        for failing in ("select count(*) from s_d_txt where n is null", "select count(*) from s_d_id where m is null",
                        "select count(*) from s_low_x where n is null", "select count(*) from s_sha where n is null",
                        "select count(*) from s_vc_id where n is null", "select count(*) from p_g where s is null",
                        "select count(*) from s_d_id", "select count(*) from s_d_nullif"):
            with pytest.raises(ProgrammingError):
                cur.execute(failing).fetchall()


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


def test_the_side_of_a_join_with_no_rows_leaves_the_other_side_unread_as_snowflake_does():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select seq4() as i, 'x' || seq4() as s, parse_json('\"g\"') as g "
                    "from table(generator(rowcount => 50))")
        cur.execute("create table nothing as select * from t where false")
        # (measured on Snowflake: each answers, though `bad` fails on every row it reads)
        bad = "bad as (select i, s::number as n from t)"
        keys = "bad as (select i, k.value::string as n from t, lateral flatten(input => object_keys(g)) k)"
        none = "e as (select a.* from t a join t b on a.i = b.i + 1000)"  # (no rows, known only as it runs)
        for q in [f"with {none}, {bad} select max(bad.n) from e left join bad on e.i = bad.i",
                  f"with {none}, {bad} select max(bad.n) from bad join e on e.i = bad.i",
                  f"with {none}, {bad} select max(bad.n) from bad right join e on e.i = bad.i",
                  f"with {none}, {bad} select count(*) from e left join bad on e.i = bad.n",
                  f"with {none}, {keys} select max(bad.n) from e left join bad on e.i = bad.i",
                  f"with {bad} select max(bad.n) from nothing e left join bad on e.i = bad.i",
                  f"with {none}, {bad} select max(bad.n) from e left join bad on e.i = bad.i left join t z on z.i = e.i",
                  f"create table built as with {none}, {bad} select e.i, bad.n from e left join bad on e.i = bad.i"]:
            cur.execute(q)
        assert cur.execute("select count(*) from built").fetchone() == (0,)
        # ...and these fail, as there: the side read first has rows, or a FULL JOIN reads both
        for q in [f"with {none}, {bad} select max(bad.n) from bad left join e on e.i = bad.i",
                  f"with {none}, {bad} select max(bad.n) from e full join bad on e.i = bad.i",
                  f"with {none}, {bad} select max(bad.n) from t z left join e on z.i = e.i left join bad on e.i = bad.i"]:
            with pytest.raises(ProgrammingError, match="Numeric value 'x0' is not recognized") as e:
                cur.execute(q)
            assert e.value.errno == 100038


def test_random_is_a_64_bit_integer_also_in_a_cte():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        # (fakesnow's rewrite of RANDOM() was made again by each SELECT the CTE is part of, and overflowed)
        low, high, n = cur.execute("with r as (select random() as x from table(generator(rowcount => 2000)) where random() <> 0) "
                                   "select min(x), max(x), count(*) from r").fetchone()
        assert n == 2000 and low < -8e18 and high > 8e18
        # a macro that names its argument twice reads one value of it (measured: TYPEOF is INTEGER on all 2,000 rows)
        assert cur.execute("select typeof(random()::variant) t, count(*) from table(generator(rowcount => 2000)) "
                           "group by 1").fetchall() == [("INTEGER", 2000)]


def test_seeded_random_starts_its_sequence_again_in_each_statement_and_each_row_takes_the_next():
    # measured on Snowflake: random(5) = random(5) in a row, random(5) <> random(6), 100 rows give 100 values, a
    # statement run again gives the same ones (the values themselves are not Snowflake's: its output function is unknown)
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        assert cur.execute("select random(5) = random(5), random(5) = random(6)").fetchone() == (True, False)
        rows = "with c as (select random(5) r from table(generator(rowcount => 100))) select r from c"
        first = [r for (r,) in cur.execute(rows).fetchall()]
        assert len(set(first)) == 100 and all(isinstance(r, int) and -2**63 <= r < 2**63 for r in first)
        assert [r for (r,) in cur.execute(rows).fetchall()] == first
        assert cur.execute("select typeof(random(5)::variant)").fetchone() == ("INTEGER",)


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


def test_a_hash_key_of_text_functions_wraps_each_column_once_and_binds_at_once():
    # datavault4dbt's hash keys: MD5 of NULLIF(CAST(REGEXP_REPLACE(CONCAT(IFNULL(CONCAT('"', REPLACE(TRIM(CAST(col AS
    # STRING))...))))))). Each text function was wrapped again in _sf_text, which names its argument a dozen times:
    # a stage of 19 columns took DuckDB five minutes to bind over 6,000 rows
    import time

    import snowflake.connector
    import sqlglot

    from fauxflake import semantics
    from fauxflake.emulator import serve

    column = ("IFNULL((CONCAT('\"', REPLACE(REPLACE(REPLACE(TRIM(CAST({c} AS STRING)), '\\\\', '\\\\\\\\'), '\"', '\\\\\"'), "
              "'^^', '--'), '\"')), '^^')")
    key = ("IFNULL(LOWER(MD5(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(" + ", '||', ".join(column.format(c=c) for c in "abcdef")
           + "), char(10), ''), char(13), '') AS STRING), '^^||^^||^^||^^||^^||^^'))), '00000000000000000000000000000000')")
    sql = f"select {key} as hk from t"
    written = semantics.rewrite(sqlglot.parse_one(sql, read="snowflake")).sql(dialect="duckdb")
    assert written.upper().count("_SF_TEXT(") == 6
    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select seq4() a, 'x' || seq4() b, 1.5 c, current_date() d, true e, null f "
                    "from table(generator(rowcount => 3000))")
        started = time.monotonic()
        hashes = cur.execute(f"select hk from ({sql}) order by 1 limit 1").fetchall()
        assert time.monotonic() - started < 10 and len(hashes[0][0]) == 32
        # the same key as Snowflake makes it: MD5 of the text of each value
        assert cur.execute("select md5('\"0\"||\"x0\"||\"1.5\"||\"' || current_date()::varchar || '\"||\"true\"||^^')").fetchone()[0] \
            == cur.execute(f"select lower(hk) from ({sql.replace('from t', 'from t where a = 0')})").fetchone()[0]
