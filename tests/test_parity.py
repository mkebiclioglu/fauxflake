from datetime import datetime

import duckdb
import pytest

from fauxflake.parity import _is_clock


def test_a_column_holding_when_each_build_ran_is_the_clock_not_a_difference():
    duck = duckdb.connect()
    frozen, began, ended = datetime(2026, 11, 18, 16, 0, 0), datetime(2026, 11, 18, 16, 5, 0), datetime(2026, 11, 18, 16, 9, 0)
    duck.execute("create table mine as select timestamptz '2026-11-18 16:00:00+00' as at, date '2026-11-18' as d, "
                 "timestamp '2026-11-18 16:00:00' as other")
    duck.execute("create table theirs as select timestamptz '2026-11-18 16:07:31+00' as at, date '2026-11-18' as d, "
                 "timestamp '2026-11-18 16:00:00' as other union all select timestamptz '2026-11-18 16:08:00+00', "
                 "date '2026-11-18', timestamp '2026-01-01'")
    both = "with m as (select * from mine), t as (select * from theirs) "
    clock = (frozen, began, ended)
    assert _is_clock(duck, both, "at", "TIMESTAMP WITH TIME ZONE", clock)
    assert _is_clock(duck, both, "d", "DATE", clock)
    assert not _is_clock(duck, both, "other", "TIMESTAMP", clock)  # a value from the data, far from either build
    assert not _is_clock(duck, both, "at", "TIMESTAMP WITH TIME ZONE", None)


def test_what_dbt_writes_of_its_own_run_is_the_clock_too():
    from datetime import timedelta

    duck = duckdb.connect()
    frozen = datetime(2026, 11, 18, 23, 0, 0)
    here, there = (datetime(2026, 11, 19, 1, 3), datetime(2026, 11, 19, 1, 8)), (datetime(2026, 11, 19, 1, 8), datetime(2026, 11, 19, 1, 20))
    shift = datetime.now().astimezone().utcoffset() or timedelta(0)
    local = lambda t: (t + shift).strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731 - this machine's time of day, as `now()` in Jinja
    duck.execute(f"""create table mine as select '2026-11-19 01:03:16.512559' as last_run, 'aaa-1' as invocation,
                     timestamp '{local(datetime(2026, 11, 19, 1, 5, 9))}' as model_time, 'x' as other""")
    duck.execute(f"""create table theirs as select '2026-11-19 01:08:57.286279' as last_run, 'bbb-2' as invocation,
                     timestamp '{local(datetime(2026, 11, 19, 1, 10, 33))}' as model_time, 'y' as other""")
    both = "with m as (select * from mine), t as (select * from theirs) "
    clock = (frozen, *there, {"local": here, "ids": ("aaa-1", "bbb-2")})
    assert _is_clock(duck, both, "last_run", "VARCHAR", clock)  # run_started_at, as text
    assert _is_clock(duck, both, "invocation", "VARCHAR", clock)
    assert _is_clock(duck, both, "model_time", "TIMESTAMP", clock)
    assert not _is_clock(duck, both, "other", "VARCHAR", clock)
    assert not _is_clock(duck, both, "invocation", "VARCHAR", (frozen, *there, {"local": here, "ids": ("zzz", "bbb-2")}))


def test_a_window_whose_order_has_ties_is_found():
    import snowflake.connector

    from fauxflake.emulator import serve
    from fauxflake.parity import _ties

    with serve() as emu:
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S"))
        conn.cursor().execute("create database if not exists D")
        conn.cursor().execute("create schema if not exists D.S")
        conn.cursor().execute("create or replace table D.S.T as select * from values (1, 'a', 1), (1, 'b', 1), (2, 'a', 2) as v(u, s, d)")
        tied = "with x as (select * from D.S.T) select u, max(s) over (partition by u order by d rows between 1 preceding and current row) from x"
        assert _ties(conn, tied).startswith("1 tied group(s)")
        assert _ties(conn, "select row_number() over (partition by u order by d, s) from D.S.T") is None  # distinct keys
        assert _ties(conn, "select rank() over (partition by u order by d) from D.S.T") is None  # ties do not matter to RANK
        # a key that is an alias of the same select
        aliased = "select u * 10 as k, s from D.S.T qualify row_number() over (partition by k order by d desc) = 1"
        assert _ties(conn, aliased).startswith("1 tied group(s)")
        # an alias in WHERE too; a name that is a column of the FROM as well is the column (u, not u * 0)
        filtered = "select u * 0 as u, d * 1 as dd, s from D.S.T where dd > 0 qualify row_number() over (partition by u order by dd) = 1"
        assert _ties(conn, filtered).startswith("1 tied group(s)")  # (u = 1 twice; partitioned by u * 0 it would be 2 rows tied on d)
        distinct = "select u * 0 as u, s as ss from D.S.T where ss is not null qualify row_number() over (partition by u order by ss) = 1"
        assert _ties(conn, distinct) is None  # (by the column u, ss differs; by u * 0, 'a' would tie)


def test_arrays_that_differ_only_in_order_and_where_such_order_comes_from():
    from fauxflake.parity import _canonical, _unordered_aggregate

    assert _canonical('["b","a"]') != _canonical('["a","b"]')
    assert _canonical('["b","a"]', unordered=True) == _canonical('["a", "b"]', unordered=True)
    assert _unordered_aggregate("select array_unique_agg(v) over (partition by k) from t") == "ARRAY_UNIQUE_AGG without an order"
    assert _unordered_aggregate("select array_agg(v) within group (order by v) from t") is None


@pytest.mark.parametrize("wide", [200, 0])
def test_models_are_compared_row_for_row_with_documents_by_content(wide, monkeypatch):
    """(wide 0: every table compared by a hash of its rows, as one of a thousand columns is)"""
    import snowflake.connector

    monkeypatch.setattr("fauxflake.parity.WIDE", wide)

    from fauxflake.emulator import serve
    from fauxflake.parity import compare

    class Project:  # the two models, as dbt lists them
        def models(self):
            return [{"unique_id": f"model.p.{n}", "name": n, "database": "D", "schema": s}
                    for n, s in (("same", "MINE"), ("other", "MINE"), ("empty", "MINE"), ("broken", "MINE"), ("listed", "MINE"))]

    with serve() as emu:
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S"))
        cur = conn.cursor()
        cur.execute("create database if not exists D")
        for schema in ("MINE", "THEIRS"):
            cur.execute(f"create schema if not exists D.{schema}")
        # the same rows, the document written with its keys in another order and 1.0 for 1; one row differs in `other`
        cur.execute("""create or replace table D.MINE.SAME as select 1 as n, 'a' as s, parse_json('{"b": 1, "a": [2, 1]}') as v""")
        cur.execute("""create or replace table D.THEIRS.SAME as select 1 as n, 'a' as s, parse_json('{"a": [2, 1], "b": 1.0}') as v""")
        cur.execute("""create or replace table D.MINE.OTHER as select * from values (1, parse_json('[1, 2]')), (2, parse_json('[3]')) as t(n, v)""")
        cur.execute("""create or replace table D.THEIRS.OTHER as select * from values (1, parse_json('[2, 1]')), (3, parse_json('[3]')) as t(n, v)""")
        cur.execute("create or replace table D.MINE.EMPTY as select 1 as n where false")
        cur.execute("create or replace table D.THEIRS.EMPTY as select 1 as n where false")
        # a view whose values fail when read, on both sides (a count alone would not read them)
        for schema in ("MINE", "THEIRS"):
            cur.execute(f"create or replace view D.{schema}.BROKEN as select to_timestamp_tz(s, 'YYYY-MM-DD\"T\"HH24:MI:SS') as t "
                        "from (select '2023-02-20' as s)")
        # text holding a list gathered without an order: the same elements, another order
        cur.execute("""create or replace table D.MINE.LISTED as select 1 as n, '[{"a":1},{"a":2}]' as items""")
        cur.execute("""create or replace table D.THEIRS.LISTED as select 1 as n, '[{"a":2},{"a":1}]' as items""")
        got = {r.model: r for r in compare(conn, emu, Project(), schema="MINE", remote_schema="THEIRS")}
        assert not got["listed"].ok and got["listed"].order_only, got["listed"]
        assert got["broken"].ok and got["broken"].note.startswith("both engines fail"), got["broken"]
        assert got["same"].ok and got["same"].snowflake_rows == 1, got["same"]
        assert (got["other"].only_emulator, got["other"].only_snowflake) == (2, 2) and "N:" in got["other"].note, got["other"]
        assert got["empty"].ok and got["empty"].snowflake_rows == 0


def test_columns_made_by_any_value_are_found():
    from fauxflake.parity import _any_values

    sql = "select date, any_value(currency) as currency, sum(usage) as usage, max(x) as top from t group by 1"
    assert _any_values(sql) == {"CURRENCY"}


def test_columns_made_by_listagg_without_an_order_are_found():
    from fauxflake.parity import _unordered_lists

    sql = ("select a, listagg(n, ', ') over (partition by a) as open_invoices, listagg(n) within group (order by n) as kept, "
           "listagg(distinct n) as names from t group by a")
    assert _unordered_lists(sql) == {"OPEN_INVOICES", "NAMES"}
    later = f"with o as ({sql}) select a, max(open_invoices) as list_of_open_invoices, max(kept) as k from o group by a"
    assert _unordered_lists(later) == {"OPEN_INVOICES", "NAMES", "LIST_OF_OPEN_INVOICES"}


def test_a_node_failing_on_one_side_only_on_a_grown_query_tag_is_not_an_engine_difference():
    from fauxflake.parity import differences

    tag = "Database Error 001008 (22023): SQL compilation error: invalid value [{...}] for parameter 'QUERY_TAG'"
    mine = {"test.p.a": ("error", tag, None), "test.p.b": ("error", "Division by zero", None)}
    theirs = {"test.p.a": ("pass", "", 0), "test.p.b": ("pass", "", 0)}
    assert {d.node: d.kind for d in differences(mine, theirs, [])} == {"test.p.a": "undetermined", "test.p.b": "emulator_only_error"}


def test_a_model_is_compared_as_the_role_reads_it_through_its_masking_policy():
    import snowflake.connector

    from fauxflake.emulator import serve
    from fauxflake.parity import _through_policies

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create table t as select 'secret' as s, 1 as n")
        cur.execute("create masking policy p as (val varchar) returns varchar -> case when current_role() = 'NOBODY' then val else '***MASKED***' end")
        cur.execute("alter table t modify column s set masking policy p")
        duck = emu.cursor()
        mine = _through_policies(emu, duck, "D", "S", "T", "D.S.T")
        assert duck.execute(f"select * from {mine}").fetchall() == [("***MASKED***", 1)]
        assert _through_policies(emu, duck, "D", "S", "OTHER", "D.S.OTHER") == "D.S.OTHER"


def test_a_snowflake_that_stops_answering_stops_the_comparison_instead_of_making_each_model_a_difference():
    import pytest
    import snowflake.connector
    from snowflake.connector.errors import ProgrammingError

    from fauxflake.emulator import serve
    from fauxflake.parity import SnowflakeUnavailable, _ties, compare

    class Spent:  # a warehouse whose resource monitor's quota is spent
        def cursor(self):
            return self

        def execute(self, *_args, **_kwargs):
            raise ProgrammingError(msg="Warehouse 'WH' cannot be resumed because resource monitor 'CAP' has exceeded its quota.",
                                   errno=90073, sqlstate="22000")

    class Project:
        def models(self):
            return [{"unique_id": "model.p.a", "name": "a", "database": "D", "schema": "S"}]

    with serve() as emu:
        cur = snowflake.connector.connect(**emu.connect_kwargs(database="D", schema="S")).cursor()
        cur.execute("create database if not exists D")
        cur.execute("create schema if not exists D.S")
        cur.execute("create or replace table D.S.A as select 1 as n")
        with pytest.raises(SnowflakeUnavailable):
            compare(Spent(), emu, Project())
    with pytest.raises(SnowflakeUnavailable):  # (not "no ties": that would call a difference an engine's)
        _ties(Spent(), "select row_number() over (partition by u order by d) from t")


def test_a_database_named_in_sql_is_read_from_its_copy_on_snowflake():
    from fauxflake.parity import renamed_sql

    moved = {"SNOWFLAKE": "FAUXFLAKE_SNOWFLAKE"}
    assert renamed_sql("select * from snowflake.account_usage.roles", moved) == "select * from FAUXFLAKE_SNOWFLAKE.account_usage.roles"
    assert renamed_sql('select * from "SNOWFLAKE"."ACCOUNT_USAGE"."X"', moved) == 'select * from FAUXFLAKE_SNOWFLAKE."ACCOUNT_USAGE"."X"'
    # strings, comments, two-part names, other databases' schemas and a quoted lower-case name stay as written
    for kept in ["select 'snowflake.account_usage.roles'", "select 1 /* snowflake.a.b */", "select snowflake.x from t",
                 "select * from my.snowflake.a", 'select * from "snowflake".a.b']:
        assert renamed_sql(kept, moved) == kept


def test_the_comparison_functions_outlive_the_cursor_that_first_read(emu):
    import gc

    from fauxflake.parity import _canonical_functions

    for _ in range(3):  # (each day's comparison reads through a cursor of its own)
        cur = emu.cursor()
        _canonical_functions(emu.duck)
        assert cur.execute("""select _fx_canon_u('[2, 1.0, {"b": 1, "a": 2}]')""").fetchone() == ('[1,2,{"a":2,"b":1}]',)
        cur.close()
        del cur
        gc.collect()


def test_times_written_from_the_clock_are_the_clock_on_both_engines():
    from datetime import datetime

    import duckdb

    from fauxflake.parity import _clock_relative

    duck = duckdb.connect()
    # dateadd('day', -30, current_timestamp) and the like: each engine's clock, less the same amounts; an hour truncated
    # to the same value on both sides is not evidence either way
    duck.execute("create table m as select * from (values ('2026-10-23 01:58:27'::timestamp), ('2026-11-21 01:58:27'::timestamp), "
                 "('2026-11-22 01:00:00'::timestamp)) t(c)")
    duck.execute("create table t as select * from (values ('2026-10-23 01:58:39.367'::timestamp), ('2026-11-21 01:58:39.367'::timestamp), "
                 "('2026-11-22 01:00:00'::timestamp)) t(c)")
    frozen, built = datetime(2026, 11, 22, 1, 58, 27), (datetime(2026, 11, 22, 1, 58, 30), datetime(2026, 11, 22, 1, 58, 50))
    assert _clock_relative(duck, "", '"c"', [(frozen, *built)])
    # the same values outside the Snowflake build: an engine difference
    assert not _clock_relative(duck, "", '"c"', [(frozen, datetime(2026, 11, 23), datetime(2026, 11, 24))])
    duck.execute("update t set c = c + interval 1 day where c < '2026-11-01'")
    assert not _clock_relative(duck, "", '"c"', [(frozen, *built)])
    # an incremental model's rows of two days (a data vault's LDTS): each the clock of its own day's build
    duck.execute("create or replace table m as select * from (values ('2026-11-22 04:02:03'::timestamp), ('2026-11-22 04:09:41'::timestamp)) t(c)")
    duck.execute("create or replace table t as select * from (values ('2026-11-22 04:02:30.366'::timestamp), ('2026-11-22 04:10:02.5'::timestamp)) t(c)")
    two_days = [(datetime(2026, 11, 22, 4, 2, 3), datetime(2026, 11, 22, 4, 2, 20), datetime(2026, 11, 22, 4, 2, 50)),
                (datetime(2026, 11, 22, 4, 9, 41), datetime(2026, 11, 22, 4, 9, 55), datetime(2026, 11, 22, 4, 10, 20))]
    assert _clock_relative(duck, "", '"c"', two_days)
    duck.execute("update t set c = c + interval 5 minute where c > '2026-11-22 04:05'")  # (fits no day's build)
    assert not _clock_relative(duck, "", '"c"', two_days)


def test_a_node_skipped_after_a_query_tag_failure_on_one_side_is_not_a_difference():
    from fauxflake.parity import differences

    tag = "000904: invalid value [x] for parameter 'QUERY_TAG'"
    local = {"model.p.a": ("error", tag, None), "test.p.not_null_a": ("skipped", "", None), "test.p.other": ("pass", "", 0)}
    remote = {"model.p.a": ("success", "", None), "test.p.not_null_a": ("pass", "", 0), "test.p.other": ("fail", "", 3)}
    found = {d.node: d.kind for d in differences(local, remote, [], {"test.p.not_null_a": ["model.p.a"], "test.p.other": ["model.p.b"]})}
    assert found == {"model.p.a": "undetermined", "test.p.not_null_a": "undetermined", "test.p.other": "test_result"}


def test_a_test_of_rows_the_data_does_not_determine_is_not_a_difference():
    from fauxflake.parity import Parity, differences

    local = {"test.p.is_empty_fct": ("warn", "", 9), "test.p.plain": ("pass", "", 0)}
    remote = {"test.p.is_empty_fct": ("warn", "", 11), "test.p.plain": ("fail", "", 2)}
    rows = [Parity("fct", 9, 11, 0, 2, "", undetermined="ties in a window")]
    found = {d.node: d.kind for d in differences(local, remote, rows, {"test.p.is_empty_fct": ["model.p.fct"], "test.p.plain": ["model.p.other"]})}
    assert found == {"test.p.is_empty_fct": "undetermined", "test.p.plain": "test_result", "fct": "undetermined"}
