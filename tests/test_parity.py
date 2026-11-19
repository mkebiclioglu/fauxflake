from datetime import datetime

import duckdb

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


def test_arrays_that_differ_only_in_order_and_where_such_order_comes_from():
    from fauxflake.parity import _canonical, _unordered_aggregate

    assert _canonical('["b","a"]') != _canonical('["a","b"]')
    assert _canonical('["b","a"]', unordered=True) == _canonical('["a", "b"]', unordered=True)
    assert _unordered_aggregate("select array_unique_agg(v) over (partition by k) from t") == "ARRAY_UNIQUE_AGG without an order"
    assert _unordered_aggregate("select array_agg(v) within group (order by v) from t") is None


def test_models_are_compared_row_for_row_with_documents_by_content():
    import snowflake.connector

    from fauxflake.emulator import serve
    from fauxflake.parity import compare

    class Project:  # the two models, as dbt lists them
        def models(self):
            return [{"unique_id": f"model.p.{n}", "name": n, "database": "D", "schema": s}
                    for n, s in (("same", "MINE"), ("other", "MINE"), ("empty", "MINE"), ("broken", "MINE"))]

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
        got = {r.model: r for r in compare(conn, emu, Project(), schema="MINE", remote_schema="THEIRS")}
        assert got["broken"].ok and got["broken"].note.startswith("both engines fail"), got["broken"]
        assert got["same"].ok and got["same"].snowflake_rows == 1, got["same"]
        assert (got["other"].only_emulator, got["other"].only_snowflake) == (2, 2) and "N:" in got["other"].note, got["other"]
        assert got["empty"].ok and got["empty"].snowflake_rows == 0
