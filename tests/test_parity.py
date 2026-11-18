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
