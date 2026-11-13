from datetime import datetime, timedelta

import snowflake.connector

from fauxflake.emulator import rewrite, serve
from fauxflake.generate import Warehouse

TABLES = ["RAW.SHOP.CUSTOMERS", "RAW.SHOP.ORDERS", "RAW.SHOP.EVENTS"]


def checksum(emu, table):
    return emu.cursor().execute(f"select count(*), bit_xor(hash(t)) from {table} t").fetchone()


def test_stepping_the_clock_equals_generating_at_once(spec):
    """The warehouse at time T is a pure function of T, however we got there."""
    end = datetime(2025, 2, 20)
    with serve() as a, serve() as b:
        Warehouse(a, spec, 0.0005).advance_to(end)
        stepped = Warehouse(b, spec, 0.0005)
        stepped.advance_to(end - timedelta(days=9))
        for hours in (24, 24, 6, 90, 72):
            stepped.advance_to(stepped.now + timedelta(hours=hours))
        assert stepped.now == end
        for t in TABLES:
            assert checksum(a, t) == checksum(b, t), t


def test_shape(warehouse, emu):
    cur = emu.cursor()
    # every foreign key resolves, even though parents and children are generated independently
    assert cur.execute("select count(*) from RAW.SHOP.ORDERS o anti join RAW.SHOP.CUSTOMERS c using (CUSTOMER_ID)").fetchone()[0] == 0
    # nothing is visible before it has been loaded
    assert cur.execute("select max(_LOADED_AT) from RAW.SHOP.EVENTS").fetchone()[0] <= warehouse.now
    # late arrivals: yesterday's load contains events from earlier days
    late = cur.execute("""select count(distinct OCCURRED_AT::date) from RAW.SHOP.EVENTS
                          where _LOADED_AT > '2025-04-30'""").fetchone()[0]
    assert late >= 3
    # at-least-once delivery produces duplicate keys in raw, at roughly the declared rate
    n, distinct = cur.execute("select count(*), count(distinct EVENT_ID) from RAW.SHOP.EVENTS").fetchone()
    assert 0.0003 < (n - distinct) / n < 0.003
    # skewed foreign keys: the busiest 1% of customers place far more than 1% of orders
    share = cur.execute("""
        with c as (select CUSTOMER_ID, count(distinct ORDER_ID) n from RAW.SHOP.ORDERS group by 1),
             r as (select n, percent_rank() over (order by n desc) pr from c)
        select sum(n) filter (where pr <= 0.01) / sum(n) from r""").fetchone()[0]
    assert share > 0.03
    # lifecycles only move forward
    assert cur.execute("""
        select count(*) from RAW.SHOP.ORDERS a join RAW.SHOP.ORDERS b using (ORDER_ID)
        where a.STATUS = 'placed' and b.STATUS = 'shipped' and b.UPDATED_AT < a.UPDATED_AT""").fetchone()[0] == 0


def test_snowflake_clock_follows_the_simulation(warehouse, emu):
    conn = snowflake.connector.connect(**emu.connect_kwargs(database="RAW", schema="SHOP"))
    today, now = conn.cursor().execute("select current_date(), current_timestamp").fetchone()
    assert str(today) == "2025-05-01" and now.replace(tzinfo=None) == warehouse.now


def test_rewrites():
    assert rewrite('create or replace temporary table A.B.C__dbt_tmp as (select 1)').startswith("create or replace table A.B.C")
    assert rewrite("create temporary table T as select 1") == "create temporary table T as select 1"
    merged = rewrite('merge into A.B.T as d using A.B.S as s on s.id = d.id '
                     'when not matched then insert ("ID", "V") values ("ID", "V")')
    assert 's."ID"' in merged and 's."V"' in merged


def test_two_columns_of_a_row_are_drawn_independently():
    """`where plan = 'pro' and region = 'eu'` keeps rows only if every pair of values occurs."""
    from fauxflake.spec import Spec

    values = {"gen": "categorical", "values": {"a": 0.25, "b": 0.25, "c": 0.25, "d": 0.25}}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.PAIRS": {
        "volume": {"rows_per_day": 2000, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"}, "PLAN": dict(values), "REGION": dict(values)}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        pairs = emu.cursor().execute("select count(distinct (PLAN, REGION)), min(n), max(n) from "
                                     "(select PLAN, REGION, count(*) over (partition by PLAN, REGION) n from RAW.T.PAIRS)").fetchone()
    assert pairs[0] == 16 and pairs[1] > pairs[2] / 2  # all sixteen pairs, in similar numbers


def test_ids_asked_for_by_name_are_held_by_the_first_rows_and_referred_to():
    from fauxflake.spec import Spec

    table = {"volume": {"rows_per_day": 300, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {
        "RAW.T.SERVERS": {**table, "columns": {"ID": {"gen": "hash_id", "named": ["93mykbogbjfrbbdqphx3zhze5c", "abc"]}}},
        "RAW.T.EVENTS": {**table, "columns": {"EVENT_ID": {"gen": "sequence"}, "SERVER_ID": {"gen": "fk", "ref": "RAW.T.SERVERS", "skew": 1.5}}},
    }}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        servers = cur.execute("select count(*), count(distinct ID), count_if(ID in ('93mykbogbjfrbbdqphx3zhze5c', 'abc')) from RAW.T.SERVERS").fetchone()
        events = cur.execute("select count(*) from RAW.T.EVENTS where SERVER_ID = '93mykbogbjfrbbdqphx3zhze5c'").fetchone()[0]
        orphans = cur.execute("select count(*) from RAW.T.EVENTS e left join RAW.T.SERVERS s on s.ID = e.SERVER_ID where s.ID is null").fetchone()[0]
    assert servers[0] == servers[1] and servers[2] == 2
    assert events > 0 and orphans == 0
