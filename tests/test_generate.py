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


def test_a_key_with_a_shape_is_unique_and_what_refers_to_it_finds_it():
    from fauxflake.spec import Spec

    table = {"volume": {"rows_per_day": 400, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {
        "RAW.T.RELEASES": {**table, "columns": {"VERSION": {"gen": "hash_id", "template": "{n}.{n}.{n}"}}},
        "RAW.T.SERVERS": {**table, "columns": {"ID": {"gen": "sequence"}, "VERSION": {"gen": "fk", "ref": "RAW.T.RELEASES", "skew": 1.5}}},
    }}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        total, distinct, shaped = cur.execute("select count(*), count(distinct VERSION), count_if(regexp_full_match(VERSION, '[0-9]+[.][0-9][.][0-9]')) "
                                              "from RAW.T.RELEASES").fetchone()
        orphans = cur.execute("select count(*) from RAW.T.SERVERS s left join RAW.T.RELEASES r on r.VERSION = s.VERSION where r.VERSION is null").fetchone()[0]
    assert total == distinct == shaped and total > 500 and orphans == 0


def test_period_bounds_are_two_months_apart_from_the_start_of_the_history():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.RELEASES": {
        "volume": {"rows_per_day": 20, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"}, "RELEASE_DATE": {"gen": "time_offset", "type": "DATE", "p": [0, 1], "seconds": [0, 9],
                                                               "unique": True, "every_days": 60}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        rows = emu.cursor().execute("select RELEASE_DATE from RAW.T.RELEASES order by 1").fetchall()
    days = [r[0] for r in rows]
    assert str(days[0])[:10] == "2025-01-01" and len(set(days)) == len(days) > 20
    assert all((b - a).days == 60 for a, b in zip(days, days[1:]))


def test_a_parent_has_no_more_rows_than_a_test_allows():
    from fauxflake.spec import Spec

    table = {"volume": {"rows_per_day": 300, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {
        "RAW.T.SUBS": {**table, "columns": {"ID": {"gen": "sequence"}}},
        "RAW.T.ITEMS": {**table, "columns": {"ITEM_ID": {"gen": "sequence"}, "SUB_ID": {"gen": "fk", "ref": "RAW.T.SUBS", "skew": 1.5, "per_parent": 2}}},
    }}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4))
        cur = emu.cursor()
        most = cur.execute("select max(n) from (select count(*) n from RAW.T.ITEMS group by SUB_ID)").fetchone()[0]
        orphans = cur.execute("select count(*) from RAW.T.ITEMS i left join RAW.T.SUBS s on s.ID = i.SUB_ID where s.ID is null").fetchone()[0]
    assert most == 2 and orphans == 0


def test_a_grain_gives_one_row_a_day_for_each_listed_value():
    from fauxflake.spec import Spec

    kinds = {"gen": "categorical", "values": {"compute": 0.5, "storage": 0.3, "other": 0.2}}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.RATES": {
        "volume": {"rows_per_day": 50, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}, "grain": ["DAY", "KIND"],
        "columns": {"DAY": {"gen": "event_time", "type": "DATE"}, "KIND": kinds, "RATE": {"gen": "dist", "type": "DOUBLE", "p": [0, 1], "v": [1, 3]}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 6))
        rows, pairs, days, kinds_seen = emu.cursor().execute(
            "select count(*), count(distinct (DAY, KIND)), count(distinct DAY), count(distinct KIND) from RAW.T.RATES").fetchone()
    assert rows == pairs == days * 3 and kinds_seen == 3 and days >= 4


def test_an_hourly_series_has_every_entity_every_hour_once():
    from fauxflake.spec import Spec

    table = {"volume": {"rows_per_day": 300, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}}
    entity = {"gen": "pool", "pool": "entity_id", "size": 4, "numeric": True}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {
        "RAW.T.METERING": {**table, "grain": ["START_TIME", "ENTITY_ID"], "columns": {
            "START_TIME": {"gen": "event_time", "trunc": "hour"}, "ENTITY_ID": dict(entity),
            "CREDITS": {"gen": "dist", "type": "DOUBLE", "p": [0, 1], "v": [0, 3]}}},
        "RAW.T.QUERIES": {**table, "columns": {"ID": {"gen": "sequence"}, "WAREHOUSE_ID": dict(entity), "AT": {"gen": "event_time"}}},
    }}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4))
        cur = emu.cursor()
        rows, pairs, hours, entities = cur.execute(
            "select count(*), count(distinct (START_TIME, ENTITY_ID)), count(distinct START_TIME), count(distinct ENTITY_ID) from RAW.T.METERING").fetchone()
        matched, queries = cur.execute(
            "select count(m.ENTITY_ID), count(*) from RAW.T.QUERIES q left join RAW.T.METERING m "
            "on m.ENTITY_ID = q.WAREHOUSE_ID and m.START_TIME = date_trunc('hour', q.AT) where q.AT < timestamp '2025-01-03'").fetchone()
    assert rows == pairs == hours * 4 and entities == 4 and hours >= 48
    assert matched == queries > 100  # every query finds its warehouse's hour


def test_links_between_documents_reach_every_row_of_the_other_table():
    from fauxflake.spec import Spec

    table = {"volume": {"rows_per_day": 300, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}}
    domain = "RAW.T.TXS.V.hash"
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {
        "RAW.T.TXS": {**table, "columns": {"V": {"gen": "object", "fields": {"hash": {"gen": "hash_id", "domain": domain}}}}},
        "RAW.T.RECEIPTS": {**table, "columns": {"V": {"gen": "object", "fields": {
            "id": {"gen": "hash_id", "domain": "RAW.T.RECEIPTS.V.id"}, "tx": {"gen": "link", "domain": domain, "depth": 0}}}}},
    }}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 5))
        cur = emu.cursor()
        txs = cur.execute("select count(*) from RAW.T.TXS").fetchone()[0]
        with_receipts, orphans = cur.execute(
            "select count(distinct t.V->>'hash'), count_if(t.V is null) from RAW.T.RECEIPTS r "
            "left join RAW.T.TXS t on t.V->>'hash' = r.V->>'tx'").fetchone()
    assert orphans == 0 and with_receipts > txs * 0.4 > 100  # not only the first fifty


def test_a_key_left_out_of_most_documents_is_left_out_however_it_is_read():
    from fauxflake.spec import Spec

    text = {"gen": "template", "template": "failure_{hex}", "optional": 0.95}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.RECEIPTS": {
        "volume": {"rows_per_day": 400, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"V": {"gen": "object", "fields": {"id": {"gen": "template", "template": "id_{hex}"},
                                                      "status.Failure": dict(text), "status.Failure~json": dict(text)}}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4))
        rows, failed = emu.cursor().execute(
            "select count(*), count_if(json_extract(V, '$.status.Failure') is not null) from RAW.T.RECEIPTS").fetchone()
    assert rows > 500 and 0 < failed < rows * 0.15


def test_less_than_a_row_a_day_is_a_row_every_few_days():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.SMALL": {
        "volume": {"rows_per_day": 100, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"}, "AT": {"gen": "event_time"}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 0.001).advance_to(datetime(2025, 4, 11))  # a hundred days at a tenth of a row a day
        rows, ids, first = emu.cursor().execute('select count(*), count(distinct ID), min("AT")::date from RAW.T.SMALL').fetchone()
    assert 9 <= rows <= 12 and ids == rows and str(first) == "2025-01-01"


def test_ids_in_a_document_and_the_lists_that_name_them_form_trees():
    from fauxflake.spec import Spec

    domain = "RAW.T.SHARDS.V.receipts[].receipt_id"
    link = {"gen": "link", "domain": domain, "depth": 1, "layered": True, "tree": True}
    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.SHARDS": {
        "volume": {"rows_per_day": 200, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"V": {"gen": "object", "fields": {
            "receipts[].receipt_id": {"gen": "hash_id", "domain": domain},
            "receipts[].outcome.receipt_ids[]": {**link, "slot": 0, "within": True},
            "txs[].hash": {"gen": "hash_id", "domain": "RAW.T.SHARDS.V.txs[].hash"},
            "txs[].outcome.receipt_ids[]": {**link, "slot": 1}}}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4))
        cur = emu.cursor()
        cur.execute("""create temp table ids as select r.unnest->>'receipt_id' as id from RAW.T.SHARDS, unnest(from_json(V->'receipts', '["json"]')) r""")
        cur.execute("""create temp table refs as
            select 'tx' as who, t.unnest->>'hash' as parent, c.unnest::varchar as child
            from RAW.T.SHARDS, unnest(from_json(V->'txs', '["json"]')) t, unnest(from_json(t.unnest->'outcome'->'receipt_ids', '["varchar"]')) c
            union all
            select 'receipt', r.unnest->>'receipt_id', c.unnest::varchar
            from RAW.T.SHARDS, unnest(from_json(V->'receipts', '["json"]')) r, unnest(from_json(r.unnest->'outcome'->'receipt_ids', '["varchar"]')) c""")
        total = cur.execute("select count(*), count(distinct id) from ids").fetchone()
        reached = cur.execute("select count(distinct child) from refs join ids on ids.id = refs.child").fetchone()[0]
        twice = cur.execute("select count(*) from (select child from refs join ids on ids.id = refs.child group by 1 having count(*) > 1)").fetchone()[0]
        txs, rooted = cur.execute("select count(distinct parent), count(distinct case when ids.id is not null then parent end) "
                                  "from refs left join ids on ids.id = refs.child where who = 'tx'").fetchone()
    assert total[0] == total[1] > 500 and twice == 0
    assert reached > total[0] * 0.5 and rooted > txs * 0.5


def test_a_log_keeps_its_rows_in_order_and_each_names_the_one_before():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "history": 4, "tables": {"RAW.T.BLOCKS": {
        "volume": {"rows_per_day": 100, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]}, "log": True,
        "columns": {"PART": {"gen": "dist", "type": "BIGINT", "p": [0, 0.5, 1], "v": [100, 200, 900]},
                    "V": {"gen": "object", "fields": {
                        "header.height": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000], "unique": True},
                        "header.hash": {"gen": "template", "template": "hash_{hex}"},
                        "header.prev_hash": {"gen": "previous", "of": "header.hash"},
                        "header.timestamp": {"gen": "time_offset", "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}}}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 5))
        cur = emu.cursor()
        cur.execute("""create temp table b as select (V->'header'->>'height')::bigint as height, V->'header'->>'hash' as hash,
                       V->'header'->>'prev_hash' as prev_hash, (V->'header'->>'timestamp')::timestamp as ts, PART from RAW.T.BLOCKS""")
        rows = cur.execute("select count(*), count(distinct ts) from b").fetchone()
        breaks = cur.execute("select count(*) from (select prev_hash, lag(hash) over (order by ts, height) as prior from b) where prior <> prev_hash").fetchone()[0]
        # a range of the growing number is a stretch of consecutive rows: the chain holds inside it too
        inside = cur.execute("select count(*), count_if(prior <> prev_hash) from (select prev_hash, lag(hash) over (order by ts, height) as prior "
                             "from b where PART between 100 and 200)").fetchone()
    assert rows[0] == rows[1] > 300 and breaks == 0
    assert inside[0] > 100 and inside[1] == 0


def test_a_log_loaded_part_way_through_a_day_has_no_row_missing_in_the_middle():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 7, "start": "2025-01-01", "history": 4, "tables": {"RAW.T.BLOCKS": {
        "volume": {"rows_per_day": 200, "weekly": [1] * 7}, "lateness": {"p": [0, 0.9, 1], "seconds": [30, 3600, 86400]}, "log": True,
        "columns": {"HEIGHT": {"gen": "sequence"}, "HASH": {"gen": "template", "template": "hash_{hex}"},
                    "PREV_HASH": {"gen": "previous", "of": "HASH"}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4, 15, 30))
        cur = emu.cursor()
        count, low, high = cur.execute("select count(*), min(HEIGHT), max(HEIGHT) from RAW.T.BLOCKS").fetchone()
        breaks = cur.execute("select count(*) from (select PREV_HASH, lag(HASH) over (order by HEIGHT) as prior from RAW.T.BLOCKS) "
                             "where prior <> PREV_HASH").fetchone()[0]
    assert count == high - low + 1 > 600 and breaks == 0


def test_one_row_without_a_parent():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 3, "start": "2025-01-01", "history": 3, "tables": {"RAW.N.SUBS": {
        "volume": {"rows_per_day": 50, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"}, "PARENT_ID": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000], "null_once": True}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 2))
        Warehouse(emu, spec, 1.0)
        rows = emu.cursor().execute("select count(*), count_if(PARENT_ID is null) from RAW.N.SUBS").fetchone()
    assert rows[0] > 20 and rows[1] == 1


def test_numbers_in_templates_inside_and_outside_documents():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "history": 3, "tables": {"RAW.Y.R": {
        "volume": {"rows_per_day": 5, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"RC_ID": {"gen": "template", "template": "rc_id_{hex}"},
                    "POB": {"gen": "template", "template": "rc_pob_id_{n}", "follows": "RC_ID"},
                    "DOC": {"gen": "object", "fields": {"[].version": {"gen": "template", "template": "{n}.{n}.{n}"}}}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        follows = cur.execute("select count(*), count_if(POB = 'rc_pob_id_' || RC_ID) from RAW.Y.R").fetchone()
        versions = [r[0] for r in cur.execute("select unnest(from_json(DOC, '[\"JSON\"]')) ->> 'version' from RAW.Y.R").fetchall()]
    assert follows[0] == follows[1] > 5
    assert versions and all(len(v.split(".")) == 3 and all(p.isdigit() and int(p) < 1000 for p in v.split(".")) for v in versions)


def test_named_numbers_and_scaled_epochs_are_generated(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.LINES": {
        "volume": {"rows_per_day": 200}, "columns": {
            "ID": {"gen": "sequence"},
            "FORM_ID": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000], "exactly": [334447, 426148]},
            "SENT_AT": {"gen": "time_offset", "epoch": True, "scale": 6, "p": [0, 1], "seconds": [0, 3600]}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        got = dict(cur.execute('select "FORM_ID", count(*) from "RAW"."T"."LINES" where "FORM_ID" > 1000 group by 1').fetchall())
        assert set(got) == {334447, 426148} and min(got.values()) > 30
        low, high = cur.execute('select min(to_timestamp("SENT_AT" / 1000000)), max(to_timestamp("SENT_AT" / 1000000)) '
                                'from "RAW"."T"."LINES"').fetchone()
        assert low.year == 2025 and high.year == 2025


def test_a_text_no_two_rows_share_starts_with_the_values_it_was_given():
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 42, "start": "2025-01-01", "tables": {"RAW.T.SERVERS": {
        "volume": {"rows_per_day": 500, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"},
                    "INSTALLATION": {"gen": "template", "template": "installation_{hex}", "named": ["u85ky7x", "qe3n8xc"]}}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        total, distinct, named = emu.cursor().execute(
            "select count(*), count(distinct INSTALLATION), count(*) filter (where INSTALLATION in ('u85ky7x', 'qe3n8xc')) "
            "from RAW.T.SERVERS").fetchone()
    assert total == distinct and named == 2


def test_a_column_is_empty_where_the_rule_about_its_rows_does_not_hold_and_emails_share_a_pool(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.RELEASES": {
        "volume": {"rows_per_day": 200}, "columns": {
            "VERSION": {"gen": "hash_id", "template": "{n}.{n}.{n}"},
            "CONTACT": {"gen": "pool", "pool": "email", "size": 20, "suffix": "@{org}.fauxmail.net"},
            "OWNER": {"gen": "template", "template": "user_{hex}@{org}.fauxmail.net"},
            "RC1_DATE": {"gen": "time_offset", "type": "DATE", "p": [0, 1], "seconds": [0, 86400],
                         "null_unless": "SPLIT_PART(({{VERSION}}), '.', 3) = '0'"}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        rows, cut, wrong = cur.execute(
            """select count(*), count("RC1_DATE"), count_if("RC1_DATE" is not null and split_part("VERSION", '.', 3) <> '0')
               from "RAW"."T"."RELEASES" """).fetchone()
        assert 0 < cut < rows and wrong == 0
        assert cur.execute("""select count(*), count_if("CONTACT" like 'email\\__%@org%.fauxmail.net' escape '\\')
                              from "RAW"."T"."RELEASES" """).fetchone() == (rows, rows)
        # an address is at one organisation wherever it is written, and the addresses are spread over several
        names, hosts, pairs = cur.execute("""select count(distinct split_part("CONTACT", '@', 1)), count(distinct split_part("CONTACT", '@', 2)),
                                             count(distinct "CONTACT") from "RAW"."T"."RELEASES" """).fetchone()
        assert names == pairs and hosts > 3
        assert cur.execute("""select count(distinct split_part("OWNER", '@', 2)) > 3, count_if("OWNER" not like 'user\\_%@org%.fauxmail.net' escape '\\')
                              from "RAW"."T"."RELEASES" """).fetchone() == (True, 0)


def test_a_table_of_readings_holds_a_row_a_day_for_each_of_a_few_things(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    raw = {"seed": 1, "start": "2025-01-01", "tables": {
        "RAW.T.SERVER": {"volume": {"rows_per_day": 2}, "grain": ["TIMESTAMP", "USER_ID"], "columns": {
            "USER_ID": {"gen": "hash_id", "of": 5, "named": ["93myk"]},
            "INSTALLATION": {"gen": "template", "template": "installation_{hex}", "follows": "USER_ID", "named": ["u85ky", "qe3n8"]},
            "TIMESTAMP": {"gen": "event_time"}, "RECEIVED_AT": {"gen": "loaded_at"}}},
        "RAW.T.EVENT": {"volume": {"rows_per_day": 300}, "columns": {
            "ID": {"gen": "sequence"}, "SERVER": {"gen": "fk", "ref": "RAW.T.SERVER", "skew": 1.5},
            "TIMESTAMP": {"gen": "event_time"}}}}}
    spec = Spec.from_raw(raw, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 11))
        cur = emu.cursor()
        # every server reports every day, however few rows a day the scale leaves the table
        assert cur.execute("""select count(distinct "USER_ID"), count(distinct "TIMESTAMP"::date), count(*),
                                     count(distinct concat("USER_ID", "TIMESTAMP"::date)), count_if("USER_ID" = '93myk')
                              from "RAW"."T"."SERVER" where "TIMESTAMP" < '2025-01-10'""").fetchone() == (5, 9, 45, 45, 9)
        # what is said of a server is the same in each of its readings, and the first servers have the named values
        assert cur.execute("""select count(distinct "INSTALLATION"), count(distinct concat("USER_ID", "INSTALLATION")),
                                     count(distinct case when "INSTALLATION" in ('u85ky', 'qe3n8') then "USER_ID" end),
                                     min(case when "USER_ID" = '93myk' then "INSTALLATION" end)
                              from "RAW"."T"."SERVER" where "TIMESTAMP" < '2025-01-10'""").fetchone() == (5, 5, 2, "u85ky")
        # and an event of a server on a day finds that server's reading of the day
        assert cur.execute("""select count(*), count(s."USER_ID") from "RAW"."T"."EVENT" e left join "RAW"."T"."SERVER" s
                              on s."USER_ID" = e."SERVER" and s."TIMESTAMP"::date = e."TIMESTAMP"::date
                              where e."TIMESTAMP" < '2025-01-10'""").fetchone()[0] > 1000
        assert cur.execute("""select count(*) from "RAW"."T"."EVENT" e left join "RAW"."T"."SERVER" s
                              on s."USER_ID" = e."SERVER" and s."TIMESTAMP"::date = e."TIMESTAMP"::date
                              where e."TIMESTAMP" < '2025-01-10' and s."USER_ID" is null""").fetchone()[0] == 0

    # a reading in five never arrives (`gaps`): every server is still there, with days missing
    raw["tables"]["RAW.T.SERVER"]["gaps"] = 0.2
    spec = Spec.from_raw(raw, tmp_path / "y.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 31))
        things, rows = emu.cursor().execute("""select count(distinct "USER_ID"), count(*) from "RAW"."T"."SERVER"
                                               where "TIMESTAMP" < '2025-01-30'""").fetchone()
        assert things == 5 and 5 * 29 * 0.6 < rows < 5 * 29 * 0.95


def test_a_table_another_took_over_from_has_rows_only_while_it_was_in_use():
    from datetime import date

    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "history": 60, "least_rows": 100, "tables": {
        "RAW.T.OLD": {"volume": {"rows_per_day": 3}, "until_day": 30, "columns": {"ID": {"gen": "sequence"}}},
        "RAW.T.NEW": {"volume": {"rows_per_day": 3}, "from_day": 30, "columns": {"ID": {"gen": "sequence"}}}}}, ".")
    old = spec.tables["RAW.T.OLD"].calendar(date(2025, 1, 1), date(2025, 3, 1), 1, 1.0, least=spec.least)
    new = spec.tables["RAW.T.NEW"].calendar(date(2025, 1, 1), date(2025, 3, 1), 1, 1.0, least=spec.least)
    assert all((n > 0) == (day < date(2025, 1, 31)) for day, _, n in old) and sum(n for _, _, n in old) >= 100
    assert all((n > 0) == (day >= date(2025, 1, 31)) for day, _, n in new) and sum(n for _, _, n in new) >= 90
    # the days a table has are the same days however far the clock has got
    earlier = spec.tables["RAW.T.NEW"].calendar(date(2025, 1, 1), date(2025, 2, 10), 1, 1.0, least=spec.least)
    assert earlier == new[:len(earlier)]


def test_a_list_is_empty_where_the_rule_about_its_rows_does_not_hold(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.CALLS": {
        "volume": {"rows_per_day": 300}, "columns": {
            "ID": {"gen": "sequence"},
            "NAME": {"gen": "categorical", "values": {"Calls": 0.8, "other_name": 0.2}},
            "SKUS": {"gen": "array", "values": ["professional", "enterprise"],
                     "empty_unless": "(COALESCE(({{NAME}}), 'Calls')) = 'Calls'"}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        rows, known, unknown, lost = emu.cursor().execute(
            """select count(*), count_if("NAME" = 'Calls' and json_array_length("SKUS") > 0), count_if(json_array_length("SKUS") = 0),
                      count_if("NAME" <> 'Calls' and json_array_length("SKUS") > 0) from "RAW"."T"."CALLS" """).fetchone()
        assert known > 0 and unknown > 0 and lost == 0 and known + unknown == rows


def test_a_field_that_follows_another_of_its_document_is_the_same_wherever_that_one_is(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.SUBS": {
        "volume": {"rows_per_day": 300}, "columns": {
            "ID": {"gen": "sequence"},
            "META": {"gen": "object", "fields": {
                "inst": {"gen": "categorical", "values": {"a": 0.25, "b": 0.25, "c": 0.25, "d": 0.25}},
                "dns": {"gen": "template", "template": "dns_{hex}", "follows": "inst"}}}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        assert emu.cursor().execute("""select count(distinct "META"->>'inst'), count(distinct "META"->>'dns'),
            count(distinct ("META"->>'inst') || ("META"->>'dns')) from "RAW"."T"."SUBS" """).fetchone() == (4, 4, 4)


def test_ids_of_a_given_length_still_match_what_refers_to_them(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {
        "RAW.T.SERVERS": {"volume": {"rows_per_day": 50}, "columns": {"ID": {"gen": "hash_id", "length": 26, "named": ["93myk"]}}},
        "RAW.T.PINGS": {"volume": {"rows_per_day": 300}, "columns": {
            "N": {"gen": "sequence"}, "SERVER_ID": {"gen": "fk", "ref": "RAW.T.SERVERS", "length": 26}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        assert cur.execute("""select count(*) = count(distinct "ID"), min(length("ID")), max(length("ID")) from "RAW"."T"."SERVERS"
                              where "ID" <> '93myk' """).fetchone() == (True, 26, 26)
        # the id the models ask for by name is still there, and referred to
        assert cur.execute("""select count(*) from "RAW"."T"."SERVERS" where "ID" = '93myk' """).fetchone()[0] == 1
        assert cur.execute("""select count(*) from "RAW"."T"."PINGS" where "SERVER_ID" = '93myk' """).fetchone()[0] >= 1
        assert cur.execute("""select count(*), count(s."ID") from "RAW"."T"."PINGS" p left join "RAW"."T"."SERVERS" s
                              on s."ID" = p."SERVER_ID" where length(p."SERVER_ID") = 26""").fetchone()[1] > 100


def test_references_spare_the_rows_a_flag_of_the_parent_marks(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {
        "RAW.T.ACCOUNTS": {"volume": {"rows_per_day": 80}, "columns": {
            "ID": {"gen": "sequence"},
            "DELETED": {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.15, False: 0.85}, "every": 7}}},
        "RAW.T.REVIEWS": {"volume": {"rows_per_day": 300}, "columns": {
            "N": {"gen": "sequence"}, "ACCOUNT_ID": {"gen": "fk", "ref": "RAW.T.ACCOUNTS", "spares": "DELETED"}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 4))
        cur = emu.cursor()
        rows, marked = cur.execute("""select count(*), count_if("DELETED") from "RAW"."T"."ACCOUNTS" """).fetchone()
        assert 0.1 < marked / rows < 0.2
        assert cur.execute("""select count(*), count_if(a."ID" is null), count_if(a."DELETED") from "RAW"."T"."REVIEWS" r
                              left join "RAW"."T"."ACCOUNTS" a on a."ID" = r."ACCOUNT_ID" """).fetchone()[1:] == (0, 0)


def test_a_list_of_known_values_holds_each_at_most_once(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.TRACKS": {"volume": {"rows_per_day": 400}, "columns": {
        "ID": {"gen": "sequence"}, "SKUS": {"gen": "array", "values": ["professional", "enterprise"]}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        rows = emu.cursor().execute("""select distinct "SKUS"::varchar from "RAW"."T"."TRACKS" """).fetchall()
        assert {r[0] for r in rows} == {"[]", '["professional"]', '["enterprise"]', '["professional","enterprise"]', '["enterprise","professional"]'}


def test_a_pool_of_versions_holds_dotted_numbers(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.NPS": {"volume": {"rows_per_day": 400}, "columns": {
        "ID": {"gen": "sequence"}, "SERVER_VERSION": {"gen": "pool", "pool": "version", "size": 50, "dotted": True}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        seen, shaped = emu.cursor().execute("""select count(distinct "SERVER_VERSION"),
            count(distinct case when regexp_matches("SERVER_VERSION", '^[0-9]+[.][0-9]+[.][0-9]+$') then "SERVER_VERSION" end)
            from "RAW"."T"."NPS" """).fetchone()
        assert seen == shaped == 50


def test_a_table_generated_again_on_its_own_still_finds_the_rows_it_refers_to(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    raw = {"seed": 1, "start": "2025-01-01", "tables": {
        "RAW.T.ACCOUNTS": {"volume": {"rows_per_day": 50}, "columns": {"ID": {"gen": "sequence"}}},
        "RAW.T.ORDERS": {"volume": {"rows_per_day": 200}, "columns": {"N": {"gen": "sequence"}, "ACCOUNT": {"gen": "fk", "ref": "RAW.T.ACCOUNTS"}}}}}
    with serve() as emu:
        Warehouse(emu, Spec.from_raw(raw, tmp_path / "x.yml"), 1.0).advance_to(datetime(2025, 1, 3))
        raw["tables"]["RAW.T.ORDERS"]["columns"]["NOTE"] = {"gen": "template", "template": "note_{hex}"}
        again = Warehouse(emu, Spec.from_raw(raw, tmp_path / "y.yml"), 1.0)  # another warehouse object: nothing carried over
        again.advance_to(datetime(2025, 1, 3), only={"RAW.T.ORDERS"})
        assert emu.cursor().execute("""select count(*), count(a."ID"), count(o."NOTE") from "RAW"."T"."ORDERS" o
                                       left join "RAW"."T"."ACCOUNTS" a on a."ID" = o."ACCOUNT" """).fetchone() == (400, 400, 400)


def test_a_value_that_goes_by_another_column_holds_what_each_case_says(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.T.HISTORY": {"volume": {"rows_per_day": 300}, "columns": {
        "ID": {"gen": "sequence"},
        "FIELD": {"gen": "categorical", "values": {"amount": 0.3, "closedate": 0.3, "stagename": 0.4}},
        "OLDVALUE": {"gen": "by", "of": "FIELD", "else": {"gen": "template", "template": "oldvalue_{hex}"}, "cases": {
            "amount": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [0, 1000]},
            "closedate": {"gen": "time_offset", "format": "%Y-%m-%d", "p": [0, 1], "seconds": [0, 86400]}}}}}}}, tmp_path / "x.yml")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        cur = emu.cursor()
        assert cur.execute("""select count(*), count(try_cast("OLDVALUE" as bigint)) from "RAW"."T"."HISTORY" where "FIELD" = 'amount' """).fetchone()[0] > 50
        rows, dates, amounts, texts = cur.execute("""select count(*), count_if("FIELD" = 'closedate' and try_cast("OLDVALUE" as date) is not null),
            count_if("FIELD" = 'amount' and try_cast("OLDVALUE" as bigint) is not null), count_if("FIELD" = 'stagename' and "OLDVALUE" like 'oldvalue\\_%' escape '\\')
            from "RAW"."T"."HISTORY" """).fetchone()
        assert rows == dates + amounts + texts and min(dates, amounts, texts) > 50
