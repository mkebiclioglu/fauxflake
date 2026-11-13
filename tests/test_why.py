"""`fauxflake why`: where a model's rows stop."""

import pytest

from fauxflake import why


@pytest.fixture()
def cursor(emu):
    conn = emu._fs.connect(database="ANALYTICS", schema="CI")
    emu.ensure_database("ANALYTICS", "CI")
    cur = conn.cursor()
    cur.execute("create or replace table orders as select 1 as id, 'open' as status, 10 as customer_id, "
                "parse_json('{\"items\": []}') as payload union all select 2, 'open', 11, parse_json('{\"items\": []}')")
    cur.execute("create or replace table customers as select 77 as id, 'eu' as region")
    yield cur
    conn.close()


def test_a_condition_no_row_meets_is_named_with_what_the_column_holds(cursor):
    found = why.explain(cursor, "with o as (select * from orders where id > 0), "
                                "shipped as (select * from o where status = 'shipped' and id < 100) select * from shipped")
    assert (found.scope, found.kind) == ("shipped", "where")
    assert found.clause == "status = 'shipped'" and ("status", ["open"]) in found.columns


def test_a_join_that_matches_nothing_shows_both_sides(cursor):
    found = why.explain(cursor, "select o.id from orders o join customers c on o.customer_id = c.id")
    assert found.kind == "join" and found.inputs == {"orders": 2, "customers": 1}
    assert dict(found.columns) == {"o.customer_id": [10, 11], "c.id": [77]} or \
        {k: sorted(v) for k, v in found.columns} == {"o.customer_id": [10, 11], "c.id": [77]}


def test_an_empty_array_under_flatten_and_an_empty_input(cursor):
    found = why.explain(cursor, "select f.value from orders, lateral flatten(input => payload:items) f")
    assert found.kind == "flatten" and "payload" in found.clause.lower()
    cursor.execute("create or replace table nothing as select 1 as id where false")
    assert why.explain(cursor, "select * from nothing where id = 1").kind == "input"


def test_a_key_that_repeats_is_traced_to_the_join_that_multiplies_rows(cursor):
    cursor.execute("create or replace table tags as select 10 as customer_id, 'a' as tag union all select 10, 'b' union all select 11, 'c'")
    found = why.explain_duplicates(cursor, "with o as (select id as order_id, customer_id from orders) "
                                           "select o.order_id, t.tag from o join tags t on o.customer_id = t.customer_id", "order_id")
    assert found.kind == "join" and "2 row(s) before it, 3 after" in found.detail
    assert found.columns == [("t.customer_id (value, times) in t", [(10, 2)])]
    again = why.explain_duplicates(cursor, "select customer_id from tags", "customer_id")
    assert again.kind == "input" and "1 repeated value" in again.detail
