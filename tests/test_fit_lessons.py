"""What the fitter reads off a model's SQL, checked without running a project."""

import sqlglot

from fauxflake import emulator
from fauxflake.fit import Fitter, _same_format


def parse(sql):
    return sqlglot.parse_one(sql, read="snowflake")


def fitter(**state):
    f = object.__new__(Fitter)
    f.trees, f.sql_of, f.nodes, f.tables, f.by_rel, f.relation_owner = {}, {}, {}, {}, {}, {}
    f.retyped, f.learned, f.dirty = set(), [], False
    for key, value in state.items():
        setattr(f, key, value)
    return f


def test_time_of_day_only_from_a_date_and_a_time_joined_for_a_timestamp():
    used = lambda sql: Fitter._columns_used(parse(sql), "time_of_day")  # noqa: E731
    assert used("select to_timestamp(logdate || ' ' || logtime) from t") == ["LOGTIME"]
    assert used("select (logdate || ' ' || logtime)::timestamp from t") == ["LOGTIME"]
    # a surrogate key is a concatenation inside a cast too
    assert used("select md5(cast(coalesce(cast(a as text), 'x') || '-' || coalesce(cast(b as text), 'x') as text)) from t") == []
    assert used("select cast(a || ' ' || b as varchar) from t") == []


def test_formats_are_matched_to_the_expression_that_failed():
    tree = parse("select to_date(booked, 'MM/DD/YYYY'), to_date(split_part(week, ' - ', 2), 'MON DD, YYYY'), "
                 "to_date(right(task, 8), 'YYYYMMDD') from t")
    assert Fitter._columns_used(tree, "format", "%m/%d/%Y") == ["BOOKED"]
    assert Fitter._columns_used(tree, "format", "%b %d, %Y") == ["WEEK"]
    f = fitter(trees={"m": tree})
    assert f.split_format("m", "WEEK", "%b %d, %Y") == "%b %d, %Y - %b %d, %Y"  # every piece parses
    assert f.split_format("m", "TASK", "%Y%m%d") == "run_%Y%m%d"  # the date is how the text ends
    assert _same_format("%Y%m%d", "%Y%m%d")


def test_columns_compared_with_text_are_text():
    tree = parse("select nullif(goal, '')::float, case when role = 'CRO' then 1 end from t where id != 'Not In Calc'")
    assert Fitter._columns_used(tree, "empty") == ["GOAL"]
    assert Fitter._columns_used(tree, "literal", "CRO") == ["ROLE"]
    assert Fitter._columns_used(tree, "literal", "Not In Calc") == ["ID"]


def test_every_column_a_model_reads_from_a_table_is_found_at_once():
    tree = parse("""
        with m as (select user_id, max(ts) as mx from "RAW".a.cfg group by 1)
        select coalesce(r.d, s.d) as d, max(coalesce(r.lowercase, s.lowercase)) as lo, s.n
        from (select s.* from "RAW".a.cfg s join m on s.user_id = m.user_id) s
        full outer join (select r.* from "RAW".b.cfg r) r on r.user_id = s.user_id group by 1""")
    f = fitter()
    assert set(f.read_through(tree, "RAW.A.CFG")) == {"USER_ID", "TS", "N", "D", "LOWERCASE"}
    assert set(f.read_through(tree, "RAW.B.CFG")) == {"D", "USER_ID", "LOWERCASE"}


def test_unlisted_columns_of_a_model_come_from_the_table_whose_star_it_selects():
    sql = """with fs as (select s.*, p.sku, row_number() over (partition by s.id order by s.t desc) as row_num
                         from RAW.BLAPI.SUBSCRIPTIONS_VERSION s
                         join RAW.STRIPE.SUBSCRIPTIONS su on su.id = s.stripe_id
                         join RAW.BLAPI.PRODUCTS p on s.product_id = p.id)
             select * from fs where row_num = 1"""
    f = fitter(by_rel={"A.B.M": "m"}, nodes={"m": {}}, sql_of={"m": sql})
    assert f.passthrough("A.B.M") == "RAW.BLAPI.SUBSCRIPTIONS_VERSION"


def test_tables_a_model_looks_up_by_name_and_does_not_find_are_generated():
    f = fitter(nodes={"model.p.focal": {"name": "focal", "database": "ANALYTICS", "relation_name": "ANALYTICS.E.FOCAL",
                                        "depends_on": {"nodes": []}}})
    emulator.EMPTY_LOOKUPS.append(
        'select distinct table_schema as "table_schema", table_name as "table_name" from RAW.information_schema.tables '
        "where table_schema ilike 'board_prod' and table_name not in ('TRACKS', 'USERS') and lower(table_name) in ('event', 'pages')\n"
        '/* {"app": "dbt", "node_id": "model.p.focal"} */')
    try:
        assert f.looked_for("model.p.focal")
        assert set(f.tables) == {"RAW.BOARD_PROD.EVENT", "RAW.BOARD_PROD.PAGES"}
        assert not f.looked_for("model.p.focal")  # nothing more to add the second time
    finally:
        emulator.EMPTY_LOOKUPS.clear()
