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
                                        "depends_on": {"nodes": []}}}, project_relations=set(), existing={})
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


def test_a_cast_teaches_only_about_what_it_reads_as_it_is():
    tree = parse("select split_part(regexp_substr(q, 'auc=[0-9]+'), '=', 2)::int, coalesce(o.f:at_risk, false)::boolean, "
                 "coalesce(o.f:n::int, m), (a + b)::float from o")
    reads = [leaf.sql(dialect="snowflake") for cast in tree.find_all(sqlglot.exp.Cast) for leaf in Fitter._cast_reads(cast.this)]
    assert "q" not in reads
    assert {"a", "b"} <= set(reads)
    assert any("at_risk" in r for r in reads)


def test_a_field_cast_to_a_number_or_a_boolean_is_generated_as_one():
    tables = {"RAW.Z.O": {"columns": {"F": {"gen": "object", "fields": {
        "at_risk": {"gen": "template", "template": "at_risk_{hex}"}, "n": {"gen": "template", "template": "n_{hex}"}}}}}}
    f = fitter(tables=tables)
    f.sources_of = lambda uid: ["RAW.Z.O"]
    tree = parse("select coalesce(o.f:at_risk, false)::boolean, o.f:n::int from RAW.Z.O o")
    for cast in tree.find_all(sqlglot.exp.Cast):
        hint = "BOOLEAN" if cast.to.is_type("boolean") else "DOUBLE"
        for leaf in Fitter._cast_reads(cast.this):
            assert f.retype_field("m", leaf, hint)
    fields = tables["RAW.Z.O"]["columns"]["F"]["fields"]
    assert fields["at_risk"]["type"] == "BOOLEAN" and fields["n"]["type"] == "BIGINT"


def test_a_column_missing_from_a_subquery_belongs_to_the_table_whose_star_it_selects():
    tree = parse("""with mt as (select user_id, max(ts) as mx from "RAW".a.cfg group by 1)
        select coalesce(r.installation, null) from (select s.* from "RAW".a.cfg s join mt on s.user_id = mt.user_id) s
        full outer join (select r.* from "RAW".b.cfg r join mt on r.user_id = mt.user_id) r on r.user_id = s.user_id""")
    tables = {"RAW.A.CFG": {"columns": {}}, "RAW.B.CFG": {"columns": {}}}
    f = fitter(trees={"m": tree}, tables=tables, existing={}, pending={}, unions={})
    f.add_column = lambda table, col: tables[table]["columns"].setdefault(col, {}) is not None
    assert f.missing_column("m", 'Values list "R" does not have a column named "INSTALLATION"')
    assert "INSTALLATION" in tables["RAW.B.CFG"]["columns"] and "INSTALLATION" not in tables["RAW.A.CFG"]["columns"]


def test_an_ambiguous_column_stays_with_the_table_a_model_names_it_with():
    freemium = parse('select s.*, charge as stripe_charge_id from "RAW".blapi.subs s left join "RAW".stripe.invoices on invoices.sub = s.id')
    invoices = parse('select invoices.charge, invoices.id from "RAW".stripe.invoices')
    free = {"gen": "template", "template": "charge_{hex}"}
    tables = {"RAW.BLAPI.SUBS": {"columns": {"CHARGE": dict(free)}}, "RAW.STRIPE.INVOICES": {"columns": {"CHARGE": dict(free)}}}
    f = fitter(trees={"a": freemium, "b": invoices}, sql_of={"a": freemium.sql().upper(), "b": invoices.sql().upper()}, tables=tables)
    assert f.ambiguous_column("a", "Ambiguous reference to column name \"CHARGE\" (use: 'S.CHARGE' or 'INVOICES.CHARGE')")
    assert "CHARGE" in tables["RAW.STRIPE.INVOICES"]["columns"] and "CHARGE" not in tables["RAW.BLAPI.SUBS"]["columns"]


def test_pieces_of_a_text_read_as_numbers_are_generated_as_numbers():
    tables = {"RAW.A.RELEASES": {"columns": {"VERSION": {"gen": "template", "template": "version_{hex}"}}}}
    tree = parse("select split_part(version, '.', 1)::number as major, split_part(version, '.', 3)::number as patch from RAW.A.RELEASES")
    f = fitter(trees={"m": tree}, tables=tables)
    f.sources_of = lambda uid: ["RAW.A.RELEASES"]
    assert f.split_numbers("m")
    assert tables["RAW.A.RELEASES"]["columns"]["VERSION"]["template"] == "{n}.{n}.{n}"


def test_an_error_inside_a_view_is_offered_to_the_view():
    nodes = {"model.p.child": {"resource_type": "model", "depends_on": {"nodes": ["model.p.view"]}},
             "model.p.view": {"resource_type": "model", "config": {"materialized": "view"}, "depends_on": {"nodes": []},
                              "compiled_code": "select 1"}}
    f = fitter(nodes=nodes)
    asked = []
    f.learn = lambda uid, message: asked.append(uid) or True
    assert f.learn_upstream("model.p.child", "Conversion Error")
    assert asked == ["model.p.view"]


def test_a_function_that_iterates_a_field_gets_an_array_in_that_field():
    from types import SimpleNamespace

    from fauxflake import objects

    tables = {"RAW.GA.EVENTS": {"columns": {"VALUE": {"gen": "array", "fields": {"key": "text", "value": "text"}}}}}
    tree = parse('select "PREP".s.parse_array(value[\'event_params\'])::variant as p from RAW.GA.EVENTS')
    emu = SimpleNamespace(_fs=SimpleNamespace(results_cache={objects._KEY: SimpleNamespace(functions={"PARSE_ARRAY": 1})}))
    f = fitter(trees={"m": tree}, tables=tables, emu=emu)
    f.sources_of, f._ancestors, f.origins = (lambda uid: ["RAW.GA.EVENTS"]), (lambda uid: set()), (lambda uid, name, depth=0: [name])
    assert f.udf_input("m", "Python exception occurred while executing the UDF: TypeError: cannot read property 'Symbol.iterator' of undefined")
    column = tables["RAW.GA.EVENTS"]["columns"]["VALUE"]
    assert column["gen"] == "object" and column["fields"]["event_params"]["gen"] == "array"


def test_a_test_on_a_column_the_model_renames_away_fails_everywhere():
    sql = "with source as (select * from RAW.A.T), renamed as (select status_id::number as id, status::number as status from source) select * from renamed"
    nodes = {"model.p.m": {"resource_type": "model"}}
    f = fitter(nodes=nodes, sql_of={"model.p.m": sql})
    test = {"attached_node": "model.p.m", "depends_on": {"nodes": ["model.p.m"]}}
    assert f.output_columns("model.p.m") == ["ID", "STATUS"]
    assert f.test_names_a_column_the_model_does_not_write(test, 'Referenced column "STATUS_ID" not found in FROM clause!')
    assert not f.test_names_a_column_the_model_does_not_write(test, 'Referenced column "STATUS" not found in FROM clause!')
    f = fitter(nodes=nodes, sql_of={"model.p.m": "select * from RAW.A.T"})
    assert f.output_columns("model.p.m") is None


def test_columns_read_past_a_written_out_listing_are_found_at_once():
    tree = parse("""with d as (select "ID", "TYPE", max(up) as valid_from from RAW.A.MEMBERS group by "ID", "TYPE"),
        renamed as (select id::number as member_id, type as member_type, requested_at::timestamp as requested_at,
                           expires_at::timestamp as expires_at, ldap::boolean as has_ldap, valid_from from d)
        select * from renamed""")
    f = fitter()
    ctes = {c.alias.upper(): c.this for c in tree.find_all(sqlglot.exp.CTE)}
    assert f.read_past_a_listing(tree, ctes, "RAW.A.MEMBERS", "REQUESTED_AT") == ["REQUESTED_AT", "EXPIRES_AT", "LDAP"]


def test_a_model_that_renames_columns_is_not_a_select_star_of_its_source():
    renames = "with source as (select email, optedout as opted_out from RAW.R.DATA) select * from source"
    stars = "with source as (select * from RAW.R.OPTOUTS) select * from source"
    f = fitter(by_rel={"A.B.X": "x", "A.B.Y": "y"}, nodes={"x": {}, "y": {}}, sql_of={"x": renames, "y": stars})
    assert f.passthrough("A.B.X") is None
    assert f.passthrough("A.B.Y") == "RAW.R.OPTOUTS"
