from datetime import date, timedelta
"""The spec can be inferred from the dbt project alone."""

from fauxflake.dbt import DbtProject, emulator_output
from fauxflake.init import scan_model, SourceUse, write_spec
from fauxflake.spec import Spec

from conftest import EXAMPLE


def test_init_reads_the_project(emu, tmp_path):
    path = tmp_path / "fauxflake.yml"
    write_spec(DbtProject(EXAMPLE, emulator_output(emu.port, "ANALYTICS")), path, "ANALYTICS")
    tables = Spec.load(path).tables
    orders = tables["RAW.SHOP.ORDERS"].columns
    assert set(orders) == {"ORDER_ID", "CUSTOMER_ID", "STATUS", "CHANNEL", "AMOUNT", "ORDERED_AT", "UPDATED_AT", "_LOADED_AT"}
    # tests declared on the mart are traced back through staging to the raw table
    assert orders["ORDER_ID"] == {"gen": "sequence"}
    assert orders["CUSTOMER_ID"]["ref"] == "RAW.SHOP.CUSTOMERS"
    assert set(orders["STATUS"]["values"]) == {"placed", "shipped", "delivered", "returned"}
    assert orders["AMOUNT"]["type"].replace(" ", "") == "DECIMAL(12,2)"  # from the cast in stg_orders
    assert orders["_LOADED_AT"] == {"gen": "loaded_at"} and orders["ORDERED_AT"] == {"gen": "event_time"}
    assert tables["RAW.SHOP.EVENTS"].columns["CUSTOMER_ID"]["gen"] == "fk"  # inferred from the key's name


def test_lineage_through_the_usual_staging_pattern():
    sources = {"RAW.APP.USERS": SourceUse()}
    out = scan_model("""
        with source as (select * from raw.app.users),
        renamed as (select id as user_id, email_address::varchar as email, created::timestamp_ntz as created_at from source)
        select * from renamed""", sources)
    assert out == {"USER_ID": ("RAW.APP.USERS", "ID"), "EMAIL": ("RAW.APP.USERS", "EMAIL_ADDRESS"),
                   "CREATED_AT": ("RAW.APP.USERS", "CREATED")}
    assert set(sources["RAW.APP.USERS"].columns) == {"ID", "EMAIL_ADDRESS", "CREATED"}


def test_lineage_through_a_deduplicating_source_cte():
    sources = {"RAW.GITLAB.ISSUES": SourceUse()}
    out = scan_model("""
        with source as (
            select *, row_number() over (partition by id order by _sdc_batched_at desc) as row_num
            from raw.gitlab.issues),
        renamed as (select title, id as issue_id, created_at as created_at_ts from source where row_num = 1)
        select * from renamed""", sources)
    assert set(sources["RAW.GITLAB.ISSUES"].columns) == {"ID", "_SDC_BATCHED_AT", "TITLE", "CREATED_AT"}
    assert out["ISSUE_ID"] == ("RAW.GITLAB.ISSUES", "ID")


def test_semi_structured_and_text_date_columns_are_inferred():
    sources = {"RAW.GH.ISSUES": SourceUse()}
    scan_model("""
        select id, user:login::string as author, user:id::int as author_id, payload:repo.name::string as repo,
               to_date(closed_on, 'YYYY/MM/DD') as closed_date, f.value::string as label
        from raw.gh.issues, lateral flatten(input => labels) f""", sources)
    use = sources["RAW.GH.ISSUES"]
    # `[]` is "each element": what is flattened holds text
    assert use.paths == {"USER": {"login": "TEXT", "id": "INT"}, "PAYLOAD": {"repo.name": "TEXT"}, "LABELS": {"[]": "TEXT"}}
    assert use.arrays == {"LABELS"} and use.formats == {"CLOSED_ON": "%Y/%m/%d"}

    scan_model("""select parse_json(context):app.version::string as v, parse_json(raw_blob) as blob
                  from raw.gh.issues where user_id <> 'USLACKBOT' and state = 'open' and created = '2025-01-01'""", sources)
    assert use.json_text == {"CONTEXT", "RAW_BLOB"} and use.paths["CONTEXT"] == {"app.version": "TEXT"}
    assert use.texts == {"USER_ID", "STATE"}  # a date-looking literal says nothing about the type
    # `<> 'USLACKBOT'` names a value to leave out, not one to generate rows with
    assert use.literals == {"STATE": {"open"}}


def test_generated_objects_and_text_dates_are_usable(emu):
    import snowflake.connector
    import yaml
    from datetime import datetime
    from fauxflake.generate import Warehouse

    spec_text = yaml.safe_dump({"seed": 1, "start": "2025-01-01", "tables": {"RAW.GH.ISSUES": {
        "volume": {"rows_per_day": 50}, "columns": {
            "ID": {"gen": "sequence"},
            "USER": {"gen": "object", "fields": {"login": {"gen": "template", "template": "u{n}"},
                                                 "id": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 9]},
                                                 "org.name": {"gen": "constant", "value": "acme"}}},
            "LABELS": {"gen": "array"},
            "CLOSED_ON": {"gen": "time_offset", "format": "%Y/%m/%d", "p": [0, 1], "seconds": [0, 86400]},
            "CREATED_AT": {"gen": "event_time"}}}}})
    path = EXAMPLE.parent / "tests" / ".tmp_spec.yml"
    path.write_text(spec_text)
    try:
        Warehouse(emu, Spec.load(path), 1.0).advance_to(datetime(2025, 1, 5))
    finally:
        path.unlink()
    conn = snowflake.connector.connect(**emu.connect_kwargs(database="RAW", schema="GH"))
    row = conn.cursor().execute("""
        select count(*), count(distinct user:login::string), min(user:id::int) >= 1, max(user:org.name::string),
               min(to_date(closed_on, 'YYYY/MM/DD')) >= '2025-01-01', count(f.value) > 0
        from raw.gh.issues, lateral flatten(input => labels, outer => true) f""").fetchone()
    assert row[1] == 200 and row[2:] == (True, "acme", True, True)


def test_union_branches_share_what_is_known_about_a_column():
    sources = {"RAW.GH.ISSUES": SourceUse(), "RAW.GL.ISSUES": SourceUse()}
    links = []
    scan_model("""
        select id, comments as comment_count from raw.gh.issues
        union all
        select id, user_notes_count::int as comment_count from raw.gl.issues""", sources, links=links)
    assert {("RAW.GH.ISSUES", "COMMENTS"), ("RAW.GL.ISSUES", "USER_NOTES_COUNT")} in links


def test_unknown_functions_get_placeholders():
    from fauxflake.emulator import engine
    from fauxflake.init import udf_stubs, unknown_functions

    manifest = {"nodes": {"model.p.m": {"resource_type": "model", "compiled_code": (
        "select util.public.parse_agent(ua) as agent, my_score(a, b), my_score(a), upper(a), soundex(a), "
        "ratio_to_report(x) over () from raw.t")}}}
    with engine() as emu:
        missing = unknown_functions(manifest, emu)
        assert missing == {"parse_agent": {1}, "my_score": {1, 2}}
        stubs = udf_stubs(missing)
        assert stubs["my_score"] == "(a0) as null::varchar, (a0, a1) as null::varchar"
        for name, body in stubs.items():  # each placeholder is a valid definition
            emu.duck.execute(f"create or replace macro _fs_global.main._udf_{name}{body}")


def test_joined_columns_share_values():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    manifest = {
        "sources": {
            "source.p.s.users": source("users", {"id": "NUMBER", "plan_code": "VARCHAR"}),
            "source.p.s.events": source("events", {"event_id": "NUMBER", "actor": "NUMBER", "plan": "VARCHAR"}),
            "source.p.s.plans": source("plans", {"code": "VARCHAR", "label": "VARCHAR", "created": "DATE"}),
        },
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select e.event_id, u.id, p.label
            from raw.s.events e
            join raw.s.users u on e.actor = u.id
            left join raw.s.plans p on lower(u.plan_code) = p.code and p.code = e.plan"""}},
    }
    spec, notes = build_spec(manifest, "A")
    tables = spec["tables"]
    assert tables["RAW.S.EVENTS"]["columns"]["ACTOR"] == {"gen": "fk", "ref": "RAW.S.USERS", "skew": 1.5}
    # plans are looked up by their code: one code each; the columns joined to it draw from those codes
    assert tables["RAW.S.PLANS"]["columns"]["CODE"] == {"gen": "template", "template": "plan_{n}"}
    assert tables["RAW.S.USERS"]["columns"]["PLAN_CODE"] == tables["RAW.S.EVENTS"]["columns"]["PLAN"] == {
        "gen": "pool", "pool": "plan", "size": 6000}


def _scan(models: dict[str, str], sources: list[str]):
    """Scan `models` (name -> SQL, in dependency order) the way `analyse` does."""
    from fauxflake.init import _PATHS

    uses = {name: SourceUse() for name in sources}
    by_relation, joins, dates, outputs = {}, [], [], {}
    for name, sql in models.items():
        outputs[name] = by_relation[name.upper()] = scan_model(sql, uses, by_relation, [], joins, dates)
    return uses, outputs, joins, dates, _PATHS


def test_a_place_inside_a_document_is_followed_through_models_and_flatten():
    uses, outputs, _, _, paths = _scan({
        "db.s.shards": "select value:chunk::variant as chunk, _part from raw.near.shards",
        "db.s.chunks": "with s as (select * from db.s.shards where chunk != 'null') "
                       "select chunk:transactions::array as chunk_transactions, chunk:header:hash::string as chunk_hash from s",
        "db.s.txs": "select value:transaction:hash::string as tx_hash, value:outcome:ids[0]::string as first_id, value as tx, index "
                    "from db.s.chunks, lateral flatten(input => chunk_transactions) where array_size(chunk_transactions) > 0",
        "db.s.final": "select t.tx_hash, t.tx:transaction:nonce::number as nonce from db.s.txs t where t.tx:transaction:kind = 'call'",
    }, ["RAW.NEAR.SHARDS"])
    use = uses["RAW.NEAR.SHARDS"]
    assert use.paths["VALUE"] == {
        "chunk": "VARIANT", "chunk.transactions": "[]", "chunk.header.hash": "TEXT", "chunk.transactions[]": None,
        "chunk.transactions[].transaction.hash": "TEXT", "chunk.transactions[].outcome.ids[]": "TEXT",
        "chunk.transactions[].transaction.nonce": "DECIMAL(38, 0)", "chunk.transactions[].transaction.kind": None}
    assert use.path_literals == {"VALUE": {"chunk.transactions[].transaction.kind": {"call"}}}
    assert outputs["db.s.final"][paths]["TX_HASH"] == ("RAW.NEAR.SHARDS", "VALUE", ("chunk", "transactions", "[]", "transaction", "hash"))


def test_objects_and_arrays_a_model_puts_together_keep_what_they_are_made_of():
    uses, _, _, _, _ = _scan({
        "db.s.a": "select id, object_construct('actions', v:actions, 'hash', v:hash) as tx, array_agg(v:outcome) within group (order by id) as outcomes "
                  "from raw.t.docs group by 1, 2",
        "db.s.b": "select f.value:kind::string as kind, a.outcomes[0]:status::string as status from db.s.a a, lateral flatten(input => a.tx:actions) f",
    }, ["RAW.T.DOCS"])
    assert {"actions[]", "actions[].kind", "outcome.status"} <= set(uses["RAW.T.DOCS"].paths["V"])


def test_encoded_text_keys_of_an_object_and_keys_tested_for_being_there():
    uses, _, _, _, _ = _scan({
        "db.s.a": "select try_parse_json(try_base64_decode_string(v:call:args)):prices as prices, f.key as action_name, "
                  "iff(v:status:Failure is not null, 'Fail', 'Success') as status "
                  "from raw.t.docs, lateral flatten(input => v:action) f where f.key = 'FunctionCall'",
    }, ["RAW.T.DOCS"])
    use = uses["RAW.T.DOCS"]
    assert "call.args~b64~json.prices" in use.paths["V"] and "action.FunctionCall" in use.paths["V"]
    assert use.path_optional == {"V": {"status.Failure"}}


def test_what_a_downstream_model_compares_with_reaches_the_source_column():
    uses, _, joins, dates, _ = _scan({
        "db.s.stg": "select id, kind as event_kind, block as block_number, acct from raw.t.events",
        "db.s.mid": "select * from db.s.stg where block_number between 100 and 200",
        "db.s.top": "select m.id from db.s.mid m join db.s.other o on m.id = o.ref where m.event_kind in ('swap', 'mint') "
                    "and m.block_number >= 7500 and m.acct = current_account_name() and m.event_kind != 'burn' "
                    "and o.day = '2022-04-01' and o.day < '2030-01-01'",
        }, ["RAW.T.EVENTS"])
    use = uses["RAW.T.EVENTS"]
    assert use.literals == {"KIND": {"swap", "mint"}, "ACCT": {"FAUXFLAKE"}}  # not 'burn': that one is filtered out
    assert use.bounds["BLOCK"] == {100.0, 200.0, 7500.0}
    assert sorted(dates) == [("before", date(2030, 1, 1)), ("in", date(2022, 4, 1))]


def test_the_history_covers_the_dates_the_sql_names():
    from fauxflake.init import history_window

    assert history_window([]) == (date(2025, 1, 1), None)
    assert history_window([("after", date(2021, 9, 1)), ("before", date(2030, 1, 1))]) == (date(2025, 1, 1), None)
    start, span = history_window([("in", date(2022, 4, 1)), ("in", date(2022, 6, 30))])
    assert start == date(2022, 4, 1) and start + timedelta(days=span) > date(2022, 6, 30)


def test_ids_inside_documents_are_keys_and_links_not_a_shared_pool():
    from fauxflake.init import _share_join_values

    template = lambda name: {"gen": "template", "template": name + "_{hex}"}  # noqa: E731
    tables = {"RAW.T.DOCS": {"columns": {"V": {"gen": "object", "fields": {
        "receipts[].receipt_id": template("receipt_id"), "receipts[].outcome.receipt_ids[]": template("receipt_ids"),
        "txs[].outcome.receipt_ids[]": template("receipt_ids")}}}}}
    place = lambda path: ("RAW.T.DOCS", "V", path)  # noqa: E731
    _share_join_values(tables, [(place("receipts[].receipt_id"), place("receipts[].outcome.receipt_ids[]")),
                                (place("txs[].outcome.receipt_ids[]"), place("receipts[].receipt_id"))], [])
    fields = tables["RAW.T.DOCS"]["columns"]["V"]["fields"]
    assert fields["receipts[].receipt_id"]["gen"] == "hash_id"
    # each referring field points at its own element of the row before (`slot`): an id has one parent at most
    # a small tree in every row: the transactions name ids of the list, which name the next one as their child
    assert fields["txs[].outcome.receipt_ids[]"] == {"gen": "link", "domain": fields["receipts[].receipt_id"]["domain"],
                                                     "depth": 1, "layered": True, "slot": 1, "tree": True}
    assert fields["receipts[].outcome.receipt_ids[]"]["slot"] == 0 and fields["receipts[].outcome.receipt_ids[]"]["within"]


def test_the_looked_up_side_of_a_join_holds_each_value_once():
    uses, _, joins, _, _ = _scan({
        "db.s.events": "select session_ref, kind from raw.t.events",
        "db.s.sessions": "select session_code, started from raw.t.sessions",
        "db.s.joined": "select e.kind, s.started from db.s.events e join db.s.sessions s on e.session_ref = s.session_code",
    }, ["RAW.T.EVENTS", "RAW.T.SESSIONS"])
    assert (("RAW.T.EVENTS", "SESSION_REF"), ("RAW.T.SESSIONS", "SESSION_CODE")) in joins

    import collections

    from fauxflake.init import _share_join_values

    counted = collections.Counter()
    scan_model("select e.kind from raw.t.events e join raw.t.sessions s on e.session_ref = s.session_code",
               {"RAW.T.EVENTS": SourceUse(), "RAW.T.SESSIONS": SourceUse()}, {}, [], [], [], counted)
    assert counted == {("RAW.T.SESSIONS", "SESSION_CODE", ""): 1, ("RAW.T.EVENTS", "SESSION_REF", ""): -1}

    template = lambda name: {"gen": "template", "template": name + "_{hex}"}  # noqa: E731
    tables = {"RAW.T.EVENTS": {"columns": {"SESSION_REF": template("session_ref")}},
              "RAW.T.SESSIONS": {"columns": {"SESSION_CODE": template("session_code")}}}
    lookups = collections.Counter({("RAW.T.SESSIONS", "SESSION_CODE", ""): 1, ("RAW.T.EVENTS", "SESSION_REF", ""): -1})
    _share_join_values(tables, joins, [], lookups)
    # sessions are looked up: one code each; events refer to those codes
    assert tables["RAW.T.SESSIONS"]["columns"]["SESSION_CODE"] == {"gen": "template", "template": "session_ref_{n}"}
    assert tables["RAW.T.EVENTS"]["columns"]["SESSION_REF"]["gen"] == "pool"


def test_filters_on_null_and_on_the_future_shape_the_column():
    uses, _, _, _, _ = _scan({
        "db.s.stg": "select id, converted as converted_at, expires as expire_at, seen from raw.t.leads",
        "db.s.open": "select id from db.s.stg where converted_at is null and seen is not null",
        "db.s.live": "select id, max(expire_at) m from db.s.stg where expire_at >= current_date group by 1",
    }, ["RAW.T.LEADS"])
    use = uses["RAW.T.LEADS"]
    assert use.nullable == {"CONVERTED"} and use.future == {"EXPIRES"}


def test_a_name_in_a_subquery_over_a_built_relation_is_not_a_source_column():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.APP.EVENT": SourceUse()}
    scan_model("""
        select m.id, max(m.ts) as max_ts from raw.app.event m
        where m.ts > (select max(max_ts) from analytics.events.by_date where _dbt_source_relation = 'x')
        group by 1""", sources)
    assert "_DBT_SOURCE_RELATION" not in sources["RAW.APP.EVENT"].columns
    assert {"ID", "TS"} <= set(sources["RAW.APP.EVENT"].columns)


def test_a_joined_model_that_selects_one_tables_star_passes_that_table_through():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.CRM.ITEM": SourceUse()}
    latest = scan_model("""
        select distinct o.id as sfid, o.* from raw.crm.item o
        join (select id, max(_seq) as seq from raw.crm.item group by id) oo on o.id = oo.id and o._seq = oo.seq
        where not isdeleted""", sources)
    scan_model("select sfid, count(*) from analytics.crm.item where product_type = 'Recurring' group by 1",
               sources, {"ANALYTICS.CRM.ITEM": latest})
    use = sources["RAW.CRM.ITEM"]
    assert use.literals["PRODUCT_TYPE"] == {"Recurring"}
    assert "SFID" not in use.columns and "ID" in use.columns


def test_a_filter_on_a_derived_table_that_is_a_star_of_a_source_reaches_the_source():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.NS.TX": SourceUse()}
    scan_model("""
        select inv.id, so.id as so_id from (select * from raw.ns.tx where kind = 'INV') inv
        left join (select * from raw.ns.tx where kind = 'SO') so on so.ref = inv.ref
        where inv.void = 'F'""", sources)
    use = sources["RAW.NS.TX"]
    assert use.literals["VOID"] == {"F"} and use.literals["KIND"] == {"INV", "SO"}


def test_a_cte_is_found_however_it_is_capitalised_and_bare_columns_beside_a_join_are_its_own():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.C.ACCOUNT": SourceUse(), "RAW.C.OPP": SourceUse()}
    out = scan_model("""
        with ACCT as (select a.id as account_id, coalesce(a.geo, a.territory) as geo, a.gov as government from raw.c.account a),
        master as (select o.account_id, o.amount from raw.c.opp o)
        select master.account_id, government, geo from master left join acct on acct.account_id = master.account_id""", sources)
    scan_model("select * from analytics.f.tx where geo = 'AMER' and government = 'Yes'", sources, {"ANALYTICS.F.TX": out})
    assert sources["RAW.C.ACCOUNT"].literals == {"GEO": {"AMER"}, "GOV": {"Yes"}}


def test_a_column_read_as_a_truth_value_is_a_boolean():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.C.ITEM": SourceUse()}
    scan_model("""select id, iff(active, 1, 0) as a, case when flagged then 1 end as f from raw.c.item
                  where not manual_override and is_won = true and kind = 'x' and amount > 3""", sources)
    columns = sources["RAW.C.ITEM"].columns
    assert {columns[c] for c in ("ACTIVE", "MANUAL_OVERRIDE", "IS_WON")} == {"BOOLEAN"}
    assert columns["KIND"] != "BOOLEAN" and columns["AMOUNT"] != "BOOLEAN" and columns["ID"] != "BOOLEAN"


def test_membership_in_a_subquery_links_the_two_columns_like_a_join():
    from fauxflake.init import SourceUse, scan_model

    sources = {"RAW.T.ACTIVITY": SourceUse(), "RAW.T.SERVERS": SourceUse()}
    joins = []
    scan_model("""
        with sdd as (select s.id as server_id, s.day from raw.t.servers s)
        select a.user_id, max(a.ts) as last_seen from raw.t.activity a
        where a.user_id in (select server_id from sdd group by 1) and a.kind not in (select kind from raw.t.servers)
        group by 1""", sources, joins=joins)
    assert joins == [(("RAW.T.ACTIVITY", "USER_ID", ""), ("RAW.T.SERVERS", "ID", ""))]


def test_a_column_tested_with_a_regular_expression_gets_texts_that_match():
    import re

    from fauxflake.init import build_spec, regex_examples

    assert regex_examples(r"(yahoo)|(gmail)") == ["yahoo", "gmail"]
    assert all(re.fullmatch(r"\d+\.\d+", t) for t in regex_examples(r"\\d+\.\\d+"))  # as written in a SQL string
    assert regex_examples("(?P<a>x)(?P=a)") == []  # beyond it: nothing rather than something wrong
    manifest = {
        "sources": {"source.p.s.log": {"database": "RAW", "schema": "S", "name": "log", "identifier": "log", "columns": {}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select uri, case when rlike(backend, '.*https_git$') then 'git' when regexp_like(backend, '.*registry$') then 'reg' end as kind
            from raw.s.log where regexp_like(uri, '^/[1-9]{1,3}[.][0-9]{1,3}/.*') and not rlike(agent, 'bot.*')"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.LOG"]["columns"]
    uris = [t for t in columns["URI"]["templates"] if "{" not in t]
    assert uris and all(re.fullmatch(r"/[1-9]{1,3}[.][0-9]{1,3}/.*", t) for t in uris)
    backends = columns["BACKEND"]["templates"]
    assert any(t.endswith("https_git") for t in backends) and any(t.endswith("registry") for t in backends)
    assert "templates" not in columns["AGENT"]  # a pattern rows are dropped by is not one to generate


def test_names_that_run_words_together_or_carry_a_custom_field_suffix_are_still_read():
    from fauxflake.init import guess

    assert {guess(c, None).get("type") for c in ("CLOSEDATE", "CREATEDDATE", "TRIAL_REQ_DATE__C", "TRANDATE")} == {"DATE"}
    assert guess("ISDELETED", None)["type"] == guess("IS_ACTIVE__C", None)["type"] == "BOOLEAN"
    assert guess("AMOUNT__C", None)["type"].startswith("DECIMAL")
    assert {guess(c, None)["gen"] for c in ("LAST_UPDATE", "CANDIDATE", "ISSUE", "GEO__C")} == {"template"}


def test_pieces_of_a_split_text_that_rows_are_kept_by_are_generated():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.log": {"database": "RAW", "schema": "S", "name": "log", "identifier": "log", "columns": {}}},
        "nodes": {
            "model.p.stg": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.STG", "compiled_code": """
                with renamed as (select split_part(request, ' ', 1) as http_method, split_part(request, ' ', 2) as url,
                                        split_part(request, ' ', 3) as protocol, status::int as status from raw.s.log)
                select * from renamed"""},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["model.p.stg"]}, "relation_name": "A.B.M", "compiled_code": """
                select url, count(*) from a.b.stg
                where http_method = 'POST' and url like any ('%/api/v1/send_push', '%/api/v1/ack') and protocol != 'HTTP/1.0'
                group by 1"""}},
    }
    request = build_spec(manifest, "A")[0]["tables"]["RAW.S.LOG"]["columns"]["REQUEST"]
    assert request["gen"] == "template"
    assert request["templates"] == ["POST {hex}/api/v1/ack {hex}", "POST {hex}/api/v1/send_push {hex}", "{hex} {hex} {hex}"]  # `_` in LIKE stands for any one character


def test_an_id_asked_for_by_name_through_a_foreign_key_exists_in_the_table_it_refers_to(emu, tmp_path):
    from fauxflake.generate import Warehouse
    from fauxflake.init import build_spec

    def source(name):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name, "columns": {}}

    def test(name, model, column, **kwargs):
        return {"resource_type": "test", "attached_node": model, "column_name": column, "depends_on": {"nodes": kwargs.pop("nodes", [model])},
                "test_metadata": {"name": name, "kwargs": {"column_name": column, **kwargs}}}

    manifest = {
        "sources": {"source.p.s.servers": source("servers"), "source.p.s.events": source("events")},
        "nodes": {
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
                select e.event_id, e.server_id, s.id from raw.s.events e join raw.s.servers s on s.id = e.server_id
                where e.server_id = '93mykbogbjfrbbdqphx3zhze5c'"""},
            "test.p.u": test("unique", "source.p.s.servers", "id"),
            "test.p.r": test("relationships", "source.p.s.events", "server_id", to="source('s','servers')", field="id",
                             nodes=["source.p.s.servers", "source.p.s.events"]),
        },
    }
    spec, _ = build_spec(manifest, "A")
    assert spec["tables"]["RAW.S.EVENTS"]["columns"]["SERVER_ID"]["gen"] == "fk"
    assert spec["tables"]["RAW.S.SERVERS"]["columns"]["ID"] == {"gen": "hash_id", "named": ["93mykbogbjfrbbdqphx3zhze5c"]}


def test_a_key_the_sql_takes_apart_keeps_its_shape():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.releases": {"database": "RAW", "schema": "S", "name": "releases", "identifier": "releases", "columns": {}}},
        "nodes": {
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
                select version, try_to_decimal(split_part(version, '.', 1)) as major, split_part(version, '.', 2)::int as minor
                from raw.s.releases"""},
            "test.p.u": {"resource_type": "test", "attached_node": "source.p.s.releases", "column_name": "version",
                         "depends_on": {"nodes": ["source.p.s.releases"]},
                         "test_metadata": {"name": "unique", "kwargs": {"column_name": "version"}}}},
    }
    assert build_spec(manifest, "A")[0]["tables"]["RAW.S.RELEASES"]["columns"]["VERSION"] == {"gen": "hash_id", "template": "{n}.{n}"}


def test_a_column_handed_to_an_array_function_is_a_list():
    sources = {"RAW.Z.TICKETS": SourceUse()}
    scan_model("""select t.id, array_to_string(custom_fields, ',') as s, array_size(tags) as n, array_contains('x'::variant, labels) as l,
                  upper(title) as title from raw.z.tickets t""", sources)
    assert sources["RAW.Z.TICKETS"].arrays == {"CUSTOM_FIELDS", "TAGS", "LABELS"}


def test_like_any_names_patterns_too():
    sources = {"RAW.Z.PLANS": SourceUse()}
    scan_model("""select case when tier like any ('%Self-Managed - Starter%', '%Support%') then 'sm' end as kind, v:name::varchar as n
                  from raw.z.plans where lower(label) like any ('tap-%', 'target-%') and v:kind like any ('%gold%', '%silver%')""", sources)
    use = sources["RAW.Z.PLANS"]
    assert use.patterns == {"TIER": {"%Self-Managed - Starter%", "%Support%"}, "LABEL": {"tap-%", "target-%"}}
    assert use.path_patterns == {"V": {"kind": {"%gold%", "%silver%"}}}


def test_a_value_that_must_be_the_same_for_every_row_of_a_group_follows_the_group():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.lic": {"database": "RAW", "schema": "S", "name": "lic", "identifier": "lic", "columns": {}}},
        "nodes": {
            "model.p.daily": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.DAILY", "compiled_code": """
                select license_id, users::int as licensed_seats, issued::timestamp as starts_at, server_id, ts::date as day from raw.s.lic"""},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["model.p.daily"]}, "relation_name": "A.B.M", "compiled_code": """
                select license_id from a.b.daily group by license_id
                having count(distinct licensed_seats) = 1 and count(distinct starts_at) = 1 and count(*) > 2"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.LIC"]["columns"]
    assert columns["USERS"]["follows"] == columns["ISSUED"]["follows"] == "LICENSE_ID"
    assert "follows" not in columns["SERVER_ID"] and "follows" not in columns["TS"]


def test_a_document_field_tested_with_a_regular_expression_gets_texts_that_match():
    sources = {"RAW.N.RECEIPTS": SourceUse()}
    staged = scan_model("select r:id::string as id, r:outcome.status::string as status_value from raw.n.receipts", sources)
    scan_model("select id, regexp_substr(status_value, 'Success') as ok from a.b.rec where not regexp_like(id, 'x.*')",
               sources, {"A.B.REC": staged})
    assert sources["RAW.N.RECEIPTS"].path_literals == {"R": {"outcome.status": {"Success"}}}


def test_lineage_follows_a_case_to_the_column_it_falls_back_to():
    sources = {"RAW.T.SERVER": SourceUse()}
    staged = scan_model("""select case when user_id = 'x1' and coalesce(inst_a, inst_b) is not null then 'fixed'
                                       else coalesce(inst_a, inst_b) end as installation_id from raw.t.server""", sources)
    scan_model("select * from a.b.stg where installation_id in ('abc', 'def')", sources, {"A.B.STG": staged})
    assert sources["RAW.T.SERVER"].literals["INST_A"] == {"abc", "def"}


def test_a_test_that_says_one_time_is_after_another_orders_them():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.releases": {"database": "RAW", "schema": "S", "name": "releases", "identifier": "releases", "columns": {}}},
        "nodes": {
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
                select version, release_date::date as release_date, rc1_date::date as rc1_date, other_date::date as o from raw.s.releases"""},
            "test.p.t": {"resource_type": "test", "attached_node": "model.p.m", "depends_on": {"nodes": ["model.p.m"]},
                         "test_metadata": {"name": "expression_is_true", "namespace": "dbt_utils",
                                           "kwargs": {"expression": "release_date > rc1_date", "model": "ref('m')"}}}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.RELEASES"]["columns"]
    assert min(columns["RELEASE_DATE"]["seconds"]) > max(columns["RC1_DATE"]["seconds"])
    assert columns["OTHER_DATE"]["seconds"] == columns["RC1_DATE"]["seconds"]


def test_times_that_bound_a_period_rows_are_looked_up_in_follow_one_another():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.releases": {"database": "RAW", "schema": "S", "name": "releases", "identifier": "releases", "columns": {}},
                    "source.p.s.issues": {"database": "RAW", "schema": "S", "name": "issues", "identifier": "issues", "columns": {}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            with rv as (select version, release_date::date as planned, dateadd(month, -1, release_date::date) as window_start,
                               announced_at::timestamp as announced_at from raw.s.releases)
            select i.id, rt.version from raw.s.issues i
            left join rv rt on i.created_at >= rt.window_start and i.created_at <= rt.planned"""}},
    }
    tables = build_spec(manifest, "A")[0]["tables"]
    assert tables["RAW.S.RELEASES"]["columns"]["RELEASE_DATE"]["every_days"] == 60 and tables["RAW.S.RELEASES"]["columns"]["RELEASE_DATE"]["unique"]
    assert "every_days" not in tables["RAW.S.RELEASES"]["columns"]["ANNOUNCED_AT"]
    assert "every_days" not in tables["RAW.S.ISSUES"]["columns"]["CREATED_AT"]


def test_columns_joined_together_to_a_seed_take_the_rows_it_lists(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.init import build_spec

    (tmp_path / "plan.csv").write_text("event_name,event_action,owner\nincident,create,a\nincident,end,a\ntasks,add_task,b\n")
    manifest = {
        "sources": {"source.p.s.tracks": {"database": "RAW", "schema": "S", "name": "tracks", "identifier": "tracks", "columns": {}}},
        "nodes": {
            "seed.p.plan": {"resource_type": "seed", "relation_name": "A.B.PLAN", "root_path": str(tmp_path), "original_file_path": "plan.csv",
                            "depends_on": {"nodes": []}},
            "model.p.stg": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.STG", "compiled_code": """
                select id as event_id, event as event_name, action as event_action, ts::timestamp as at from raw.s.tracks"""},
            "test.p.t": {"resource_type": "test", "depends_on": {"nodes": ["model.p.stg", "seed.p.plan"]}, "compiled_code": """
                with seen as (select event_name, event_action, max(at) from a.b.stg group by 1, 2)
                select pe.event_name from a.b.plan pe left join seen t
                  on t.event_name = pe.event_name and t.event_action = pe.event_action
                where t.event_name is null"""}},
    }
    spec, _ = build_spec(manifest, "A")
    columns = spec["tables"]["RAW.S.TRACKS"]["columns"]
    assert columns["EVENT"] == {"gen": "row_of", "group": "A.B.PLAN", "values": ["incident", "incident", "tasks"]}
    assert columns["ACTION"]["values"] == ["create", "end", "add_task"]
    from fauxflake.spec import Spec

    with serve() as emu:
        Warehouse(emu, Spec.from_raw({**spec, "start": "2025-01-01"}, "."), 0.01).advance_to(datetime(2025, 1, 3))
        pairs = set(emu.cursor().execute("select EVENT, ACTION from RAW.S.TRACKS").fetchall())
    assert pairs == {("incident", "create"), ("incident", "end"), ("tasks", "add_task")}


def test_a_table_matched_on_an_entity_and_its_hour_is_a_series():
    from fauxflake.init import build_spec

    def source(name):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name, "columns": {}}

    manifest = {
        "sources": {"source.p.s.metering": source("metering"), "source.p.s.queries": source("queries")},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            with hourly as (select date_trunc('hour', start_time) as hour, entity_id as warehouse_id, sum(credits) as credits
                            from raw.s.metering group by 1, 2),
            q as (select query_id, warehouse_id, date_trunc('hour', started_at) as hour from raw.s.queries)
            select q.query_id, hourly.credits from q
            join hourly on q.warehouse_id = hourly.warehouse_id and q.hour = hourly.hour"""}},
    }
    tables = build_spec(manifest, "A")[0]["tables"]
    metering = tables["RAW.S.METERING"]
    assert metering["columns"]["ENTITY_ID"] == {"gen": "pool", "pool": "entity_id", "size": 4, "numeric": True}
    assert tables["RAW.S.QUERIES"]["columns"]["WAREHOUSE_ID"] == metering["columns"]["ENTITY_ID"]


def test_a_key_that_reports_a_failure_is_there_in_few_documents():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.receipts": {"database": "RAW", "schema": "S", "name": "receipts", "identifier": "receipts", "columns": {}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select v:id::string as id, iff(v:outcome.status.Failure is not null, 'Fail', 'Success') as status,
                   iff(v:outcome.proof is not null, 1, 0) as proven from raw.s.receipts"""}},
    }
    fields = build_spec(manifest, "A")[0]["tables"]["RAW.S.RECEIPTS"]["columns"]["V"]["fields"]
    assert fields["outcome.status.Failure"]["optional"] == 0.95 and fields["outcome.proof"]["optional"] == 0.5


def test_table_flatten_and_a_mark_stripped_before_json_is_parsed(tmp_path):
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.init import build_spec
    from fauxflake.spec import Spec

    manifest = {
        "sources": {"source.p.s.r": {"database": "RAW", "schema": "S", "name": "r", "identifier": "r", "columns": {}}},
        "nodes": {
            "model.p.a": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.A",
                          "compiled_code": "select r:id::string as id, r:outcome.logs as logs from raw.s.r"},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["model.p.a"]}, "relation_name": "A.B.M", "compiled_code": """
                select id, replace(value, 'EVENT_JSON:') as json, try_parse_json(json):event::string as event
                from a.b.a, table(flatten(input => logs)) where event is not null"""}},
    }
    spec, _ = build_spec(manifest, "A")
    fields = spec["tables"]["RAW.S.R"]["columns"]["R"]["fields"]
    assert "outcome.logs[]~json.event" in fields and fields["outcome.logs[]~prefix"] == {"gen": "constant", "value": "EVENT_JSON:"}
    with serve() as emu:
        Warehouse(emu, Spec.from_raw({**spec, "start": "2025-01-01"}, "."), 0.01).advance_to(datetime(2025, 1, 3))
        logs = [row[0] for row in emu.cursor().execute("select R->'outcome'->'logs'->>0 from RAW.S.R limit 50").fetchall()]
    assert logs and all(text.startswith('EVENT_JSON:{"event":') for text in logs)


def test_a_model_read_by_a_written_out_name_is_known_whatever_the_order():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.lead": {"database": "RAW", "schema": "S", "name": "lead", "identifier": "lead", "columns": {}}},
        "nodes": {  # the reader comes first and names no dependency
            "model.p.a_reader": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.READER", "compiled_code": """
                select lead.sfid, lead.company from a.orgm.lead where lead.status = 'MQL' and not lead.isdeleted"""},
            "model.p.lead": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.ORGM.LEAD", "compiled_code": """
                select distinct o.id as sfid, o.* from raw.s.lead o
                join (select id, max(seq) as seq from raw.s.lead group by id) oo on o.id = oo.id and o.seq = oo.seq"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.LEAD"]["columns"]
    assert {"COMPANY", "STATUS", "ISDELETED", "ID", "SEQ"} <= set(columns) and "SFID" not in columns
    assert "MQL" in columns["STATUS"]["values"]


def test_pieces_of_a_text_inside_a_document_that_rows_are_kept_by():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.r": {"database": "RAW", "schema": "S", "name": "r", "identifier": "r", "columns": {}}},
        "nodes": {
            "model.p.ev": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.EV", "compiled_code": """
                select r:id::string as id, value as log from raw.s.r, lateral flatten(r:outcome.logs)"""},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["model.p.ev"]}, "relation_name": "A.B.M", "compiled_code": """
                select id, log, split(log, ' ') as log_parts, split(log_parts[0]::string, '@')[1]::string as signer,
                       log_parts[1]::string as action, split(log_parts[2]::string, '.')[0]::number as amount_raw
                from a.b.ev where log like '@%' and log_parts[1]::string in ('staking', 'unstaking')"""}},
    }
    logs = build_spec(manifest, "A")[0]["tables"]["RAW.S.R"]["columns"]["R"]["fields"]["outcome.logs[]"]
    assert logs["gen"] == "template"
    assert logs["templates"][:2] == ["@{hex} staking {n}", "@{hex} unstaking {n}"] and "@{hex}" in logs["templates"]


def test_the_range_the_table_s_own_reader_keeps_is_the_range_generated():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.shards": {"database": "RAW", "schema": "S", "name": "shards", "identifier": "shards", "columns": {}}},
        "nodes": {
            "model.p.load": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.LOAD", "compiled_code": """
                select block_number, value from raw.s.shards where block_number between 9820000 and 10000000"""},
            "model.p.social": {"resource_type": "model", "depends_on": {"nodes": ["model.p.load"]}, "relation_name": "A.B.SOCIAL", "compiled_code": """
                select * from a.b.load where block_number >= 75000000"""}},
    }
    number = build_spec(manifest, "A")[0]["tables"]["RAW.S.SHARDS"]["columns"]["BLOCK_NUMBER"]
    assert number["v"] == [9820000, 10000000] and number["p"] == [0, 1]


def test_a_cte_that_joins_and_selects_one_sources_star_passes_that_source_through():
    uses = {"RAW.B.SUBS": SourceUse(), "RAW.B.PRODUCTS": SourceUse()}
    cte = """with subscriptions as (
               select s.*, p.sku, row_number() over (partition by s.id order by s.version desc) as row_num
               from raw.b.subs s join raw.b.products p on s.product_id = p.id)"""
    whole = scan_model(cte + " select * from subscriptions where row_num = 1", uses, {}, [], [])
    assert whole["SKU"] == ("RAW.B.PRODUCTS", "SKU")
    assert whole["*"] == ("RAW.B.SUBS", "ROW_NUM")
    named = scan_model(cte + " select x.customer_id, x.sku, x.row_num from subscriptions x", uses, {}, [], [])
    assert named == {"CUSTOMER_ID": ("RAW.B.SUBS", "CUSTOMER_ID"), "SKU": ("RAW.B.PRODUCTS", "SKU")}


def test_a_calendar_joined_to_a_rows_start_and_end_does_not_make_them_periods():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.items": {"database": "RAW", "schema": "S", "name": "items", "identifier": "items", "columns": {}},
                    "source.p.s.dates": {"database": "RAW", "schema": "S", "name": "dates", "identifier": "dates", "columns": {}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select i.id, d.date as day, i.amount / 365 as daily from raw.s.items i
            left join raw.s.dates d on d.date::date >= i.start_date::date and d.date::date <= i.end_date::date"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.ITEMS"]["columns"]
    assert "every_days" not in columns["START_DATE"] and "every_days" not in columns["END_DATE"]


def test_a_declared_source_that_a_model_builds_is_read_as_that_model():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.raw.lead": {"database": "RAW", "schema": "S", "name": "lead", "identifier": "lead", "columns": {}},
                    "source.p.orgm.lead": {"database": "A", "schema": "ORGM", "name": "lead", "identifier": "lead", "columns": {}}},
        "nodes": {
            "model.p.lead": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.ORGM.LEAD",
                             "compiled_code": "select * from raw.s.lead"},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["model.p.lead"]}, "relation_name": "A.B.M",
                          "compiled_code": "select l.id from a.orgm.lead l where l.activated_on is null"}},
    }
    tables = build_spec(manifest, "A")[0]["tables"]
    assert "A.ORGM.LEAD" not in tables and tables["RAW.S.LEAD"]["columns"]["ACTIVATED_ON"]["null_rate"] == 0.3


def test_union_branches_listing_the_same_columns_in_another_order_are_not_lined_up_by_position():
    uses = {"RAW.S.OLD_EVENTS": SourceUse(), "RAW.S.NEW_EVENTS": SourceUse()}
    links: list = []
    scan_model('''select "ID", "SENT_AT", "IS_FIRST" from raw.s.old_events
                  union all select "IS_FIRST", "ID", "SENT_AT" from raw.s.new_events''', uses, {}, links=links)
    assert links == []
    scan_model("select id, sent_at from raw.s.old_events union all select event_id, sent from raw.s.new_events", uses, {}, links=links)
    assert {("RAW.S.OLD_EVENTS", "ID"), ("RAW.S.NEW_EVENTS", "EVENT_ID")} in [set(g) for g in links]


def test_two_thresholds_leave_values_beyond_the_larger_one():
    from fauxflake.init import within_bounds

    gen = {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    within_bounds(gen, {0, 1000000})  # `bytes > 0` in one model, `bytes > 1000000` in another
    assert gen["v"] == [0, 1000000, 1500000] and gen["p"] == [0.0, 0.5, 1.0]
    kept = {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    within_bounds(kept, {40, 60}, exact=True)  # the one range the table's reader keeps
    assert kept["v"] == [40, 60]


def test_a_column_joined_to_a_seed_holds_its_values_and_a_few_rows_outside_them(tmp_path):
    from fauxflake.init import build_spec

    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "team.csv").write_text("user_id,start_date\n22132,2020-01-01\n87854,2021-06-01\n")
    manifest = {
        "sources": {"source.p.s.issues": {"database": "RAW", "schema": "S", "name": "issues", "identifier": "issues", "columns": {}}},
        "nodes": {
            "seed.p.team": {"resource_type": "seed", "relation_name": "A.SEED.TEAM", "root_path": str(tmp_path),
                            "original_file_path": "seeds/team.csv", "depends_on": {"nodes": []}},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["seed.p.team"]}, "relation_name": "A.B.M", "compiled_code": """
                select i.id, (t.user_id is not null) as is_team from raw.s.issues i left join a.seed.team t on i.author_id = t.user_id"""}},
    }
    values = build_spec(manifest, "A")[0]["tables"]["RAW.S.ISSUES"]["columns"]["AUTHOR_ID"]["values"]
    assert set(values) == {"22132", "87854", "95773"} and values["95773"] == 0.15


def test_a_column_joined_to_a_seed_that_nobody_asks_null_of_holds_only_the_seeds_values(tmp_path):
    from fauxflake.init import build_spec

    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "tokens.csv").write_text("token_id,symbol\n22132,NEAR\n87854,USN\n")
    manifest = {
        "sources": {"source.p.s.prices": {"database": "RAW", "schema": "S", "name": "prices", "identifier": "prices", "columns": {}}},
        "nodes": {
            "seed.p.tokens": {"resource_type": "seed", "relation_name": "A.SEED.TOKENS", "root_path": str(tmp_path),
                              "original_file_path": "seeds/tokens.csv", "depends_on": {"nodes": []}},
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["seed.p.tokens"]}, "relation_name": "A.B.M", "compiled_code": """
                select p.price, t.symbol from raw.s.prices p left join a.seed.tokens t on p.token_id = t.token_id"""}},
    }
    values = build_spec(manifest, "A")[0]["tables"]["RAW.S.PRICES"]["columns"]["TOKEN_ID"]["values"]
    assert set(values) == {"22132", "87854"}


def test_a_flattened_place_that_also_has_named_keys_is_an_object():
    from fauxflake.init import flattened_objects

    paths = {"q.body.data.result[]": None, "q.body.data.result[].value[]": "DOUBLE", "q.message": "TEXT", "q.success": "BOOLEAN",
             "q[]": None, "q[].data.resultType": "TEXT", "q[].status": "TEXT", "tags[]": None, "tags[].name": "TEXT"}
    assert flattened_objects(paths) == {
        "q.body.data.result[]": None, "q.body.data.result[].value[]": "DOUBLE", "q.message": "TEXT", "q.success": "BOOLEAN",
        "q.body.data.resultType": "TEXT", "q.body.status": "TEXT", "tags[]": None, "tags[].name": "TEXT"}
    mixed = {"[].factor": "DOUBLE", "[].factor[]": None, "[].factor[].name": "TEXT", "[].name": "TEXT"}
    assert flattened_objects(mixed) == {"[].factor": "DOUBLE", "[].name": "TEXT"}


def test_a_snapshots_config_names_columns_of_the_source_it_selects_whole():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.n.lines": {"database": "RAW", "schema": "N", "name": "lines", "identifier": "lines", "columns": {}}},
        "nodes": {"snapshot.p.lines_snapshot": {
            "resource_type": "snapshot", "depends_on": {"nodes": []}, "relation_name": "A.SNAPSHOTS.LINES_SNAPSHOT",
            "config": {"strategy": "timestamp", "unique_key": "unique_key", "updated_at": "date_last_modified_gmt"},
            "compiled_code": "select * from raw.n.lines"}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.N.LINES"]["columns"]
    assert "UNIQUE_KEY" in columns and columns["DATE_LAST_MODIFIED_GMT"]["gen"] in ("time_offset", "updated_at", "event_time")


def test_the_columns_of_one_coalesce_share_what_is_known_of_one():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.l": {"database": "RAW", "schema": "S", "name": "leads", "identifier": "leads", "columns": {}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select id, employees::number as n, coalesce(employees, zoominfo_employees, cognism_employees) as employee_count,
                   coalesce(name, nickname) as called from raw.s.leads"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.LEADS"]["columns"]
    assert columns["COGNISM_EMPLOYEES"]["gen"] == columns["ZOOMINFO_EMPLOYEES"]["gen"] == "dist"
    assert columns["NICKNAME"]["gen"] == "template"


def test_builder_role_is_the_one_policies_let_through():
    from fauxflake.init import builder_role

    manifest = {"macros": {"macro.p.create_masking_policy": {"macro_sql": """
        CREATE MASKING POLICY IF NOT EXISTS p AS (val TEXT) RETURNS TEXT ->
        CASE WHEN CURRENT_ROLE() IN ('TRANSFORMER','LOADER') THEN val
             WHEN IS_ROLE_IN_SESSION('{{ policy }}') THEN val ELSE '***MASKED***' END"""},
        "macro.p.other": {"macro_sql": "select current_role() = 'NOBODY'"}}, "nodes": {}}
    assert builder_role(manifest) == "TRANSFORMER"
    assert builder_role({"macros": {}, "nodes": {}}) is None


def test_memory_is_bounded_below_duckdb_default():
    import duckdb

    from fauxflake import emulator

    default = duckdb.connect().execute("select current_setting('memory_limit')").fetchone()[0]
    with emulator.engine() as emu:
        bounded = emu.duck.execute("select current_setting('memory_limit')").fetchone()[0]
    assert bounded != default


def test_a_long_history_keeps_its_recent_end_and_thins_the_rest():
    from fauxflake.init import LONGEST, history_window, thinned
    from fauxflake.spec import Spec

    # one old upper limit no longer leaves out what is asked of the last years
    start, span = history_window([("before", date(2018, 1, 15)), ("after", date(2024, 6, 21)), ("after", date(2026, 11, 1))])
    assert start <= date(2017, 12, 1) and start + timedelta(days=span) > date(2026, 11, 1)
    start, span = history_window([("before", date(2015, 1, 15)), ("after", date(2034, 6, 21))])
    assert span == LONGEST and start + timedelta(days=span) > date(2034, 6, 21)
    assert thinned(1500) is None and 0 < thinned(3300) < 0.5
    spec = Spec.from_raw({"seed": 1, "start": "2018-01-01", "history": 3300, "recent": 365, "older": thinned(3300), "tables": {
        "RAW.T.EVENTS": {"volume": {"rows_per_day": 1000}, "columns": {"ID": {"gen": "sequence"}}}}}, "x.yml")
    cal = spec.tables["RAW.T.EVENTS"].calendar(spec.start, spec.start + timedelta(days=3300), 1, 0.001, None, spec.thin)
    cut = spec.thin[0]
    assert all(n >= 1 for day, _, n in cal if day >= cut)  # a row a day in the last year
    assert 1300 < sum(n for _, _, n in cal) < 1700  # as many rows as four years would hold
    assert sum(n for day, _, n in cal if day < date(2018, 3, 1)) >= 10  # and the first weeks are not empty


def test_numbers_asked_for_by_name_are_generated():
    from fauxflake.init import scan_model, SourceUse

    uses = {"RAW.T.LINES": SourceUse()}
    scan_model("select to_timestamp(sent_at::int, 6) as sent, v['at']::int as at_raw, to_timestamp(v['at']::int, 3) as at "
               "from RAW.T.LINES where calendar_id = 2 and form_id in (334447, 426148) and not other_id = 7", uses)
    use = uses["RAW.T.LINES"]
    assert use.equals == {"CALENDAR_ID": {2}, "FORM_ID": {334447, 426148}}
    assert use.scales[("SENT_AT", "")] == 6 and use.scales[("V", "at")] == 3


def test_a_document_is_followed_through_object_insert_and_delete():
    from fauxflake.init import _PATHS, scan_model, SourceUse

    uses = {"RAW.G.DETAIL": SourceUse()}
    out = scan_model("""
        with source as (
          select object_delete(parse_json(value), 'exported_at') as trimmed,
                 object_insert(trimmed, 'credits', array_sort(value['credits']::variant), true) as sorted_credits
          from RAW.G.DETAIL),
        grouped as (select sorted_credits as value, count(*) as n from source group by 1),
        renamed as (select value['account_id']::varchar as account_id, value['usage']['amount']::float as amount,
                           value['credits']::variant as credits from grouped)
        select * from renamed""", uses)
    assert {"account_id", "usage.amount", "credits[]"} <= set(uses["RAW.G.DETAIL"].paths["VALUE"])
    assert out[_PATHS]["CREDITS"] == ("RAW.G.DETAIL", "VALUE", ("credits",))


def test_text_ranges_and_named_values_beside_patterns():
    from fauxflake.init import scan_model, SourceUse

    uses = {"RAW.N.ACCOUNTS": SourceUse()}
    scan_model("select * from RAW.N.ACCOUNTS where account_number between '4000' and '4999' "
               "and (plan like 'saas%' or plan in ('1,000 CI Minutes')) and namespace_id = '4347861'", uses)
    use = uses["RAW.N.ACCOUNTS"]
    assert use.text_bounds == {"ACCOUNT_NUMBER": {4000, 4999}}
    assert use.equals == {"NAMESPACE_ID": {4347861}}
    assert use.literals["PLAN"] == {"1,000 CI Minutes"} and use.patterns["PLAN"]


def test_a_reference_to_a_column_that_is_not_the_key_holds_its_values():
    from fauxflake.init import build_spec

    def model(name, sql, **more):
        return {"resource_type": "model", "name": name, "unique_id": f"model.p.{name}", "compiled_code": sql, "database": "A", "schema": "S",
                "relation_name": f"A.S.{name}", "depends_on": {"nodes": more.pop("deps", [])}, "package_name": "p",
                "original_file_path": f"models/{name}.sql", "config": {"materialized": "table"}, **more}

    def source(name):
        return {"resource_type": "source", "name": name, "unique_id": f"source.p.raw.{name}", "database": "RAW", "schema": "S",
                "identifier": name, "relation_name": f"RAW.S.{name}", "source_name": "raw", "package_name": "p", "columns": {}}

    manifest = {"nodes": {
        "model.p.opps": model("opps", "select id, stagename as stage_name from RAW.S.OPPS", deps=["source.p.raw.OPPS"]),
        "model.p.stages": model("stages", "select id as stage_id, masterlabel as primary_label from RAW.S.STAGES "
                                          "where masterlabel in ('1-Discovery', '8-Closed Lost')", deps=["source.p.raw.STAGES"]),
        "test.p.rel": {"resource_type": "test", "name": "rel", "unique_id": "test.p.rel", "package_name": "p", "column_name": "stage_name",
                       "attached_node": "model.p.opps", "depends_on": {"nodes": ["model.p.stages", "model.p.opps"]},
                       "test_metadata": {"name": "relationships", "kwargs": {"column_name": "stage_name", "to": "ref('stages')",
                                                                              "field": "primary_label"}},
                       "refs": [{"name": "stages"}, {"name": "opps"}]}},
        "sources": {"source.p.raw.OPPS": source("OPPS"), "source.p.raw.STAGES": source("STAGES")}}
    spec, _notes = build_spec(manifest, "A")[:2]
    child = spec["tables"]["RAW.S.OPPS"]["columns"]["STAGENAME"]
    assert child["gen"] == "categorical" and {"1-Discovery", "8-Closed Lost"} <= set(child["values"])


def test_values_that_contain_something_also_begin_the_way_that_is_asked():
    from fauxflake.init import like_shapes

    assert like_shapes({"http%", "%utm_content=partner%"}) == ["http{hex}", "http{hex}utm_content=partner{hex}"]
    assert like_shapes({"%a%", "%.com"}) == ["{hex}.com", "{hex}a{hex}"]


def test_a_time_matched_with_a_calendar_day_is_a_whole_day():
    from fauxflake.init import scan_model, SourceUse

    uses = {"RAW.P.PINGS": SourceUse()}
    scan_model("with pings as (select ping_date::timestamp as ping_date, sent_at from RAW.P.PINGS), "
               "days as (select date_day from A.B.DIM_DATE) "
               "select * from pings inner join days on ping_date = date_day", uses, None, [], [])
    assert uses["RAW.P.PINGS"].whole_days == {"PING_DATE"}


def test_the_id_a_column_falls_back_to_is_recorded():
    from fauxflake.init import scan_model, SourceUse

    uses = {"RAW.G.NAMESPACES": SourceUse()}
    scan_model("select id, coalesce(plan_id, 34) as plan_id, coalesce(seats, 0) as seats from RAW.G.NAMESPACES", uses)
    assert uses["RAW.G.NAMESPACES"].fallbacks == {"PLAN_ID": {34}}


def test_a_table_listed_by_its_row_count_holds_that_many_rows():
    from datetime import date

    from fauxflake.init import rows_asked
    from fauxflake.spec import Table

    manifest = {"macros": {"macro.p.events": {"macro_sql": "{% macro events(pattern, min_rows=100) %} select table_name from "
                                                           "raw.information_schema.tables where row_count >= {{min_rows}} {% endmacro %}"}},
                "nodes": {}}
    assert rows_asked(manifest) == 100
    assert rows_asked({"macros": {}, "nodes": {"m": {"raw_code": "select 1 where row_count > 5"}}}) is None
    table = Table(name="A.B.C", raw={"volume": {"rows_per_day": 20}}, columns={})
    whole = table.calendar(date(2023, 2, 1), date(2024, 12, 15), 42, 0.001, None, None, (100, 683))
    assert sum(n for _, _, n in whole) == 100
    part = table.calendar(date(2023, 2, 1), date(2023, 6, 1), 42, 0.001, None, None, (100, 683))
    assert part == whole[:len(part)]  # the same days hold the same rows however far the clock has moved


def test_a_surrogate_key_is_traced_to_the_column_it_hashes():
    from fauxflake.init import _ANY, _HASHED, scan_model, SourceUse

    uses = {"RAW.T.SERVER": SourceUse(), "RAW.L.SERVER": SourceUse(), "RAW.N.FEEDBACK": SourceUse()}
    staged = scan_model("select id, split_part(version, '-', 1) as version_full from RAW.T.SERVER", uses)
    assert staged[_ANY + "VERSION_FULL"] == (("RAW.T.SERVER", "VERSION", "-"),)
    models = {"A.B.STG": staged, "A.B.LEGACY": scan_model("select id, version as version_full from RAW.L.SERVER", uses)}
    spined = scan_model("select coalesce(t.version_full, l.version_full, 'x' || t.id) as version_full "
                        "from A.B.STG t left join A.B.LEGACY l on t.id = l.id", uses, models)
    dim = scan_model("with versions as (select distinct version_full from A.B.SPINED) "
                     "select md5(cast(coalesce(cast(version_full as text), '_null_') as text)) as version_id, version_full from versions",
                     uses, {**models, "A.B.SPINED": spined})
    assert dim[_HASHED + "VERSION_ID"] == (("RAW.T.SERVER", "VERSION", "-"), ("RAW.L.SERVER", "VERSION"))
    fact = scan_model("select coalesce(f.server_version, f.old_version) as all_versions, "
                      "md5(cast(coalesce(cast(all_versions as text), '_null_') as text)) as version_id, "
                      "md5(cast(f.a as text) || '-' || cast(f.b as text)) as pair_id from RAW.N.FEEDBACK f", uses)
    assert fact[_HASHED + "VERSION_ID"] == (("RAW.N.FEEDBACK", "SERVER_VERSION"), ("RAW.N.FEEDBACK", "OLD_VERSION"))
    assert _HASHED + "PAIR_ID" not in fact  # a key of two columns: neither decides it


def test_a_text_whose_shapes_begin_with_a_category_follows_it():
    from fauxflake.generate import begins_with
    from fauxflake.init import follow_categories

    columns = {"CATEGORY": {"gen": "categorical", "values": {"cli elt": 0.3, "cli invoke": 0.3, "cli run": 0.2, "other": 0.2}},
               "KIND": {"gen": "categorical", "values": {"a": 0.5, "b": 0.5}},
               "COMMAND": {"gen": "template", "templates": ["cli elt{hex} --transform run{hex}", "cli invoke dbt{hex}",
                                                            "{hex} --environment={hex}", "command_{hex}"]}}
    follow_categories(columns)
    assert columns["COMMAND"] == {"gen": "template", "begins_with": "CATEGORY", "templates": [
        "cli elt{hex} --transform run{hex}", "cli invoke dbt{hex}", "cli run {hex} --environment={hex}"]}
    again = {k: dict(v) for k, v in columns.items()}
    follow_categories(columns)
    assert columns == again
    assert begins_with(columns["COMMAND"]["templates"] + ["exactly this"], list(columns["CATEGORY"]["values"])) == {
        "cli elt": ["cli elt{hex} --transform run{hex}", "exactly this"], "cli invoke": ["cli invoke dbt{hex}"],
        "cli run": ["cli run {hex} --environment={hex}"], "other": ["other {hex}"]}
    from datetime import datetime

    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse

    spec = Spec.from_raw({"seed": 1, "start": "2025-01-01", "tables": {"RAW.P.EVENTS": {
        "volume": {"rows_per_day": 2000, "weekly": [1] * 7}, "lateness": {"p": [0, 1], "seconds": [0, 1]},
        "columns": {"ID": {"gen": "sequence"}, **columns}}}}, ".")
    with serve() as emu:
        Warehouse(emu, spec, 1.0).advance_to(datetime(2025, 1, 3))
        total, begin, kinds = emu.cursor().execute(
            "select count(*), sum(case when starts_with(command, category) then 1 else 0 end), count(distinct category) "
            "from RAW.P.EVENTS").fetchone()
    assert total > 1000 and begin == total and kinds == 4


def test_a_column_named_as_an_email_holds_addresses():
    from fauxflake.init import MAILBOX, address_emails, guess, holds_email

    assert holds_email("CONTACTEMAIL") and holds_email("CUST_EMAIL__C") and holds_email("context_traits_email")
    assert not holds_email("HASOPTEDOUTOFEMAIL") and not holds_email("INACTIVE_SERVER_EMAILS_SENT")
    assert guess("TRIAL_EMAIL", "VARCHAR")["template"].endswith(MAILBOX)
    assert guess("HASOPTEDOUTOFEMAIL", None)["template"] == "hasoptedoutofemail_{hex}"
    tables = {
        "A": {"columns": {"EMAIL": {"gen": "template", "template": "email_{n}"},
                          "NAME": {"gen": "template", "template": "name_{hex}"}}},
        "B": {"columns": {"CONTACT": {"gen": "pool", "pool": "email", "size": 200},
                          "OWNER_EMAIL": {"gen": "template", "templates": ["a@b.cc", "owner_email_{hex}"]},
                          "N": {"gen": "pool", "pool": "n", "size": 5, "numeric": True}}},
    }
    assert sorted(address_emails(tables)) == [("A", "EMAIL"), ("B", "CONTACT"), ("B", "OWNER_EMAIL")]
    assert tables["A"]["columns"]["EMAIL"]["template"] == "email_{n}" + MAILBOX
    assert tables["B"]["columns"]["CONTACT"]["suffix"] == MAILBOX
    assert tables["B"]["columns"]["OWNER_EMAIL"]["templates"] == ["a@b.cc", "owner_email_{hex}" + MAILBOX]
    assert tables["A"]["columns"]["NAME"] == {"gen": "template", "template": "name_{hex}"}
    assert address_emails(tables) == []


def test_a_text_cast_handed_to_a_timestamp_parser_says_the_column_is_a_time():
    sources = {"RAW.T.LICENSE": SourceUse()}
    scan_model("""
        with source as (select * from raw.t.license)
        select try_to_timestamp_ntz(cast(_start as varchar)) as starts_at, cast(name as varchar) as name from source""", sources)
    assert sources["RAW.T.LICENSE"].columns["_START"] == "TIMESTAMP"
    assert sources["RAW.T.LICENSE"].columns["NAME"].upper() in ("VARCHAR", "TEXT")


def test_a_join_on_surrogate_keys_of_several_columns_links_what_they_are_made_of():
    key = "md5(cast(coalesce(cast(server_id as text), '_null_') || '-' || coalesce(cast(server_date as text), '_null_') as text))"
    uses, outputs, joins, *_ = _scan({
        "a.b.servers": f"select user_id as server_id, cast(timestamp as date) as server_date, {key} as daily_server_id "
                       "from raw.t.server",
        "a.b.activity": f"select user_id as server_id, cast(timestamp as date) as server_date, {key} as daily_server_id, "
                        "mau from raw.t.activity",
        "a.b.joined": "select s.server_id, a.mau from a.b.servers s left join a.b.activity a "
                      "on s.daily_server_id = a.daily_server_id",
    }, ["RAW.T.SERVER", "RAW.T.ACTIVITY"])
    assert joins == [(("RAW.T.SERVER", "USER_ID"), ("RAW.T.ACTIVITY", "USER_ID"))]  # the servers, not the days


def test_a_column_joined_to_the_domain_of_an_email_holds_the_domain_addresses_are_at():
    from fauxflake.init import MAILBOX

    uses, *_ = _scan({"a.b.link": "select l.id, a.id from raw.t.lead l left join raw.t.account a "
                                  "on a.domain = split_part(l.email, '@', 2) where a.id is not null"},
                     ["RAW.T.LEAD", "RAW.T.ACCOUNT"])
    assert "org3.fauxmail.net" in uses["RAW.T.ACCOUNT"].literals["DOMAIN"] and len(uses["RAW.T.ACCOUNT"].literals["DOMAIN"]) == 24


def test_a_rule_a_test_states_about_the_rows_that_have_a_value_is_kept_by_leaving_the_others_empty():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.releases": {
            "database": "RAW", "schema": "S", "name": "releases", "identifier": "releases",
            "columns": {c: {"data_type": t} for c, t in {"version": "VARCHAR", "release_date": "DATE",
                                                         "actual_release_date": "DATE", "rc1_date": "DATE"}.items()}}},
        "nodes": {
            "model.p.stg": {"resource_type": "model", "depends_on": {"nodes": ["source.p.s.releases"]}, "relation_name": "A.B.STG",
                            "compiled_code": """
                select version, try_to_decimal(split_part(version, '.', 3)) as version_patch,
                       release_date::date as planned_release_date,
                       dateadd(month, -1, planned_release_date) as release_start_date,
                       coalesce(actual_release_date::date, planned_release_date) as actual_release_date,
                       rc1_date::date as rc1_date
                from raw.s.releases"""},
            "test.p.rc1": {"resource_type": "test", "attached_node": "model.p.stg", "depends_on": {"nodes": ["model.p.stg"]},
                           "config": {"where": "rc1_date is not null"},
                           "test_metadata": {"name": "expression_is_true", "namespace": "dbt_utils",
                                             "kwargs": {"expression": "version_patch = '0'"}}},
            "test.p.start": {"resource_type": "test", "attached_node": "model.p.stg", "depends_on": {"nodes": ["model.p.stg"]},
                             "test_metadata": {"name": "expression_is_true", "namespace": "dbt_utils",
                                               "kwargs": {"expression": "release_start_date < actual_release_date"}}},
        },
    }
    spec, notes = build_spec(manifest, "A")
    columns = spec["tables"]["RAW.S.RELEASES"]["columns"]
    assert "{{VERSION}}" in columns["RC1_DATE"]["null_unless"] and "null_unless" not in columns["VERSION"]
    # the start is worked out from the planned date, the release is the actual date when there is one: it comes later
    assert min(columns["ACTUAL_RELEASE_DATE"]["seconds"]) > max(columns["RELEASE_DATE"]["seconds"])


def test_created_comes_before_updated_also_inside_a_document():
    from fauxflake.init import created_comes_first

    week = {"gen": "time_offset", "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
    fields = {"created": dict(week), "updated": dict(week), "resolutiondate": dict(week), "status.name": {"gen": "template", "template": "x"},
              "parent.created": dict(week), "duedate": dict(week)}
    created_comes_first(fields)
    assert min(fields["updated"]["seconds"]) > 604800 and min(fields["resolutiondate"]["seconds"]) > 604800
    assert fields["created"] == week and fields["parent.created"] == week and fields["duedate"] == week


def test_a_coalesce_in_one_branch_of_a_union_is_lined_up_with_the_other_branch():
    uses = {name: SourceUse() for name in ("RAW.A.SCORE", "RAW.B.SCORE")}
    links: list = []
    scan_model("""
        with a as (select id, server_version as version_full from raw.a.score),
             b as (select id, coalesce(serverversion, server_version) as version_full from raw.b.score)
        select id, version_full from a union select id, version_full from b""", uses, {}, links, [], [])
    assert {(m[0], m[1]) for group in links for m in group if any(x[1] == "SERVER_VERSION" for x in group)} == {
        ("RAW.A.SCORE", "SERVER_VERSION"), ("RAW.B.SCORE", "SERVERVERSION"), ("RAW.B.SCORE", "SERVER_VERSION")}


def test_rows_kept_as_the_latest_of_each_thing_each_day_are_readings():
    uses, *_ = _scan({"a.b.latest": """
        select user_id as server_id, cast(timestamp as date) as server_date, version
        from raw.t.server
        qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1"""}, ["RAW.T.SERVER"])
    assert uses["RAW.T.SERVER"].readings == {("USER_ID", "TIMESTAMP")}


def test_a_row_that_names_another_row_of_its_own_table_refers_to_it():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.opportunity": {
            "database": "RAW", "schema": "S", "name": "opportunity", "identifier": "opportunity",
            "columns": {c: {"data_type": t} for c, t in {"id": "VARCHAR", "renewed_by_opportunity_id__c": "NUMBER",
                                                         "amount": "NUMBER"}.items()}}},
        "nodes": {
            "model.p.m": {"resource_type": "model", "depends_on": {"nodes": ["source.p.s.opportunity"]}, "relation_name": "A.B.M",
                          "compiled_code": """
                with opportunity as (select distinct o.id as sfid, o.* from raw.s.opportunity o)
                select a.sfid, sum(b.amount) as renewed
                from opportunity a left join opportunity b on a.sfid = b.renewed_by_opportunity_id__c
                group by 1"""},
            "test.p.u": {"resource_type": "test", "attached_node": "model.p.m", "depends_on": {"nodes": ["model.p.m"]},
                         "column_name": "sfid", "test_metadata": {"name": "unique", "kwargs": {"column_name": "sfid"}}},
        },
    }
    spec, _notes = build_spec(manifest, "A")
    columns = spec["tables"]["RAW.S.OPPORTUNITY"]["columns"]
    assert columns["ID"]["gen"] in ("hash_id", "sequence")
    assert columns["RENEWED_BY_OPPORTUNITY_ID__C"]["gen"] == "fk" and columns["RENEWED_BY_OPPORTUNITY_ID__C"]["ref"] == "RAW.S.OPPORTUNITY"


def test_a_case_and_its_complement_say_when_a_list_has_something():
    from fauxflake.init import build_spec

    def source(name):
        return {"database": "RAW", "schema": "CALLS", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in {"id": "VARCHAR", "user_id": "VARCHAR", "context_feature_name": "VARCHAR",
                                                             "context_feature_skus": "ARRAY"}.items()}}

    manifest = {
        "sources": {"source.p.c.started": source("started"), "source.p.c.ended": source("ended")},
        "nodes": {
            "model.p.stg": {"resource_type": "model", "depends_on": {"nodes": ["source.p.c.started"]}, "relation_name": "A.B.STG",
                            "compiled_code": """
                select id as event_id, user_id, coalesce(context_feature_name, 'Calls') as feature_name,
                       parse_json(coalesce(context_feature_skus, '[]'))::array as feature_skus
                from raw.calls.started"""},
            "model.p.daily": {"resource_type": "model", "depends_on": {"nodes": ["model.p.stg"]}, "relation_name": "A.B.DAILY",
                              "compiled_code": """
                select user_id,
                       count(distinct case when feature_name = 'Calls' and array_size(feature_skus) > 0 then event_id end) as known,
                       count(distinct case when array_size(feature_skus) = 0 then event_id end) as unknown,
                       count(distinct event_id) as total
                from a.b.stg group by 1"""},
        },
    }
    spec, _notes = build_spec(manifest, "A")
    for table in ("RAW.CALLS.STARTED", "RAW.CALLS.ENDED"):  # (the other table of the schema has the same columns)
        skus = spec["tables"][table]["columns"]["CONTEXT_FEATURE_SKUS"]
        assert skus["gen"] == "array" and "{{CONTEXT_FEATURE_NAME}}" in skus["empty_unless"] and not skus.get("null_rate")


def test_joined_columns_keep_the_values_asked_for_by_name_beside_a_pattern():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    manifest = {
        "sources": {"source.p.s.old": source("old", {"category": "VARCHAR", "n": "NUMBER"}),
                    "source.p.s.new": source("new", {"kind": "VARCHAR", "n": "NUMBER"})},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select o.n from raw.s.old o join raw.s.new w on o.category = w.kind
            where o.category = 'cli elt' or o.category like 'cli add%'"""}},
    }
    columns = {t: d["columns"] for t, d in build_spec(manifest, "A")[0]["tables"].items()}
    for held in (columns["RAW.S.OLD"]["CATEGORY"], columns["RAW.S.NEW"]["KIND"]):
        assert held["gen"] == "template" and "cli elt" in held["templates"], held


def test_what_is_read_in_the_values_of_a_flattened_object_stays_when_no_key_is_read_deeper():
    from fauxflake.init import flattened_objects

    paths = {"actions[]": None, "actions[].Transfer": None, "actions[].FunctionCall": None, "actions[][]": None, "actions[][].method_name": "TEXT"}
    # one key for each value, which is one kind of thing or another: the generator writes the object that way
    assert flattened_objects(paths) == {k: v for k, v in paths.items() if k != "actions[][]"}
    # a key read deeper by name holds an object: what is read in the values is read in it
    moved = {}
    deeper = flattened_objects({**paths, "actions[].FunctionCall.gas": "DOUBLE"}, moved)
    assert deeper["actions[].FunctionCall.method_name"] == "TEXT" and "actions[][].method_name" not in deeper
    assert moved["actions[][].method_name"] == ["actions[].FunctionCall.method_name"]


def test_columns_joined_to_a_category_bring_the_values_they_are_asked_for_by_name():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    manifest = {
        "sources": {"source.p.s.rates": source("rates", {"usage": "VARCHAR", "service": "VARCHAR"})},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select a.usage from raw.s.rates a join raw.s.rates b on a.usage = b.service
            where b.service in ('STORAGE', 'COMPUTE') and (a.usage = 'compute' or a.usage like '%adj%')"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.RATES"]["columns"]
    assert "compute" in columns["USAGE"]["values"] and "STORAGE" in columns["USAGE"]["values"], columns


def test_the_latest_row_of_each_hostname_looked_up_by_installation_makes_a_hostname_of_one_installation():
    sources = {"RAW.S.SUBS": SourceUse(), "RAW.S.SERVERS": SourceUse()}
    scan_model("""
        with latest as (
            select s.meta:"inst"::varchar as installation_id, s.meta:"dns"::varchar as hostname
            from raw.s.subs s
            qualify row_number() over (partition by s.meta:"dns"::varchar order by s.created desc) = 1)
        select v.id, l.hostname from raw.s.servers v left join latest l on v.installation_id = l.installation_id""",
               sources, joins=[])
    assert sources["RAW.S.SUBS"].one_per == {(("META", "dns"), ("META", "inst"))}


def test_an_id_the_models_measure_is_that_long_wherever_it_is_matched():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    manifest = {
        "sources": {"source.p.s.servers": source("servers", {"id": "VARCHAR", "name": "VARCHAR"}),
                    "source.p.s.pings": source("pings", {"server_id": "VARCHAR", "at": "TIMESTAMP"})},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            select s.id, p.at from raw.s.servers s join raw.s.pings p on p.server_id = s.id where length(p.server_id) = 26"""}},
    }
    columns = {t: d["columns"] for t, d in build_spec(manifest, "A")[0]["tables"].items()}
    assert columns["RAW.S.SERVERS"]["ID"].get("length") == 26 and columns["RAW.S.PINGS"]["SERVER_ID"].get("length") == 26, columns


def test_the_reading_still_says_where_a_column_comes_from_when_a_flattened_object_was_moved():
    from fauxflake.init import analyse

    manifest = {
        "sources": {"source.p.s.shards": {"database": "RAW", "schema": "S", "name": "shards", "identifier": "shards",
                                          "columns": {"value": {"data_type": "VARIANT"}, "id": {"data_type": "NUMBER"}}}},
        "nodes": {"model.p.m": {"resource_type": "model", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "name": "m", "compiled_code": """
            with a as (select s.id, f.value as action, s.value:actions[0]:Transfer as t, s.value:actions[0]:FunctionCall as c
                       from raw.s.shards s, lateral flatten(input => s.value:actions) f),
                 b as (select a.id, g.key as name, g.value:method_name::string as method_name from a, lateral flatten(input => a.action) g)
            select * from b where method_name = 'stake'"""}},
    }
    analysis = analyse(manifest, "A")
    fields = analysis.spec["tables"]["RAW.S.SHARDS"]["columns"]["VALUE"]["fields"]
    assert "stake" in str(fields.get("actions[][].method_name")), fields
    assert analysis.place("model.p.m", "ID") == ("RAW.S.SHARDS", "ID", "")


def test_two_texts_that_begin_with_each_others_values_do_not_follow_each_other():
    from fauxflake.init import follow_categories

    columns = {"A": {"gen": "template", "templates": ["run", "run fast {hex}", "a_{hex}"]},
               "B": {"gen": "template", "templates": ["run", "run fast", "run slow {hex}", "b_{hex}"]}}
    follow_categories(columns)
    assert [c for c, d in columns.items() if d.get("begins_with")] in (["A"], ["B"], [])


def test_a_test_that_returns_things_with_two_facts_at_odds_deals_the_facts_to_two_halves():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    manifest = {
        "sources": {"source.p.s.servers": source("servers", {"id": "VARCHAR", "installation": "VARCHAR", "at": "TIMESTAMP"}),
                    "source.p.s.licenses": source("licenses", {"server_id": "VARCHAR", "license_id": "VARCHAR", "company": "VARCHAR"}),
                    "source.p.s.clouds": source("clouds", {"installation": "VARCHAR", "company": "VARCHAR"})},
        "nodes": {
            "model.p.info": {"resource_type": "model", "name": "info", "depends_on": {"nodes": []}, "relation_name": "A.B.INFO", "compiled_code": """
                with every as (select coalesce(l.server_id, s.id) as server_id, l.license_id, s.installation as installation_id, l.company
                               from raw.s.licenses l full outer join raw.s.servers s on l.server_id = s.id)
                select e.server_id, e.license_id, e.installation_id, e.company as license_company, c.company as cloud_company
                from every e left join raw.s.clouds c on c.installation = e.installation_id"""},
            "test.p.one_source": {"resource_type": "test", "name": "one_source", "depends_on": {"nodes": ["model.p.info"]}, "compiled_code": """
                select server_id, license_id, installation_id from a.b.info
                where (license_company is not null and cloud_company is not null and license_company <> cloud_company)"""}},
    }
    columns = {t: d["columns"] for t, d in build_spec(manifest, "A")[0]["tables"].items()}
    rules = {columns["RAW.S.LICENSES"]["LICENSE_ID"].get("null_unless"), columns["RAW.S.SERVERS"]["INSTALLATION"].get("null_unless")}
    assert rules == {"HASH(({{SERVER_ID}})::VARCHAR) % 2 = 0", "HASH(({{ID}})::VARCHAR) % 2 = 1"}, columns


def test_tests_that_state_the_shape_of_an_id_and_the_order_of_two_times_are_read():
    from fauxflake.init import build_spec

    def source(name, cols):
        return {"database": "RAW", "schema": "S", "name": name, "identifier": name,
                "columns": {c: {"data_type": t} for c, t in cols.items()}}

    def test(name, kwargs, column=None):
        return {"resource_type": "test", "name": name, "attached_node": "model.p.orders", "column_name": column,
                "depends_on": {"nodes": ["model.p.orders"]}, "test_metadata": {"name": "expression_is_true", "kwargs": kwargs}}

    manifest = {
        "sources": {"source.p.s.orders": source("orders", {"id": "NUMBER", "valid_from": "TIMESTAMP", "valid_to": "TIMESTAMP"}),
                    "source.p.s.lines": source("lines", {"line": "NUMBER", "order_id": "NUMBER"})},
        "nodes": {
            "model.p.orders": {"resource_type": "model", "name": "orders", "depends_on": {"nodes": []}, "relation_name": "A.B.ORDERS",
                               "compiled_code": "select o.id, o.valid_from, o.valid_to, l.line from raw.s.orders o join raw.s.lines l on l.order_id = o.id"},
            "test.p.shape": test("shape", {"expression": "REGEXP '[0-9a-z]{32}'", "column_name": "id"}, "id"),
            "test.p.order": test("order", {"expression": ">= valid_from", "column_name": "valid_to"}, "valid_to")},
    }
    columns = {t: d["columns"] for t, d in build_spec(manifest, "A")[0]["tables"].items()}
    assert columns["RAW.S.ORDERS"]["ID"] == {"gen": "hash_id", "length": 32}
    assert columns["RAW.S.LINES"]["ORDER_ID"]["gen"] == "fk" and columns["RAW.S.LINES"]["ORDER_ID"]["length"] == 32
    assert min(columns["RAW.S.ORDERS"]["VALID_TO"]["seconds"]) > max(columns["RAW.S.ORDERS"]["VALID_FROM"].get("seconds", [0]))


def test_a_list_holds_what_the_models_ask_whether_it_contains():
    sources = {"RAW.S.MRS": SourceUse()}
    scan_model("""select id from raw.s.mrs where array_contains('database::approved'::variant, labels)
                  and contains(title, 'All') and not array_contains('wip'::variant, labels)""", sources)
    assert sources["RAW.S.MRS"].array_values == {"LABELS": {"database::approved"}} and "LABELS" in sources["RAW.S.MRS"].arrays
    assert sources["RAW.S.MRS"].patterns == {"TITLE": {"%All%"}}


def test_no_helper_of_the_reading_shares_its_name_with_a_variable():
    """`scan_model` and `analyse` define helpers inside themselves; a variable of the same name replaces the helper
    for everything after it, and the reading goes on with whatever that variable holds."""
    import ast
    from pathlib import Path

    import fauxflake.init as init

    tree = ast.parse(Path(init.__file__).read_text())
    for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name in ("scan_model", "analyse")):
        helpers, assigned = set(), set()

        class Names(ast.NodeVisitor):
            depth = 0

            def visit_FunctionDef(self, node):  # noqa: N802
                if self.depth == 1:
                    helpers.add(node.name)
                    return
                self.depth += 1
                self.generic_visit(node)
                self.depth -= 1

            def visit_Lambda(self, node):  # noqa: N802
                return

            def visit_Name(self, node):  # noqa: N802
                if isinstance(node.ctx, ast.Store):
                    assigned.add(node.id)

        Names().visit(fn)
        assert not helpers & assigned, (fn.name, sorted(helpers & assigned))


def test_what_is_read_no_further_back_than_a_mark_lies_within_that_of_the_top_of_the_range_loaded():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.shards": {"database": "RAW", "schema": "S", "name": "shards", "identifier": "shards",
                                          "columns": {"block": {"data_type": "NUMBER"}, "v": {"data_type": "VARCHAR"}}}},
        "nodes": {
            "model.p.load": {"resource_type": "model", "name": "load", "depends_on": {"nodes": []}, "relation_name": "A.B.LOAD",
                             "compiled_code": "select block, v from raw.s.shards where block between 9820000 and 10000000"},
            "model.p.map": {"resource_type": "model", "name": "map", "depends_on": {"nodes": ["model.p.load"]}, "relation_name": "A.B.MAP",
                            "compiled_code": "select * from a.b.load where block >= (select max(block) from a.b.done) - 20000"},
            "model.p.late": {"resource_type": "model", "name": "late", "depends_on": {"nodes": ["model.p.load"]}, "relation_name": "A.B.LATE",
                             "compiled_code": "select * from a.b.load where block >= 75000000"}},
    }
    held = build_spec(manifest, "A")[0]["tables"]["RAW.S.SHARDS"]["columns"]["BLOCK"]
    assert held["v"] == [9980000, 10000000] and held["p"] == [0, 1], held


def test_a_text_of_numbers_in_one_union_branch_is_the_same_kind_of_text_in_the_others():
    from fauxflake.init import _share_union_values

    tables = {"RAW.V.USAGE": {"columns": {"VERSION": {"gen": "template", "templates": ["{n}.{n}", "{n}.{n}-pre"]}}},
              "RAW.V.PINGS": {"columns": {"RESULTS": {"gen": "object", "fields": {"version": {"gen": "template", "template": "version_{hex}"}}}}}}
    _share_union_values(tables, [{("RAW.V.USAGE", "VERSION"), ("RAW.V.PINGS", "RESULTS", "version")}])
    assert tables["RAW.V.PINGS"]["columns"]["RESULTS"]["fields"]["version"] == {"gen": "template", "templates": ["{n}.{n}", "{n}.{n}-pre"]}


def test_the_value_of_a_history_of_changes_goes_by_the_field_it_is_about():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.history": {"database": "RAW", "schema": "S", "name": "history", "identifier": "history", "columns": {
            c: {"data_type": "VARCHAR"} for c in ("opportunityid", "field", "oldvalue")}}},
        "nodes": {"model.p.m": {"resource_type": "model", "name": "m", "depends_on": {"nodes": []}, "relation_name": "A.B.M", "compiled_code": """
            with base as (select opportunityid as id, field as opportunity_field, oldvalue as old_value from raw.s.history
                          where field in ('amount', 'closedate', 'stagename')),
                 turned as (select * from base pivot(max(old_value) for opportunity_field in ('amount', 'closedate', 'stagename')))
            select id, closedate::date as close_date, amount::float as amount, stagename from turned"""}},
    }
    held = build_spec(manifest, "A")[0]["tables"]["RAW.S.HISTORY"]["columns"]["OLDVALUE"]
    assert held["gen"] == "by" and held["of"] == "FIELD" and set(held["cases"]) == {"amount", "closedate"}, held
    assert held["cases"]["closedate"]["format"] == "%Y-%m-%d" and held["cases"]["amount"]["gen"] == "dist"


def test_a_table_a_snapshot_takes_one_row_of_for_each_key_is_not_a_table_of_readings():
    from fauxflake.init import build_spec

    def manifest(snapshot: bool) -> dict:
        nodes = {"model.p.daily": {"resource_type": "model", "name": "daily", "depends_on": {"nodes": []}, "relation_name": "A.B.DAILY", "compiled_code": """
            select id, modified::date as day, name from raw.s.users
            qualify row_number() over (partition by id, modified::date order by modified desc) = 1"""}}
        if snapshot:
            nodes["snapshot.p.users_snapshot"] = {"resource_type": "snapshot", "name": "users_snapshot", "depends_on": {"nodes": []},
                                                  "relation_name": "A.S.USERS_SNAPSHOT", "config": {"unique_key": "id", "updated_at": "modified"},
                                                  "compiled_code": "select * from raw.s.users"}
        return {"sources": {"source.p.s.users": {"database": "RAW", "schema": "S", "name": "users", "identifier": "users", "columns": {
            "id": {"data_type": "VARCHAR"}, "modified": {"data_type": "TIMESTAMP"}, "name": {"data_type": "VARCHAR"}}}}, "nodes": nodes}

    assert build_spec(manifest(False), "A")[0]["tables"]["RAW.S.USERS"].get("grain")  # readings, as far as the models say
    table = build_spec(manifest(True), "A")[0]["tables"]["RAW.S.USERS"]
    assert not table.get("grain") and not table["columns"]["ID"].get("of")


def test_a_column_a_try_function_reads_holds_text():
    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.events": {"database": "RAW", "schema": "S", "name": "events", "identifier": "events", "columns": {
            "original_timestamp": {"data_type": "VARCHAR"}, "amount": {"data_type": "VARCHAR"}, "day": {"data_type": "VARCHAR"}}}},
        "nodes": {"model.p.m": {"resource_type": "model", "name": "m", "depends_on": {"nodes": []}, "relation_name": "A.B.M",
                                "compiled_code": "select try_to_timestamp_ntz(original_timestamp) as at, try_to_number(amount) as n, "
                                                 "try_to_date(day) as d from raw.s.events"}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.EVENTS"]["columns"]
    assert columns["ORIGINAL_TIMESTAMP"]["format"] == "%Y-%m-%d %H:%M:%S", columns
    assert columns["DAY"].get("format") == "%Y-%m-%d" or columns["DAY"].get("type", "").upper() == "VARCHAR", columns
    assert columns["AMOUNT"].get("type") == "VARCHAR", columns


def test_text_parsed_with_a_format_is_written_in_that_format_also_inside_a_document():
    import duckdb

    from fauxflake.init import build_spec

    manifest = {
        "sources": {"source.p.s.issues": {"database": "RAW", "schema": "S", "name": "issues", "identifier": "issues", "columns": {
            "fields": {"data_type": "VARIANT"}, "created": {"data_type": "VARCHAR"}}}},
        "nodes": {"model.p.m": {"resource_type": "model", "name": "m", "depends_on": {"nodes": []}, "relation_name": "A.B.M",
                                "compiled_code": """select to_timestamp_tz(fields:resolutiondate::varchar, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM') as r,
                                                           to_timestamp(created, 'YYYY-MM-DD HH24:MI:SS.FF3') as c from raw.s.issues"""}},
    }
    columns = build_spec(manifest, "A")[0]["tables"]["RAW.S.ISSUES"]["columns"]
    inside = columns["FIELDS"]["fields"]["resolutiondate"]
    assert inside["format"] == "%Y-%m-%dT%H:%M:%S.%f000+0000" and columns["CREATED"]["format"] == "%Y-%m-%d %H:%M:%S.%g", columns
    # what strftime writes with it is what Snowflake reads with the model's format
    written = duckdb.connect().execute(f"select strftime(timestamp '2024-01-02 03:04:05.123456', '{inside['format']}')").fetchone()[0]
    assert written == "2024-01-02T03:04:05.123456000+0000"


def test_a_number_lined_up_with_a_made_up_text_in_a_union_makes_the_text_a_number():
    from fauxflake.init import _share_union_values

    tables = {"A.S.ONE": {"columns": {"DURATION": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}}},
              "A.S.TWO": {"columns": {"DURATION": {"gen": "template", "template": "duration_{hex}"}}},
              "A.S.THREE": {"columns": {"DURATION": {"gen": "template", "template": "x-{hex}"}}}}
    _share_union_values(tables, [{("A.S.ONE", "DURATION"), ("A.S.TWO", "DURATION"), ("A.S.THREE", "DURATION")}])
    assert tables["A.S.TWO"]["columns"]["DURATION"] == {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    assert tables["A.S.THREE"]["columns"]["DURATION"]["gen"] == "template"  # a text with a shape of its own stays


def test_a_placeholder_number_lined_up_with_a_reference_in_a_union_holds_the_keys():
    from fauxflake.init import _share_union_values

    ref = {"gen": "fk", "ref": "A.S.ACCOUNT", "skew": 1.5}
    tables = {"A.S.NOW": {"columns": {"ACCOUNT_ID": dict(ref)}},
              "A.S.BEFORE": {"columns": {"ACCOUNT_ID": {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}}}}
    _share_union_values(tables, [{("A.S.NOW", "ACCOUNT_ID"), ("A.S.BEFORE", "ACCOUNT_ID")}])
    assert tables["A.S.BEFORE"]["columns"]["ACCOUNT_ID"] == ref


def test_dbt_first_runs_its_commands_before_each_build(tmp_path, monkeypatch):
    """`dbt: {first: [seed]}`: a project whose `dbt seed` makes its own sources (a seed's post-hook) is seeded before
    each build, on either engine."""
    import dbt.cli.main

    from fauxflake.dbt import DbtProject

    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p\nversion: '1'\nconfig-version: 2\n")
    ran = []

    class Runner:
        def invoke(self, args):  # noqa: ANN001, ANN201
            ran.append(next(a for a in args if not a.startswith("-") and a not in ("none",)))
            return type("R", (), {"success": True, "exception": None})()

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", Runner)
    monkeypatch.setenv("FAUXFLAKE_FIRST", "seed")
    project = DbtProject(tmp_path, {"type": "snowflake", "host": "example"})
    assert project.invoke(["build"]) and ran == ["seed", "build"]
    ran.clear()
    assert project.invoke(["test"]) and ran == ["test"]
