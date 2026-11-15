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
    assert request["templates"] == ["POST {hex}/api/v1/ack {hex}", "POST {hex}/api/v1/sendxpush {hex}", "{hex} {hex} {hex}"]  # `_` in LIKE stands for any one character


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
