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
    assert fields["txs[].outcome.receipt_ids[]"] == {"gen": "link", "domain": fields["receipts[].receipt_id"]["domain"],
                                                     "depth": 1, "layered": True, "slot": 1}
    assert fields["receipts[].outcome.receipt_ids[]"]["slot"] == 0


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
