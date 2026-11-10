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
    assert use.paths == {"USER": {"login": "TEXT", "id": "INT"}, "PAYLOAD": {"repo.name": "TEXT"}}
    assert use.arrays == {"LABELS"} and use.formats == {"CLOSED_ON": "%Y/%m/%d"}

    scan_model("""select parse_json(context):app.version::string as v, parse_json(raw_blob) as blob
                  from raw.gh.issues where user_id <> 'USLACKBOT' and state = 'open' and created = '2025-01-01'""", sources)
    assert use.json_text == {"CONTEXT", "RAW_BLOB"} and use.paths["CONTEXT"] == {"app.version": "TEXT"}
    assert use.texts == {"USER_ID", "STATE"}  # a date-looking literal says nothing about the type
    assert use.literals == {"USER_ID": {"USLACKBOT"}, "STATE": {"open"}}


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
