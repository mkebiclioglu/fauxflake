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
