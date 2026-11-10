"""Source table styles and storage."""

import copy
from datetime import datetime, timedelta

from fauxflake.emulator import serve
from fauxflake.generate import Warehouse
from fauxflake.spec import Spec, Table


def upsert_spec(spec):
    tables = {n: Table(n, copy.deepcopy(t.raw), copy.deepcopy(t.columns)) for n, t in spec.tables.items()}
    tables["RAW.SHOP.ORDERS"].raw["mode"] = "upsert"
    return Spec(seed=spec.seed, start=spec.start, tables=tables, dbt=spec.dbt)


def test_upsert_sources_hold_the_latest_version_per_key(spec):
    end = datetime(2025, 2, 20)
    up = upsert_spec(spec)
    state = "select count(*), count(distinct ORDER_ID), bit_xor(hash(t)) from RAW.SHOP.ORDERS t"
    with serve() as a, serve() as b, serve() as log:
        Warehouse(a, up, 0.0005).advance_to(end)
        stepped = Warehouse(b, up, 0.0005)
        stepped.advance_to(end - timedelta(days=8))
        while stepped.now < end:
            stepped.advance_to(stepped.now + timedelta(days=2))
        rows, keys, digest = a.cursor().execute(state).fetchone()
        assert rows == keys and (rows, keys, digest) == b.cursor().execute(state).fetchone()

        # and it is exactly the newest version of each order in the change log
        Warehouse(log, spec, 0.0005).advance_to(end)
        latest = log.cursor().execute("""select count(*), bit_xor(hash(t)) from (
            select * from RAW.SHOP.ORDERS qualify row_number() over (partition by ORDER_ID order by UPDATED_AT desc, _LOADED_AT desc) = 1) t""").fetchone()
        assert latest == (rows, digest)


def test_data_can_live_on_disk(spec, tmp_path):
    with serve(db_path=tmp_path) as emu:
        Warehouse(emu, spec, 0.0002).advance_to(datetime(2025, 1, 15))
        assert emu.cursor().execute("select count(*) from RAW.SHOP.EVENTS").fetchone()[0] > 10_000
    assert (tmp_path / "RAW.db").stat().st_size > 100_000
    with serve(db_path=tmp_path):  # a new run starts empty
        assert not (tmp_path / "RAW.db").exists()


def test_check_finds_columns_the_spec_does_not_generate(emu, spec):
    import json

    from conftest import EXAMPLE
    from fauxflake.dbt import DbtProject, emulator_output
    from fauxflake.init import check

    project = DbtProject(EXAMPLE, emulator_output(emu.port, "ANALYTICS"))
    project.invoke(["compile"], quiet=True)
    manifest = json.loads((project.target_path / "manifest.json").read_text())
    assert check(manifest, spec, "ANALYTICS") == []
    broken = upsert_spec(spec)
    del broken.tables["RAW.SHOP.ORDERS"].columns["CHANNEL"]
    del broken.tables["RAW.SHOP.EVENTS"]
    assert check(manifest, broken, "ANALYTICS") == [
        "RAW.SHOP.ORDERS.CHANNEL: used by the project, missing from the spec",
        "RAW.SHOP.EVENTS: used by the project, missing from the spec"]


def test_udfs_from_the_spec_are_callable(emu, spec):
    import snowflake.connector

    with_udfs = upsert_spec(spec)
    with_udfs.udfs = {"fiscal_year": "(d) as year(d + interval 11 month)", "id15to18": "(x) as x || 'AAA'"}
    Warehouse(emu, with_udfs, 0.0002)
    conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
    row = conn.cursor().execute(
        "select fiscal_year('2025-03-01'::date), analytics.udfs.fiscal_year('2025-01-15'::date), ID15TO18('abc')").fetchone()
    assert row == (2026, 2025, "abcAAA")
