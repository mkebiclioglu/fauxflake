"""Round trip: generate from a spec, profile the result like prod, compare."""

import shutil
from datetime import datetime

import snowflake.connector
import yaml

from fauxflake.profile import fit_volume, profile_spec

CUSTOMERS = ("{volume: {rows_per_day: 40}, columns: {CUSTOMER_ID: {gen: sequence}, "
             "CREATED_AT: {gen: event_time}, _LOADED_AT: {gen: loaded_at}}}")

SKELETON = """
seed: 1
start: 2025-01-01
tables:
  RAW.SHOP.EVENTS:
    key: event_id
    event_time: occurred_at
    loaded_at: _loaded_at
    columns:
      CUSTOMER_ID: {gen: fk, ref: RAW.SHOP.CUSTOMERS, skew: 2.0}
  RAW.SHOP.ORDERS:
    key: order_id
    event_time: ordered_at
    loaded_at: _loaded_at
    columns:
      CUSTOMER_ID: {gen: fk, ref: RAW.SHOP.CUSTOMERS}
      UPDATED_AT: {gen: updated_at}
      STATUS: {gen: constant, value: placed}
"""


def test_profile_recovers_the_shape(warehouse, emu, spec, tmp_path):
    path = tmp_path / "fauxflake.yml"
    path.write_text(SKELETON)
    conn = snowflake.connector.connect(**emu.connect_kwargs(database="RAW", schema="SHOP"))
    profile_spec(path, conn, days=90, k_anonymity=5, log=lambda *_: None)
    got = yaml.safe_load(path.read_text())["tables"]

    events, truth = got["RAW.SHOP.EVENTS"], spec.tables["RAW.SHOP.EVENTS"].raw
    assert abs(events["volume"]["rows_per_day"] / (truth["volume"]["rows_per_day"] * 0.001 * 1.6 ** (119 / 365)) - 1) < 0.2
    assert events["volume"]["weekly"][5] > events["volume"]["weekly"][0]  # weekend uplift survives
    assert events["columns"]["EVENT_ID"] == {"gen": "hash_id"}
    assert events["columns"]["CUSTOMER_ID"]["gen"] == "fk"  # declared columns are left alone
    freq = events["columns"]["EVENT_TYPE"]["values"]
    assert abs(freq["page_view"] - 0.7) < 0.03 and abs(freq["purchase"] - 0.04) < 0.01
    seconds = events["lateness"]["seconds"]
    assert 1 < seconds[1] < 6 and 10_000 < seconds[3] < 40_000 and seconds[-1] <= 250_700

    orders = got["RAW.SHOP.ORDERS"]["columns"]
    assert orders["AMOUNT"]["type"] == "DECIMAL(12,2)"
    assert 30 < orders["AMOUNT"]["v"][4] < 48  # median near 39
    assert orders["CHANNEL"]["gen"] == "categorical"

    # and the measured spec is immediately usable to generate again
    from fauxflake.emulator import serve
    from fauxflake.generate import Warehouse
    from fauxflake.spec import Spec
    text = path.read_text().replace("tables:", "tables:\n  RAW.SHOP.CUSTOMERS: " + CUSTOMERS, 1)
    path.write_text(text)
    with serve() as again:
        counts = Warehouse(again, Spec.load(path), 1.0).advance_to(datetime(2025, 2, 1))
    assert counts["RAW.SHOP.EVENTS"] > 100_000


def test_rare_values_are_never_recorded(warehouse, emu, tmp_path):
    cur = emu.cursor()
    cur.execute("update RAW.SHOP.EVENTS set EVENT_TYPE = 'secret-campaign-x' where EVENT_ID in "
                "(select EVENT_ID from RAW.SHOP.EVENTS where OCCURRED_AT > '2025-04-01' limit 7)")
    path = tmp_path / "fauxflake.yml"
    path.write_text(SKELETON)
    conn = snowflake.connector.connect(**emu.connect_kwargs(database="RAW", schema="SHOP"))
    profile_spec(path, conn, k_anonymity=50, log=lambda *_: None)
    assert "secret-campaign-x" not in path.read_text()


def test_fit_volume_finds_growth_and_seasonality():
    from datetime import date, timedelta
    daily = [(date(2025, 1, 1) + timedelta(days=i),
              1000 * 1.5 ** (i / 365) * (1.3 if (date(2025, 1, 1) + timedelta(days=i)).weekday() >= 5 else 1.0))
             for i in range(120)]
    v = fit_volume(daily)
    assert abs(v["growth_per_year"] - 0.5) < 0.05
    assert v["weekly"][6] / v["weekly"][0] > 1.25
