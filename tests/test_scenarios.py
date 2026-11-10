from datetime import date, datetime, timedelta

from fauxflake.emulator import serve
from fauxflake.generate import Warehouse, scenario

from test_generate import TABLES, checksum

DAY = date(2025, 2, 10)


def events_on(emu, day, extra="count(*)"):
    return emu.cursor().execute(f"select {extra} from RAW.SHOP.EVENTS where OCCURRED_AT::date = '{day}'").fetchone()[0]


def test_scenarios_bend_the_data(spec):
    end = datetime(2025, 2, 20)
    with serve() as emu:
        Warehouse(emu, spec, 0.0005).advance_to(end)
        normal = events_on(emu, DAY)
    with serve() as emu:
        Warehouse(emu, spec, 0.0005, scenario("empty-day", DAY, days=1)).advance_to(end)
        assert events_on(emu, DAY) == 0 and events_on(emu, DAY + timedelta(days=1)) > 0
    with serve() as emu:
        Warehouse(emu, spec, 0.0005, scenario("volume-spike", DAY, days=1)).advance_to(end)
        assert 8 < events_on(emu, DAY) / normal < 12
    with serve() as emu:
        Warehouse(emu, spec, 0.0005, scenario("duplicate-storm", DAY, days=1)).advance_to(end)
        assert 0.2 < events_on(emu, DAY, "1 - count(distinct EVENT_ID) / count(*)") < 0.3
    with serve() as emu:
        Warehouse(emu, spec, 0.0005, scenario("null-surge", DAY, days=1)).advance_to(end)
        assert 0.25 < events_on(emu, DAY, "count(*) filter (where EVENT_TYPE is null) / count(*)") < 0.35
        assert events_on(emu, DAY, "count(*) filter (where EVENT_ID is null)") == 0  # keys are never nulled
    with serve() as emu:
        Warehouse(emu, spec, 0.0005, scenario("loader-outage", DAY, days=1)).advance_to(end)
        gap = emu.cursor().execute("""select count(*) from RAW.SHOP.EVENTS
            where _LOADED_AT >= '2025-02-10 00:00:00' and _LOADED_AT < '2025-02-11 06:00:00'""").fetchone()[0]
        assert gap == 0  # nothing lands while the loader is down


def test_scenarios_keep_the_clock_deterministic(spec):
    end = datetime(2025, 2, 20)
    sc = scenario("late-spike", DAY)
    with serve() as a, serve() as b:
        Warehouse(a, spec, 0.0005, sc).advance_to(end)
        stepped = Warehouse(b, spec, 0.0005, sc)
        stepped.advance_to(end - timedelta(days=12))
        while stepped.now < end:
            stepped.advance_to(stepped.now + timedelta(days=1))
        for t in TABLES:
            assert checksum(a, t) == checksum(b, t), t
