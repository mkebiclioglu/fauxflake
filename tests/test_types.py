"""What the client is told a column holds: the type codes and Python values Snowflake's connector gives."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import snowflake.connector

from fauxflake import emulator


@pytest.fixture(scope="module")
def cur():
    with emulator.serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
        yield conn.cursor()


def test_arrays_and_objects_are_described_as_such(cur):
    cur.execute("select array_agg(column1) a, object_construct('k', 1) o, parse_json('1') v, array_construct(1) c from values (1), (2)")
    assert [(d.name, d.type_code) for d in cur.description] == [("A", 10), ("O", 9), ("V", 5), ("C", 10)]


def test_a_division_of_numbers_is_a_number(cur):
    cur.execute("select 1/4, avg(column1), 1e0/3, column1::float / 2 from values (1), (2), (4) group by column1 order by column1 limit 1")
    assert cur.fetchall() == [(Decimal("0.250000"), Decimal("1.000000"), Decimal("0.333333"), 0.5)]
    assert [d.scale for d in cur.description][:3] == [6, 6, 6]


def test_a_timestamp_tz_keeps_its_offset(cur):
    cur.execute("create or replace table zoned (t timestamp_tz, l timestamp_ltz)")
    cur.execute("insert into zoned values ('2025-01-01 12:00:00 +02:00', '2025-01-01 12:00:00 +02:00'), "
                "(convert_timezone('America/Los_Angeles', '2025-07-01 12:00:00'::timestamp_ntz), null)")
    cur.execute("select t, t::varchar, l from zoned order by t")
    rows = cur.fetchall()
    assert [r[0].utcoffset() for r in rows] == [timedelta(hours=2), timedelta(hours=-7)]
    assert [r[1] for r in rows] == ["2025-01-01 12:00:00.000 +0200", "2025-07-01 05:00:00.000 -0700"]
    assert rows[0][2] == datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    cur.execute("describe table zoned")
    assert [r[1] for r in cur.fetchall()] == ["TIMESTAMP_TZ(9)", "TIMESTAMP_LTZ"]
    cur.execute("select count(*) from zoned where t > '2025-01-01 11:00:00'")
    assert cur.fetchall() == [(1,)]


def test_the_session_time_zone(cur):
    """Measured on Snowflake with ALTER SESSION SET TIMEZONE = 'America/Los_Angeles' (2026-11-23)."""
    cur.execute("alter session set timezone = 'America/Los_Angeles'")
    try:
        cur.execute("select '2025-01-01 12:00:00 +00:00'::timestamp_ltz::varchar, '2025-01-01 12:00:00'::timestamp_ntz::timestamp_ltz::varchar, "
                    "'2025-01-01 12:00:00'::timestamp_tz::varchar, '2025-07-01 12:00:00'::timestamp_ntz::timestamp_tz::varchar, "
                    "'2025-01-01 12:00:00 +00:00'::timestamp_ltz::timestamp_ntz, '2025-01-01 02:00:00 +00:00'::timestamp_ltz::date, "
                    "hour('2025-01-01 12:00:00 +00:00'::timestamp_ltz), date_trunc('day', '2025-01-01 02:00:00 +00:00'::timestamp_ltz)::varchar, "
                    "convert_timezone('UTC', '2025-01-01 12:00:00'::timestamp_ntz)::varchar, "
                    "to_char('2025-01-01 12:00:00 +00:00'::timestamp_ltz, 'YYYY-MM-DD HH24:MI TZH:TZM')")
        assert cur.fetchall() == [("2025-01-01 04:00:00.000 -0800", "2025-01-01 12:00:00.000 -0800", "2025-01-01 12:00:00.000 -0800",
                                   "2025-07-01 12:00:00.000 -0700", datetime(2025, 1, 1, 4, 0), datetime(2024, 12, 31).date(), 4,
                                   "2024-12-31 00:00:00.000 -0800", "2025-01-01 20:00:00.000 Z", "2025-01-01 04:00 -08:00")]
        cur.execute("select '2025-01-01 12:00:00 +00:00'::timestamp_ltz, to_json(object_construct('t', '2025-01-01 12:00:00 +00:00'::timestamp_ltz)), "
                    "datediff(hour, '2025-01-01 00:00:00'::timestamp_ntz, '2025-01-01 12:00:00 +00:00'::timestamp_ltz)")
        value, text, hours = cur.fetchone()
        assert value.utcoffset() == timedelta(hours=-8) and value.hour == 4
        assert (text, hours) == ('{"t":"2025-01-01 04:00:00.000 -0800"}', 4)
        cur.execute("show parameters like 'TIMEZONE' in session")
        assert cur.fetchone()[1] == "America/Los_Angeles"
    finally:
        cur.execute("alter session unset timezone")
    cur.execute("select '2025-01-01 12:00:00'::timestamp_ntz::timestamp_ltz::varchar")
    assert cur.fetchall() == [("2025-01-01 12:00:00.000 Z",)]
