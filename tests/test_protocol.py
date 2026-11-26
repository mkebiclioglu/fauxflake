"""The Snowflake endpoint itself, as the connector uses it."""

import json
import urllib.request

import snowflake.connector

from fauxflake import emulator


def test_a_repeated_request_returns_the_first_answer():
    # the connector re-sends a statement it has waited on for about a minute, with the same requestId
    with emulator.serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
        cur = conn.cursor()
        cur.execute("create table t (x int)")
        url = f"http://127.0.0.1:{emu.port}/queries/v1/query-request?requestId=11111111-2222-3333-4444-555555555555"
        body = json.dumps({"sqlText": "insert into t values (1)"}).encode()
        headers = {"Authorization": f'Snowflake Token="{conn.rest.token}"', "Content-Type": "application/json"}
        answers = [json.loads(urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers)).read())
                   for _ in range(2)]
        assert [a["success"] for a in answers] == [True, True]
        assert cur.execute("select count(*) from t").fetchall() == [(1,)]  # ran once, not twice
        conn.close()


def test_dml_statistics_as_snowflake_reports_them():
    """cursor.stats (dbt's rows_inserted / rows_updated / rows_deleted), measured on Snowflake for each kind of statement."""
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        emu.ensure_database("P", "PUBLIC")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="P", schema="PUBLIC"))
        cur = conn.cursor()
        seen = []
        for sql in ["create or replace table st (id int, v varchar)", "insert into st values (1,'a'),(2,'b')", "update st set v = 'z' where id = 1",
                    "merge into st using (select 2 id, 'm' v union all select 3, 'n') s on st.id = s.id when matched then update set v = s.v "
                    "when not matched then insert values (s.id, s.v)", "delete from st where id = 3", "select * from st",
                    "create or replace table st2 as select * from st", "truncate table st2"]:
            cur.execute(sql)
            seen.append((cur.stats.num_rows_inserted, cur.stats.num_rows_updated, cur.stats.num_rows_deleted))
        conn.close()
    assert seen == [(None, None, None), (2, 0, 0), (0, 1, 0), (1, 1, 0), (0, 0, 1), (None, None, None), (2, 0, 0), (0, 0, 2)]


def test_result_types_as_snowflake_describes_them():
    """The type each value is described as (dbt's contracts compare these), measured on Snowflake."""
    import snowflake.connector
    from snowflake.connector.constants import FIELD_ID_TO_NAME

    from fauxflake.emulator import serve

    expected = {"'2020-01-01'::timestamp_ltz": "TIMESTAMP_LTZ", "current_timestamp()": "TIMESTAMP_LTZ", "sysdate()": "TIMESTAMP_NTZ",
                "'2020-01-01 00:00:00 +02:00'::timestamp_tz": "TIMESTAMP_TZ", "cast(null as timestamp_tz)": "TIMESTAMP_TZ",
                "cast(null as timestamp_ltz)": "TIMESTAMP_LTZ", "to_geography('POINT(1 2)')": "GEOGRAPHY",
                "to_geometry('POINT(1 2)')": "GEOMETRY", "[1, 2]::vector(int, 2)": "VECTOR"}
    with serve() as emu:
        emu.ensure_database("P", "PUBLIC")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="P", schema="PUBLIC"))
        cur = conn.cursor()
        seen = {}
        for expr in expected:
            cur.execute(f"select {expr} as x")
            cur.fetchall()
            seen[expr] = FIELD_ID_TO_NAME[cur.description[0].type_code]
        cur.execute("alter session set timezone = 'America/New_York'")
        (shown,) = cur.execute("select '2020-01-01 12:00:00'::timestamp_ltz").fetchone()
        conn.close()
    assert seen == expected
    assert (shown.hour, str(shown.tzinfo)) == (12, "America/New_York")
