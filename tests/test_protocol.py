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
