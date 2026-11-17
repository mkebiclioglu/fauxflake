"""The emulator must give the answers real Snowflake gave (recorded in conformance/golden.json)."""

import pytest
import snowflake.connector

from fauxflake import conformance as c
from fauxflake.emulator import serve


@pytest.fixture(scope="module")
def conn():
    with serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")  # the cases that create something name the schema the recording account has
        connection = snowflake.connector.connect(**emu.connect_kwargs(database="CONFORMANCE", schema="PUBLIC"))
        yield connection
        connection.close()


def _params():
    for case in c.cases():
        marks = [pytest.mark.xfail(reason=case["gap"], strict=True)] if case.get("gap") else []
        yield pytest.param(case, id=case["id"], marks=marks)


@pytest.mark.parametrize("case", _params())
def test_matches_snowflake(conn, case):
    expected = c.golden()[case["id"]]
    actual = c.run(conn, case["sql"])
    assert c.matches(expected, actual), f"\nsnowflake: {expected}\nemulator:  {actual}"
