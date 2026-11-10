from datetime import datetime
from pathlib import Path

import pytest

from fauxflake.emulator import serve
from fauxflake.generate import Warehouse
from fauxflake.spec import Spec

EXAMPLE = Path(__file__).parent.parent / "example"


@pytest.fixture(scope="session")
def spec():
    return Spec.load(EXAMPLE / "fauxflake.yml")


@pytest.fixture()
def emu():
    with serve() as e:
        yield e


@pytest.fixture()
def warehouse(emu, spec):
    wh = Warehouse(emu, spec, scale=0.001)
    wh.advance_to(datetime(2025, 5, 1))
    return wh
