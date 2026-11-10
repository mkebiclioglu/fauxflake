"""Save a model, get it and its children rebuilt, with the change against HEAD."""

import shutil
from datetime import datetime

from fauxflake.dev import DevSession
from fauxflake.generate import Warehouse

from conftest import EXAMPLE
from test_diff import git


def test_save_rebuilds_and_reports(emu, spec, tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(EXAMPLE, repo, ignore=shutil.ignore_patterns("target", ".fauxflake", "logs"))
    git("init", "-q", "-b", "main", cwd=repo)
    git("add", "-A", cwd=repo)
    git("commit", "-q", "-m", "base", cwd=repo)
    Warehouse(emu, spec, 0.0005).advance_to(datetime(2025, 1, 21))

    lines: list[str] = []
    session = DevSession(emu, repo, "ANALYTICS", log=lines.append)
    try:
        assert session.start() and lines[-1].startswith("ok     everything")
        assert session.changed() == []

        model = repo / "models" / "staging" / "stg_orders.sql"
        model.write_text(model.read_text().replace("iff(status = 'returned', 0, amount)", "0"))
        assert session.changed() == [model]
        lines.clear()
        assert session.rebuild([model])
        assert lines[0].startswith("ok     stg_orders.sql")
        assert any("fct_orders" in line and "NET_AMOUNT" in line for line in lines)

        model.write_text("select nonsense from")
        lines.clear()
        assert not session.rebuild(session.changed())
        assert lines[0].startswith("FAILED")
    finally:
        session.close()
