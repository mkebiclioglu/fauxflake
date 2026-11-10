"""A code change is reported as the exact rows and columns it alters."""

import shutil
import subprocess
from datetime import datetime

from fauxflake.diff import markdown, run
from fauxflake.generate import Warehouse

from conftest import EXAMPLE


def git(*args, cwd):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args], cwd=cwd, check=True, capture_output=True)


def test_diff_pinpoints_a_logic_change(emu, spec, tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(EXAMPLE, repo / "analytics", ignore=shutil.ignore_patterns("target", ".fauxflake", "logs"))
    git("init", "-q", "-b", "main", cwd=repo)
    git("add", "-A", cwd=repo)
    git("commit", "-q", "-m", "base", cwd=repo)

    model = repo / "analytics" / "models" / "staging" / "stg_orders.sql"
    model.write_text(model.read_text().replace("iff(status = 'returned', 0, amount)",
                                               "iff(status in ('returned', 'placed'), 0, amount)"))
    (repo / "analytics" / "models" / "marts" / "new_model.sql").write_text("select 1 as one")

    rows = sum(Warehouse(emu, spec, 0.0005).advance_to(datetime(2025, 1, 21)).values())
    diffs = {d.name: d for d in run(emu, repo / "analytics", "main", "ANALYTICS", [], log=lambda *_: None)}

    assert diffs["stg_customers"].status == "same" and diffs["fct_daily_events"].status == "same"
    assert diffs["new_model"].status == "added"
    orders = diffs["fct_orders"]
    assert orders.status == "changed" and orders.key == "ORDER_ID"
    assert list(orders.column_changes) == ["NET_AMOUNT"]  # only the column the change touches
    assert 0 < orders.changed_rows < orders.head_rows

    report = markdown(list(diffs.values()), "main", rows)
    assert "`NET_AMOUNT`" in report and "`new_model` | added" in report
    assert not (repo / ".git" / "worktrees").exists() or not any((repo / ".git" / "worktrees").iterdir())
