"""The inner loop: save a file, see the rebuilt models and what changed, in about a second.

On start the last commit is built once as a baseline. Every save rebuilds only
the edited models and their children, then reports how the output now differs
from that baseline, so the effect of uncommitted edits is always on screen.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import yaml

from . import diff as differ
from .dbt import DbtProject, emulator_output
from .emulator import Emulator

WATCHED = (".sql", ".yml", ".yaml", ".csv", ".py", ".md")


class DevSession:
    def __init__(self, emu: Emulator, project_dir: Path, database: str, log=print):
        self.emu, self.dir, self.log = emu, project_dir.resolve(), log
        self.head = DbtProject(self.dir, emulator_output(emu.port, database))
        self.base: DbtProject | None = None
        self._tmp: Path | None = None
        self._top: Path | None = None
        config = yaml.safe_load((self.dir / "dbt_project.yml").read_text())
        self.model_paths = [self.dir / p for p in config.get("model-paths", ["models"])]
        others = [p for key in ("macro-paths", "seed-paths", "snapshot-paths", "test-paths")
                  for p in config.get(key, [key.split("-")[0] + "s"])]
        self.roots = self.model_paths + [self.dir / p for p in others]
        self._seen = self._scan()
        try:  # a baseline needs git; without it the loop still rebuilds
            self._top = Path(differ._git("rev-parse", "--show-toplevel", cwd=self.dir))
            self._tmp = Path(tempfile.mkdtemp(prefix="fauxflake-dev-"))
            base_dir = differ.checkout_base(self.dir, "HEAD", self._tmp / "base")
            self.base = DbtProject(base_dir, emulator_output(emu.port, database))
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.log("not a git repository: rebuilding on save, without a baseline to compare against")

    def _scan(self) -> dict[Path, float]:
        files = {self.dir / "dbt_project.yml": (self.dir / "dbt_project.yml").stat().st_mtime}
        for root in self.roots:
            if root.exists():
                for f in root.rglob("*"):
                    if f.suffix in WATCHED and f.is_file():
                        files[f] = f.stat().st_mtime
        return files

    def changed(self) -> list[Path]:
        now = self._scan()
        out = [f for f in now.keys() | self._seen.keys() if now.get(f) != self._seen.get(f)]
        self._seen = now
        return sorted(out)

    def start(self) -> bool:
        if self.base and not self.base.invoke(["run"], schema="BASE", quiet=True):
            self.log("the last commit does not build; continuing without a baseline")
            self.base = None
        return self.rebuild([])

    def rebuild(self, paths: list[Path]) -> bool:
        """Rebuild what `paths` affect (everything, when in doubt) and report."""
        t0 = time.time()
        only_models = paths and all(
            p.suffix == ".sql" and p.exists() and any(r in p.parents for r in self.model_paths) for p in paths)
        select = [arg for p in paths for arg in ("--select", f"path:{p.relative_to(self.dir)}+")] if only_models else []
        # full refresh: an incremental model downstream of an edit would otherwise keep rows
        # computed by the old logic, and the comparison would understate the change
        ok = self.head.invoke(["build", "--full-refresh", *select], schema="HEAD", quiet=True)
        took = time.time() - t0
        what = ", ".join(p.name for p in paths) or "everything"
        self.log(f"{'ok    ' if ok else 'FAILED'} {what}  ({took:.1f}s)")
        if ok and self.base:
            for d in differ.compare(self.emu, self.base, self.head, "BASE", "HEAD"):
                if d.status in ("added", "removed"):
                    self.log(f"       {d.name}: {d.status}")
                elif d.status == "changed":
                    cols = ", ".join(f"{c} {n:,}" for c, n in d.column_changes.items())
                    cols += "".join(f" +{c}" for c in d.columns_added) + "".join(f" -{c}" for c in d.columns_removed)
                    rows = f"{d.base_rows:,}" if d.base_rows == d.head_rows else f"{d.base_rows:,} -> {d.head_rows:,}"
                    self.log(f"       {d.name}: {d.changed_rows:,} of {rows} rows differ from HEAD"
                             + (f"  [{cols.strip()}]" if cols.strip() else ""))
        return ok

    def close(self) -> None:
        if self._tmp and self._top:
            subprocess.run(["git", "worktree", "remove", "--force", str(self._tmp / "base")], cwd=self._top, capture_output=True)
            shutil.rmtree(self._tmp, ignore_errors=True)

    def watch(self, interval: float = 0.4) -> None:
        self.log("watching for changes (Ctrl-C to stop)")
        try:
            while True:
                time.sleep(interval)
                paths = self.changed()
                if paths:
                    time.sleep(0.1)  # let editors finish writing
                    paths = sorted(set(paths) | set(self.changed()))
                    self.rebuild(paths)
        except KeyboardInterrupt:
            pass
