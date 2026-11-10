"""What does this change do to the data?

Inputs are deterministic, so the base branch and the working tree can be built
on exactly the same synthetic rows. Whatever differs in the output is caused by
the code change and nothing else.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .dbt import DbtProject, emulator_output
from .emulator import Emulator


@dataclass
class ModelDiff:
    name: str
    status: str  # same | changed | added | removed
    base_rows: int = 0
    head_rows: int = 0
    changed_rows: int = 0  # rows present in only one version (or keys that differ, when a key is known)
    key: str | None = None
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    columns_retyped: list[str] = field(default_factory=list)
    column_changes: dict[str, int] = field(default_factory=dict)


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def checkout_base(project_dir: Path, base: str, into: Path) -> Path:
    """The dbt project as it is at `base`, in a throwaway worktree."""
    top = Path(_git("rev-parse", "--show-toplevel", cwd=project_dir))
    _git("worktree", "add", "--detach", str(into), base, cwd=top)
    base_project = into / project_dir.resolve().relative_to(top.resolve())
    packages = project_dir / "dbt_packages"
    if packages.exists() and not (base_project / "dbt_packages").exists():
        (base_project / "dbt_packages").symlink_to(packages.resolve())
    return base_project


def _unique_keys(project: DbtProject) -> dict[str, str]:
    manifest = json.loads((project.target_path / "manifest.json").read_text())
    keys = {}
    for node in manifest["nodes"].values():
        meta = node.get("test_metadata")
        if node["resource_type"] == "test" and meta and meta["name"] == "unique" and node.get("column_name"):
            model = manifest["nodes"].get(node.get("attached_node") or "")
            if model and model["resource_type"] == "model":
                keys.setdefault(model["name"], node["column_name"].upper())
    return keys


def compare(emu: Emulator, base: DbtProject, head: DbtProject, base_schema: str, head_schema: str) -> list[ModelDiff]:
    cur = emu.cursor()
    b = {n["name"]: n for n in base.models()}
    h = {n["name"]: n for n in head.models()}
    keys = _unique_keys(head)
    out = []
    for name in sorted(set(b) | set(h)):
        if name not in b or name not in h:
            node, schema = (h[name], head_schema) if name in h else (b[name], base_schema)
            rows = cur.execute(f'select count(*) from {node["database"]}.{schema}.{node.get("alias") or name}').fetchone()[0]
            out.append(ModelDiff(name, "added" if name in h else "removed",
                                 base_rows=0 if name in h else rows, head_rows=rows if name in h else 0))
            continue
        rel_b = f'{b[name]["database"]}.{base_schema}.{b[name].get("alias") or name}'
        rel_h = f'{h[name]["database"]}.{head_schema}.{h[name].get("alias") or name}'
        cols_b = {c: t for c, t, *_ in cur.execute(f"describe {rel_b}").fetchall()}
        cols_h = {c: t for c, t, *_ in cur.execute(f"describe {rel_h}").fetchall()}
        d = ModelDiff(name, "same",
                      base_rows=cur.execute(f"select count(*) from {rel_b}").fetchone()[0],
                      head_rows=cur.execute(f"select count(*) from {rel_h}").fetchone()[0],
                      columns_added=[c for c in cols_h if c not in cols_b],
                      columns_removed=[c for c in cols_b if c not in cols_h],
                      columns_retyped=[c for c in cols_h if c in cols_b and cols_b[c] != cols_h[c]])
        common = [c for c in cols_h if c in cols_b]
        sel = ", ".join(f'"{c}"' for c in common)
        key = keys.get(name)
        if key in common and cur.execute(f'select count(*) = count(distinct "{key}") from {rel_h}').fetchone()[0]:
            d.key = key
            rest = [c for c in common if c != key]
            per_col = ", ".join(f'count(*) filter (where b."{c}" is distinct from h."{c}")' for c in rest) or "0"
            anyc = " or ".join(f'b."{c}" is distinct from h."{c}"' for c in rest) or "false"
            row = cur.execute(f"""
                select count(*) filter (where b."{key}" is null or h."{key}" is null or {anyc}), {per_col}
                from {rel_b} b full outer join {rel_h} h on b."{key}" = h."{key}" """).fetchone()
            d.changed_rows = row[0]
            d.column_changes = {c: n for c, n in zip(rest, row[1:]) if n}
        elif common:
            d.changed_rows = cur.execute(f"""
                select greatest((select count(*) from (select {sel} from {rel_b} except all select {sel} from {rel_h})),
                                (select count(*) from (select {sel} from {rel_h} except all select {sel} from {rel_b})))
            """).fetchone()[0]
        if d.changed_rows or d.columns_added or d.columns_removed or d.columns_retyped or d.base_rows != d.head_rows:
            d.status = "changed"
        out.append(d)
    return out


def run(emu: Emulator, project_dir: Path, base_ref: str, database: str, dbt_args: list[str], log=print) -> list[ModelDiff]:
    """Sources must already be generated in `emu`."""
    tmp = Path(tempfile.mkdtemp(prefix="fauxflake-base-"))
    top = Path(_git("rev-parse", "--show-toplevel", cwd=project_dir))
    try:
        base_dir = checkout_base(project_dir, base_ref, tmp / "base")
        head = DbtProject(project_dir, emulator_output(emu.port, database))
        base = DbtProject(base_dir, emulator_output(emu.port, database))
        log(f"building {base_ref} ...")
        if not base.invoke(["run", *dbt_args], schema="BASE", quiet=True):
            raise SystemExit(f"dbt run failed on {base_ref}")
        log("building the working tree ...")
        if not head.invoke(["run", *dbt_args], schema="HEAD", quiet=True):
            raise SystemExit("dbt run failed on the working tree")
        return compare(emu, base, head, "BASE", "HEAD")
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(tmp / "base")], cwd=top, capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)


def markdown(diffs: list[ModelDiff], base_ref: str, source_rows: int) -> str:
    changed = [d for d in diffs if d.status != "same"]
    lines = [f"### fauxflake data diff vs `{base_ref}`", "",
             f"Both versions were built on the same {source_rows:,} synthetic source rows, "
             "so every difference below comes from the code change.", ""]
    if not changed:
        return "\n".join(lines + [f"No model output changed ({len(diffs)} models compared)."])
    lines += ["| model | rows | changed rows | columns |", "| --- | --- | --- | --- |"]
    for d in changed:
        if d.status in ("added", "removed"):
            lines.append(f"| `{d.name}` | {d.status}, {max(d.base_rows, d.head_rows):,} rows | | |")
            continue
        rows = f"{d.base_rows:,}" if d.base_rows == d.head_rows else f"{d.base_rows:,} → {d.head_rows:,}"
        pct = f" ({d.changed_rows / max(d.base_rows, d.head_rows, 1):.1%})" if d.changed_rows else ""
        notes = [f"`{c}` {n:,}" for c, n in sorted(d.column_changes.items(), key=lambda x: -x[1])]
        notes += [f"+`{c}`" for c in d.columns_added] + [f"−`{c}`" for c in d.columns_removed]
        notes += [f"`{c}` retyped" for c in d.columns_retyped]
        how = "" if d.key else " (no unique key: counted as rows present in only one version)"
        lines.append(f"| `{d.name}` | {rows} | {d.changed_rows:,}{pct}{how} | {', '.join(notes)} |")
    same = len(diffs) - len(changed)
    if same:
        lines += ["", f"{same} other model{'s' if same != 1 else ''} unchanged."]
    return "\n".join(lines)
