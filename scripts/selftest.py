"""Run a dbt project that brings its own data (seeds) and its own tests on the emulator.

    uv run python scripts/selftest.py <dbt project dir> [--database NAME] [--vars '{...}'] [--json out.json]

Packages such as dbt-utils ship integration tests: seeds with inputs and expected outputs,
and tests that compare them. Passing those on the emulator is evidence of correct answers,
not only of SQL that runs. Prints every node that did not pass, grouped by error.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("project_dir", type=Path)
    ap.add_argument("--database", default="ANALYTICS")
    ap.add_argument("--vars", default=None)
    ap.add_argument("--json")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--target", default=None, help="dbt target name, for projects that branch on target.name")
    ap.add_argument("dbt_args", nargs="*", help="extra arguments for `dbt build` (after --)")
    args = ap.parse_args()

    import os

    if args.target:
        os.environ["FAUXFLAKE_TARGET"] = args.target
    from fauxflake.dbt import DbtProject, emulator_output
    from fauxflake.emulator import serve
    from fauxflake.init import provide_env

    with serve() as emu:
        project = DbtProject(args.project_dir, emulator_output(emu.port, args.database, args.threads), name="selftest")
        project.deps()
        provide_env(project)
        extra = (["--vars", args.vars] if args.vars else []) + args.dbt_args
        project.invoke(["build", *extra], quiet=True)
    results = json.loads((project.target_path / "run_results.json").read_text())["results"]
    counts = collections.Counter(r["status"] for r in results)
    reasons: dict[str, list[str]] = collections.defaultdict(list)
    for r in results:
        if r["status"] in ("error", "fail"):
            message = (r.get("message") or "").strip().splitlines()
            line = next((m for m in message if re.search(r"Error|error|Got \d+ result", m)), message[0] if message else "")
            reasons[f'{r["status"]}: {re.sub(r"\s+", " ", line)[:170]}'].append(r["unique_id"].split(".", 2)[-1])
    print(f"{args.project_dir.name}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    for why, nodes in sorted(reasons.items(), key=lambda x: -len(x[1])):
        print(f"  {len(nodes):3}  {why}\n         e.g. {nodes[0]}")
    if args.json:
        Path(args.json).write_text(json.dumps({"counts": counts, "reasons": reasons}, indent=1))


if __name__ == "__main__":
    main()
