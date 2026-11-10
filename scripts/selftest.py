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
    ap.add_argument("--steps", default="seed; build --exclude-resource-type seed",
                    help="dbt commands to run in order, separated by ';' (a node's last result counts)")
    ap.add_argument("--snowflake", default=None, metavar="CONNECTION",
                    help="also build on this real account (~/.snowflake/connections.toml) and compare every model row for row")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--schema", default="CI", help="default schema, for projects whose sources expect a fixed one")
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
        # seeds first: these packages read them as sources, which dbt cannot order after the seeds
        import shlex

        latest: dict[str, dict] = {}
        for step in [shlex.split(part) for part in args.steps.split(";") if part.strip()]:
            project.invoke([*step, *extra], schema=args.schema, quiet=True)
            path = project.target_path / "run_results.json"
            if path.exists():
                latest.update({r["unique_id"]: r for r in json.loads(path.read_text())["results"]})
                path.unlink()
        results = list(latest.values())
        parity = []
        if args.snowflake:
            import snowflake.connector

            from fauxflake.dbt import snowflake_output
            from fauxflake.parity import compare

            remote = DbtProject(args.project_dir, snowflake_output(args.snowflake, args.database, args.threads), name="selftest_snowflake")
            for step in (["seed", "--full-refresh"], ["run", "--full-refresh"]):
                remote.invoke([*step, *extra], schema=args.schema, quiet=True)
            conn = snowflake.connector.connect(connection_name=args.snowflake)
            parity = compare(conn, emu, project)
            conn.close()
    counts = collections.Counter(r["status"] for r in results)
    reasons: dict[str, list[str]] = collections.defaultdict(list)
    for r in results:
        if r["status"] in ("error", "fail"):
            message = (r.get("message") or "").strip().splitlines()
            lines = [m.strip() for m in message if m.strip() and "compiled code at" not in m]
            line = next((m for m in lines if re.search(r"\d{6} \(|Got \d+ result", m)), lines[-1] if lines else "")
            line = re.sub(r'"[^"]*"|\'[^\']*\'', "X", line)  # group errors that differ only in names
            reasons[f'{r["status"]}: {re.sub(r"\s+", " ", line)[:170]}'].append(r["unique_id"].split(".", 2)[-1])
    print(f"{args.project_dir.name}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    for why, nodes in sorted(reasons.items(), key=lambda x: -len(x[1])):
        print(f"  {len(nodes):3}  {why}\n         e.g. {nodes[0]}")
    if parity:
        # two builds minutes apart cannot agree on "now": set those models apart
        manifest = json.loads((project.target_path / "manifest.json").read_text())["nodes"]
        by_name = {n["name"]: uid for uid, n in manifest.items() if n["resource_type"] == "model"}
        clock = re.compile(r"\b(current_timestamp|current_date|current_time|sysdate|getdate|localtimestamp)\b", re.IGNORECASE)

        def reads_clock(uid: str, seen: set) -> bool:
            node = manifest.get(uid)
            if node is None or uid in seen:
                return False
            seen.add(uid)
            return bool(clock.search(node.get("compiled_code") or "")) or any(reads_clock(d, seen) for d in node["depends_on"]["nodes"])

        differing = [p for p in parity if not p.ok]
        timed = [p for p in differing if not p.note and p.emulator_rows == p.snowflake_rows and reads_clock(by_name.get(p.model, ""), set())]
        filled = sum(p.emulator_rows > 0 or p.snowflake_rows > 0 for p in parity)
        print(f"  row for row against Snowflake: {len(parity) - len(differing)} of {len(parity)} models identical "
              f"({filled} have rows); {len(timed)} more differ only where they read the current time")
        for p in differing:
            if p not in timed:
                print(f"    differs  {p.model}: emulator={p.emulator_rows} snowflake={p.snowflake_rows} "
                      f"only_emulator={p.only_emulator} only_snowflake={p.only_snowflake} {p.note}")
    if args.json:
        Path(args.json).write_text(json.dumps({"counts": counts, "reasons": reasons,
                                               "parity": [vars(p) for p in parity]}, indent=1))


if __name__ == "__main__":
    main()
