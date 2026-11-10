"""Measure how much of a Snowflake dbt project's SQL the fauxflake emulator can handle.

    uv run python scripts/compat.py render  <dbt project dir> --out <dir>   # dbt compile -> <dir>/*.sql
    uv run python scripts/compat.py measure <name>=<dir> [...] --json report.json

`render` runs `dbt deps` + `dbt compile` with the real dbt-snowflake adapter pointed at the
emulator (no tables exist, so models whose Jinja needs to query the warehouse do not render).
`measure` scores each rendered model at three levels, none of which binds against real schemas:

  L1  parses as Snowflake SQL in sqlglot
  L2  goes through the emulator's transform pipeline, generates DuckDB SQL with no
      "unsupported" construct, and DuckDB's parser accepts it
  L3  every function in the DuckDB SQL exists in the emulator's DuckDB catalog

    uv run python scripts/compat.py execute <dbt project dir> <dir> --json exec.json

`execute` goes further: it runs every rendered model through the emulator, in DAG order, as
`CREATE TABLE AS` on empty tables. Sources are stubbed and their columns are inferred from
binder errors (all VARCHAR), so it proves "binds and plans in the emulator", not correct results.

The project is treated as data: only its SQL, YAML and Jinja macros are read.
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

# COMPAT_SRC pins the emulator to a snapshot (e.g. `git archive <rev> src`) while the repo keeps moving.
sys.path.insert(0, os.environ.get("COMPAT_SRC") or str(Path(__file__).resolve().parent.parent / "src"))


# ---------------------------------------------------------------- render

def render(project_dir: Path, out: Path, target: str, threads: int) -> None:
    import yaml
    from dbt.cli.main import dbtRunner

    from fauxflake.emulator import serve

    project = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
    prof_dir = out / "_profiles"
    prof_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")

    def invoke(port: int, *args: str):
        (prof_dir / "profiles.yml").write_text(yaml.safe_dump({project["profile"]: {"target": target, "outputs": {target: {
            "type": "snowflake", "account": "fauxflake", "user": "ci", "password": "ci", "host": "127.0.0.1",
            "port": port, "protocol": "http", "database": "ANALYTICS", "schema": "CI", "warehouse": "LOCAL",
            "role": "CI", "threads": threads}}}}))
        return dbtRunner().invoke(["--quiet", "--no-version-check", *args, "--project-dir", str(project_dir),
                                   "--profiles-dir", str(prof_dir), "--target", target])

    with serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        def with_env(*args: str):  # projects read many env vars; give each missing one a placeholder
            for _ in range(120):
                res = invoke(emu.port, *args)
                m = re.search(r"Env var required but not provided: '([^']+)'", str(res.exception or ""))
                if not m:
                    return res
                name = m.group(1)  # keep databases distinct so same-named tables in RAW/PREP/PROD don't collide
                os.environ[name] = ("FX_" + re.sub(r"\W", "_", name)) if re.search(r"DATABASE|_DB\b", name) else "FX"
            return res

        if (project_dir / "packages.yml").exists() or (project_dir / "dependencies.yml").exists():
            with_env("deps")
        res = with_env("parse")
        if not res.success:
            print("dbt parse failed:", str(res.exception)[:1500])
            sys.exit(1)
        # `dbt compile` aborts on the first node that fails; here a failure just means
        # "this model's Jinja needs a real warehouse", so keep going and count it.
        from dbt.task.compile import CompileTask
        CompileTask.raise_on_first_error = lambda self: False
        CompileTask._mark_dependent_errors = lambda self, *a, **k: None  # compiling needs no upstream
        res = invoke(emu.port, "compile", "--no-populate-cache", "--select", "resource_type:model")

    manifests = sorted((p for p in project_dir.rglob("manifest.json") if "dbt_packages" not in p.parts),
                       key=lambda p: p.stat().st_mtime)
    if manifests:
        (out / "_manifest.json").write_bytes(manifests[-1].read_bytes())
    results = getattr(res.result, "results", None) or []
    if not results:
        print("dbt compile produced no results:", str(res.exception)[:1500])
    ok = 0
    errors = collections.Counter()
    for r in results:
        node = r.node
        if node.resource_type != "model":
            continue
        code = getattr(node, "compiled_code", None)
        if str(r.status) in ("success", "RunStatus.Success") and code:
            (out / f"{node.unique_id}.sql").write_text(code)
            ok += 1
        else:
            msg = re.sub(r"\s+", " ", str(r.message or "no compiled code"))
            errors[re.sub(r" in model \S+ \(\S+\)", "", msg)[:160]] += 1
    total = ok + sum(errors.values())
    (out / "_render.json").write_text(json.dumps({"models": total, "rendered": ok, "errors": errors.most_common(40)}, indent=1))
    print(f"rendered {ok}/{total} models")
    for msg, n in errors.most_common(8):
        print(f"  {n:5d}  {msg}")


# ---------------------------------------------------------------- measure

WINDOW_BUILTINS = {"row_number", "rank", "dense_rank", "lag", "lead", "first_value", "last_value", "nth_value",
                   "ntile", "percent_rank", "cume_dist", "first", "last"}


class Scorer:
    def __init__(self) -> None:
        import duckdb  # noqa: F401
        from fakesnow.instance import FakeSnow

        logging.getLogger("sqlglot").setLevel(logging.CRITICAL)
        from fauxflake import emulator

        self._serving = emulator.serve()  # the emulator exactly as dbt would see it
        self.fs = self._serving.__enter__()._fs
        self.conn = self.fs.connect(database="ANALYTICS", schema="CI")
        self.cur = self.conn.cursor()
        self.duck = self.fs.duck_conn.cursor()
        self.known = {r[0].lower() for r in self.duck.execute("select distinct function_name from duckdb_functions()").fetchall()}

    def score(self, sql: str) -> dict:
        """{'level': 0..3, 'stage': where it stopped, 'why': [reasons]}"""
        import duckdb
        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import ErrorLevel

        from fauxflake.emulator import rewrite

        try:
            tree = sqlglot.parse_one(rewrite(sql), read="snowflake")
        except Exception as e:  # noqa: BLE001
            return {"level": 0, "stage": "L1 parse", "why": [_norm(str(e))]}
        if tree is None or isinstance(tree, exp.Command):
            return {"level": 0, "stage": "L1 parse", "why": ["parsed only as opaque command"]}
        try:
            parts = [self.cur._transform(p, None) for p in self.cur._transform_explode(tree)]
            out = [p.sql(dialect="duckdb", unsupported_level=ErrorLevel.RAISE) for p in parts]
        except Exception as e:  # noqa: BLE001
            return {"level": 1, "stage": "L2 transpile", "why": [f"{type(e).__name__}: {_norm(str(e))}"]}
        missing: set[str] = set()
        for stmt in out:
            try:
                if len(duckdb.extract_statements(stmt)) < 1:
                    raise ValueError("empty statement")
                (tree_json,) = self.duck.execute("select json_serialize_sql(?)", [stmt]).fetchone()
            except Exception as e:  # noqa: BLE001
                return {"level": 1, "stage": "L2 duckdb parse", "why": [_norm(str(e))]}
            parsed = json.loads(tree_json)
            if parsed.get("error"):
                return {"level": 1, "stage": "L2 duckdb parse", "why": [_norm(parsed.get("error_message", "error"))]}
            for name in _functions(parsed):
                if re.fullmatch(r"[a-z_][a-z0-9_]*", name) and name not in self.known and name not in WINDOW_BUILTINS:
                    missing.add(name)
        if missing:
            return {"level": 2, "stage": "L3 functions", "why": sorted(missing)}
        return {"level": 3, "stage": "ok", "why": []}


def _functions(node):
    if isinstance(node, dict):
        if node.get("class") in ("FUNCTION", "WINDOW") and node.get("function_name"):
            if not node.get("is_operator"):
                yield f'{node.get("schema") + "." if node.get("schema") and False else ""}{node["function_name"]}'.lower()
        for v in node.values():
            yield from _functions(v)
    elif isinstance(node, list):
        for v in node:
            yield from _functions(v)


def _norm(msg: str) -> str:
    msg = re.sub(r"\x1b\[[0-9;]*m", "", msg)
    first = msg.strip().splitlines()[0] if msg.strip() else ""
    first = re.sub(r"[Ll]ine \d+,? [Cc]ol:? ?\d+\.?", "", first)
    first = re.sub(r"\d+", "N", first)
    return re.sub(r"\s+", " ", first).strip()[:140]


def features(sql: str) -> set[str]:
    """Snowflake functions and constructs a model uses, for the 'what do real projects need' ranking."""
    import sqlglot
    from sqlglot import exp

    out: set[str] = set()
    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except Exception:  # noqa: BLE001
        return out
    for node in tree.walk():
        if isinstance(node, exp.Anonymous):
            out.add(f"fn:{node.name.upper()}")
        elif isinstance(node, exp.Func):
            out.add(f"fn:{node.sql_name().upper()}")
        elif isinstance(node, (exp.Qualify, exp.Pivot, exp.Lateral, exp.JSONExtract, exp.MatchRecognize, exp.Connect)):
            out.add(f"syntax:{type(node).__name__.upper()}")
    return out


def measure(specs: list[str], json_path: str | None, examples: int) -> None:
    scorer = Scorer()
    report = {}
    for spec in specs:
        name, _, d = spec.partition("=")
        files = sorted(p for p in Path(d).glob("*.sql"))
        levels = collections.Counter()
        why: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
        sample: dict[str, list[str]] = collections.defaultdict(list)
        feat_total, feat_fail = collections.Counter(), collections.Counter()
        for f in files:
            sql = f.read_text()
            r = scorer.score(sql)
            for lvl in range(1, r["level"] + 1):
                levels[lvl] += 1
            fs = features(sql)
            feat_total.update(fs)
            if r["level"] < 3:
                feat_fail.update(fs)
                for reason in r["why"]:
                    why[r["stage"]][reason] += 1
                    if len(sample[reason]) < examples:
                        sample[reason].append(f.stem)
        n = len(files)
        render_info = json.loads((Path(d) / "_render.json").read_text()) if (Path(d) / "_render.json").exists() else {}
        report[name] = {
            "rendered": render_info, "scored": n,
            "L1": levels[1], "L2": levels[2], "L3": levels[3],
            "failures": {stage: c.most_common(40) for stage, c in why.items()},
            "examples": sample,
            "features": [(k, feat_total[k], feat_fail[k]) for k, _ in feat_total.most_common(400)],
        }
        pct = lambda x: f"{x:5d} ({100 * x / n:5.1f}%)" if n else "0"  # noqa: E731
        print(f"\n== {name}: {n} rendered models" + (f" of {render_info.get('models')}" if render_info else ""))
        print(f"   L1 parse {pct(levels[1])}   L2 transpile {pct(levels[2])}   L3 functions {pct(levels[3])}")
        for stage in ("L1 parse", "L2 transpile", "L2 duckdb parse", "L3 functions"):
            for reason, c in why[stage].most_common(12):
                print(f"   {stage:<16} {c:5d}  {reason}")
    if json_path:
        Path(json_path).write_text(json.dumps(report, indent=1))


# ---------------------------------------------------------------- execute

def _fq(relation_name: str) -> str:
    return ".".join(part.strip('"').upper() for part in relation_name.split("."))


# Errors that most likely come from stub columns all being VARCHAR, not from the emulator.
STUB_NOISE = re.compile(r"VARCHAR|Could not convert string '[^']*' to INT|Cannot extract field .* because it is not a struct")


TIMEY = re.compile(r"(_AT|_DATE|_TIME|_TIMESTAMP|_TS|_DATETIME)$|^(DATE|TIMESTAMP|CREATED|UPDATED|MODIFIED)$")


def execute(project_dir: Path, rendered: Path, json_path: str | None, max_passes: int, typed: bool = False) -> None:
    import graphlib

    import sqlglot
    from fakesnow.instance import FakeSnow
    from sqlglot import exp

    from fauxflake import emulator

    logging.getLogger("sqlglot").setLevel(logging.CRITICAL)
    man = json.loads((rendered / "_manifest.json").read_text())
    nodes = {**man["nodes"], **man["sources"]}
    serving = emulator.serve()  # the emulator exactly as dbt would see it
    fs = serving.__enter__()._fs
    duck = fs.duck_conn.cursor()
    cur = fs.connect(database="ANALYTICS", schema="CI").cursor()

    sql_of = {p.stem: p.read_text().strip().rstrip(";") for p in rendered.glob("*.sql")}
    rel = {uid: _fq(n["relation_name"]) for uid, n in nodes.items()
           if n.get("relation_name") and n["resource_type"] in ("model", "seed", "snapshot", "source")}
    ephemeral = {uid for uid, n in nodes.items() if n["resource_type"] == "model" and n["config"]["materialized"] == "ephemeral"}
    runnable = [uid for uid in sql_of if uid in rel and uid not in ephemeral]
    by_rel = {rel[uid]: uid for uid in runnable}

    def deps(uid: str, seen=None) -> set[str]:
        out, seen = set(), seen or set()
        for d in nodes[uid]["depends_on"]["nodes"]:
            if d in ephemeral and d not in seen:
                out |= deps(d, seen | {d})
            elif d in rel:
                out.add(d)
        return out

    dep = {uid: deps(uid) for uid in runnable}
    stubs: dict[str, set[str]] = {}

    def col_type(col: str) -> str:
        # optional second estimate: guess that date-looking column names are timestamps
        return "timestamp" if typed and TIMEY.search(col) else "varchar"

    def make_stub(fq: str) -> None:
        db, schema, _ = fq.split(".")
        try:
            fs.connect(database=db, schema=schema).close()
            duck.execute(f'create or replace table {fq} ("_FX_STUB" varchar)')
        except Exception:  # noqa: BLE001  (e.g. INFORMATION_SCHEMA sources)
            return
        stubs.setdefault(fq, set())
        for col in stubs[fq]:
            duck.execute(f'alter table {fq} add column if not exists "{col}" {col_type(col)}')

    for fq in sorted({r for r in rel.values() if r.count(".") == 2}):
        db, schema, _ = fq.split(".")
        try:
            fs.connect(database=db, schema=schema).close()
        except Exception:  # noqa: BLE001
            pass
    for uid, fq in rel.items():
        if uid in runnable or fq.count(".") != 2 or fq in by_rel:
            continue
        n = nodes[uid]
        if n["resource_type"] == "seed":
            try:  # seeds are data files: let DuckDB read the header and infer types
                duck.execute(f"create or replace table {fq} as select * from read_csv_auto(?) limit 0",
                             [str(project_dir / n["original_file_path"])])
                continue
            except Exception:  # noqa: BLE001
                pass
        make_stub(fq)

    trees: dict[str, exp.Expression | None] = {}

    def tree(uid: str):
        if uid not in trees:
            try:
                trees[uid] = sqlglot.parse_one(sql_of[uid], read="snowflake")
            except Exception:  # noqa: BLE001
                trees[uid] = None
        return trees[uid]

    def real_tables(node, cte_bodies, depth=0) -> set[str]:
        out = set()
        for t in node.find_all(exp.Table):
            name = t.name.upper()
            if not t.db and name in cte_bodies:
                if depth < 6:
                    out |= real_tables(cte_bodies[name], cte_bodies, depth + 1)
            elif t.catalog and t.db:
                out.add(f"{t.catalog.upper()}.{t.db.upper()}.{name}")
        return out

    def blameable(fq: str, depth=0) -> bool:
        if fq in stubs:
            return True
        uid = by_rel.get(fq)
        t = tree(uid) if uid and depth <= 8 else None
        if t is None or not t.find(exp.Star):
            return False
        tables = real_tables(t, {c.alias.upper(): c.this for c in t.find_all(exp.CTE)})
        return len(tables) == 1 and blameable(next(iter(tables)), depth + 1)

    def blame(fq: str, col: str, depth=0) -> bool:
        """Add `col` to the stub that should have provided it, following SELECT * passthroughs."""
        if fq in stubs:
            if col in stubs[fq]:
                return False
            stubs[fq].add(col)
            duck.execute(f'alter table {fq} add column if not exists "{col}" {col_type(col)}')
            return True
        uid = by_rel.get(fq)
        if uid is None or depth > 8:
            return False
        t = tree(uid)
        if t is None or not t.find(exp.Star):
            return False
        ctes = {c.alias.upper(): c.this for c in t.find_all(exp.CTE)}
        tables = real_tables(t, ctes)
        if len(tables) == 1 and blame(next(iter(tables)), col, depth + 1):
            pending.add(uid)
            return True
        return False

    def infer(uid: str, msg: str) -> bool:
        m1 = re.search(r'(?:Table|Values list) "([^"]+)" does not have a column named "([^"]+)"', msg)
        # DuckDB reports a missing column that shares its name with a SELECT alias differently
        m2 = re.search(r'Referenced column "([^"]+)" not found in FROM clause', msg) or \
            re.search(r'Referenced column "?([^"\s]+)"? not found in FROM clause', msg) or \
            re.search(r'Column "([^"]+)" referenced that exists in the SELECT clause', msg)
        if not (m1 or m2):
            return False
        t = tree(uid)
        if t is None:
            return False
        ctes = {c.alias.upper(): c.this for c in t.find_all(exp.CTE)}
        col = (m1.group(2) if m1 else m2.group(1)).upper()
        candidates: set[str] = set()
        if m1:
            alias = m1.group(1).upper()
            for tb in t.find_all(exp.Table):
                if tb.alias_or_name.upper() != alias:
                    continue
                if not tb.db and tb.name.upper() in ctes:
                    candidates |= real_tables(ctes[tb.name.upper()], ctes)
                elif tb.catalog and tb.db:
                    candidates.add(f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}")
            if not candidates and alias in ctes:
                candidates = real_tables(ctes[alias], ctes)
        if not candidates:
            candidates = real_tables(t, ctes)
        if len(candidates) > 1:  # tables built by earlier models have fixed columns; only stubs can be missing one
            candidates = {c for c in candidates if blameable(c)}
        return len(candidates) == 1 and blame(next(iter(candidates)), col)

    order = list(graphlib.TopologicalSorter({u: {d for d in dep[u] if d in dep} for u in runnable}).static_order())
    if os.environ.get("COMPAT_DEBUG"):  # debug: run only matching models
        order = [u for u in order if os.environ["COMPAT_DEBUG"] in u]
    status: dict[str, tuple[str, str]] = {}
    pending: set[str] = set()
    for n_pass in range(1, max_passes + 1):
        before = sum(len(v) for v in stubs.values())
        pending.clear()
        for uid in order:
            fq = rel[uid]
            if fq.count(".") != 2:
                status[uid] = ("skipped", "relation is not database.schema.name")
                continue
            stmt = f"create or replace table {fq} as (\n{sql_of[uid]}\n)"
            stubs.pop(fq, None)
            if os.environ.get("COMPAT_TRACE"):
                print(f"  {time.strftime('%H:%M:%S')} {uid}", file=sys.stderr, flush=True)
            fast: list[str] | None = None
            for _ in range(2000):
                try:
                    if fast is None:
                        cur.execute(stmt)  # the path dbt takes
                        status[uid] = ("ok", "")
                        break
                    for part in fast:  # same DuckDB SQL, without re-running the transpiler per inferred column
                        cur._duck_conn.execute(part)
                    fast = None
                    continue
                except Exception as e:  # noqa: BLE001
                    msg = str(e)
                    if os.environ.get("COMPAT_DEBUG") and os.environ["COMPAT_DEBUG"] in uid:
                        print(f"  DEBUG try {uid}: {msg[:160]!r}", file=sys.stderr, flush=True)
                    if infer(uid, msg):
                        if fast is None:
                            try:
                                parsed = sqlglot.parse_one(emulator.rewrite(stmt), read="snowflake")
                                fast = [cur._transform(p, None).sql(dialect="duckdb") for p in cur._transform_explode(parsed)]
                            except Exception:  # noqa: BLE001
                                fast = None
                        continue
                    reason = _norm(re.sub(r"^\d+ \(\w+\): ", "", msg))
                    if os.environ.get("COMPAT_DEBUG") and os.environ["COMPAT_DEBUG"] in uid:
                        print(f"  DEBUG {uid}: {msg[:700]}", file=sys.stderr, flush=True)
                    if re.search(r"does not have a column named|not found in FROM clause|exists in the SELECT clause", msg):
                        status[uid] = ("unknown", "could not infer which source owns a column: " + re.sub(r'"[^"]*"', "X", reason)[:60])
                    elif re.search(r"(Table|Schema|Catalog) with name .* does not exist|Referenced table .* not found", msg):
                        status[uid] = ("unknown", "references a relation dbt does not know about")
                    elif STUB_NOISE.search(reason):
                        status[uid] = ("stub_type", reason)
                    else:
                        status[uid] = ("fail", reason)
                    make_stub(fq)  # let downstream models carry on against a stub
                    break
        added = sum(len(v) for v in stubs.values()) - before
        counts = collections.Counter(s for s, _ in status.values())
        print(f"pass {n_pass}: {dict(counts)}  (+{added} inferred columns)", flush=True)
        if not added and not pending:
            break

    n = len(order)
    counts = collections.Counter(s for s, _ in status.values())
    reasons = {k: collections.Counter(r for s, r in status.values() if s == k).most_common(60) for k in ("fail", "stub_type", "unknown")}
    print(f"\n{n} models: " + "  ".join(f"{k}={v} ({100 * v / n:.1f}%)" for k, v in counts.most_common()))
    for k in ("fail", "stub_type"):
        print(f"-- {k}")
        for reason, c in reasons[k][:25]:
            print(f"   {c:5d}  {reason}")
    if json_path:
        Path(json_path).write_text(json.dumps({
            "models": n, "counts": counts, "reasons": reasons,
            "status": {uid: list(v) for uid, v in status.items() if v[0] != "ok"}}, indent=1))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("render")
    r.add_argument("project_dir", type=Path)
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--target", default="prod")
    r.add_argument("--threads", type=int, default=8)
    m = sub.add_parser("measure")
    m.add_argument("specs", nargs="+", help="name=dir of rendered .sql files")
    m.add_argument("--json")
    m.add_argument("--examples", type=int, default=3)
    e = sub.add_parser("execute")
    e.add_argument("project_dir", type=Path)
    e.add_argument("rendered", type=Path)
    e.add_argument("--json")
    e.add_argument("--passes", type=int, default=5)
    e.add_argument("--typed-stubs", action="store_true", help="guess TIMESTAMP for date-looking column names")
    a = ap.parse_args()
    if a.cmd == "render":
        a.out.mkdir(parents=True, exist_ok=True)
        render(a.project_dir.resolve(), a.out.resolve(), a.target, a.threads)
    elif a.cmd == "execute":
        execute(a.project_dir.resolve(), a.rendered.resolve(), a.json, a.passes, a.typed_stubs)
    else:
        measure(a.specs, a.json, a.examples)


if __name__ == "__main__":
    main()
