from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta

from pathlib import Path

import click

from . import __version__
from .spec import Spec


def _start(spec: Spec, history: int) -> datetime:
    return datetime.combine(spec.start, datetime.min.time()) + timedelta(days=history)


common = [
    click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True, help="Source data spec."),
    click.option("--project-dir", default=".", show_default=True, help="dbt project directory."),
    click.option("--scale", default=0.001, show_default=True, help="Fraction of production volume to generate."),
    click.option("--history", default=60, show_default=True, help="Days of history present at the start."),
    click.option("--db-path", default=None, help="Keep data in DuckDB files here instead of memory (for large scales)."),
    click.option("--keep", is_flag=True, help="Keep the warehouse in the project's .fauxflake/warehouse, for `fauxflake query`."),
]


def _warehouse_dir(project_dir, db_path, keep):  # noqa: ANN001, ANN202
    from pathlib import Path

    return db_path or (str(Path(project_dir) / ".fauxflake" / "warehouse") if keep else None)


def with_common(f):
    for opt in reversed(common):
        f = opt(f)
    return f


def emulator_temp(path: str) -> None:
    """A scratch warehouse to remove even when the run is cut short."""
    from . import emulator

    emulator.TEMP_DIRS.append(path)


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Run dbt-snowflake CI on synthetic data, locally, on DuckDB."""
    import signal
    import sys

    def stop(*_: object) -> None:  # a cancelled CI job: leave through the normal exit, so temporary files go
        import os
        import shutil
        import threading
        import time

        def give_up() -> None:  # a statement that will not return must not keep the process alive
            from . import emulator

            time.sleep(20)
            for path in emulator.TEMP_DIRS:
                shutil.rmtree(path, ignore_errors=True)
            os._exit(143)

        threading.Thread(target=give_up, daemon=True).start()
        if hasattr(signal, "SIGALRM"):  # and should the interpreter itself hang while shutting down, the kernel ends it
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.alarm(45)
        sys.exit(143)

    try:
        signal.signal(signal.SIGTERM, stop)
    except ValueError:  # not the main thread (the test runner)
        pass


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def build(spec_path, project_dir, scale, history, db_path, keep, dbt_args):
    """Generate sources and run `dbt build` once. Extra args go to dbt."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .generate import Warehouse

    import contextlib
    import tempfile

    spec = Spec.load(spec_path)
    with contextlib.ExitStack() as stack:
        if db_path is None:
            # on disk, as `init` fitted it: a model too large for memory spills instead of failing. Removed afterwards.
            db_path = stack.enter_context(tempfile.TemporaryDirectory(prefix="fauxflake-"))
            emulator_temp(db_path)
        emu = stack.enter_context(serve(db_path=db_path))
        t0 = time.time()
        wh = Warehouse(emu, spec, scale)
        counts = wh.advance_to(_start(spec, history))
        click.echo(f"generated {sum(counts.values()):,} rows in {time.time() - t0:.1f}s")
        expected = spec.dbt.get("expected_failures") or []
        if expected:
            click.echo(f"leaving out {len(expected)} test(s) listed under dbt.expected_failures in {spec_path}")
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        ok = project.invoke(["build", *dbt_args])
    if not ok:
        click.echo("\nto look at the data behind a failure: " + ("fauxflake query \"select ...\"" if keep else
                   "run again with --keep, then fauxflake query \"select ...\" (or --model <name>)"), err=True)
    sys.exit(0 if ok else 1)


@main.command(name="dbt", context_settings={"ignore_unknown_options": True})
@with_common
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def dbt_command(spec_path, project_dir, scale, history, db_path, keep, dbt_args):
    """Run any dbt command against the kept warehouse: `fauxflake dbt test -s my_model`.

    The warehouse of the last `build --keep` is reused as it is, so one model or one test runs in
    the time it takes itself. Without a kept warehouse, sources are generated first."""
    from pathlib import Path

    from . import emulator
    from .dbt import DbtProject, emulator_output
    from .emulator import define_udfs, serve
    from .generate import Warehouse

    if not dbt_args:
        raise click.ClickException("which dbt command? For example: fauxflake dbt test -s my_model")
    spec = Spec.load(spec_path)
    db_path = _warehouse_dir(project_dir, db_path, True)
    kept = bool(list(Path(db_path).glob("*.db")))
    with serve(db_path=db_path, keep=kept) as emu:
        if kept:
            define_udfs(emu, spec.udfs)
            for file in Path(db_path).glob("*.db"):
                emu.ensure_database(file.stem, "CI")
            state = Path(db_path) / "fauxflake.json"
            if state.exists():
                emulator.FROZEN_NOW = datetime.fromisoformat(json.loads(state.read_text())["now"])
        else:
            counts = Warehouse(emu, spec, scale).advance_to(_start(spec, history))
            click.echo(f"generated {sum(counts.values()):,} rows")
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        ok = project.invoke(list(dbt_args))
    sys.exit(0 if ok else 1)


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.option("--days", default=7, show_default=True, help="Simulated days to advance.")
@click.option("--step-hours", default=24.0, show_default=True, help="Hours between dbt runs.")
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def simulate(spec_path, project_dir, scale, history, db_path, keep, days, step_hours, dbt_args):
    """Advance the clock run by run, then compare incremental models to a full refresh."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .generate import Warehouse
    from .simulate import simulate as run

    spec = Spec.load(spec_path)
    with serve(db_path=db_path) as emu:
        wh = Warehouse(emu, spec, scale)
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        drifts = run(wh, project, _start(spec, history), days, step_hours, list(dbt_args), log=click.echo)
    click.echo("")
    for d in drifts:
        mark = "ok   " if d.ok else "DRIFT"
        if d.model.endswith("(snapshot)"):
            click.echo(f"{mark} {d.model:<32} versions={d.incremental_rows:,} "
                       f"keys_with_several_current_rows={d.only_incremental:,} overlapping_versions={d.only_rebuilt:,}")
            continue
        click.echo(f"{mark} {d.model:<32} incremental={d.incremental_rows:,} rebuilt={d.rebuilt_rows:,} "
                   f"only_incremental={d.only_incremental:,} only_rebuilt={d.only_rebuilt:,}")
    if not all(d.ok for d in drifts):
        click.echo("\nIncremental models differ from a full refresh after simulated time passed.", err=True)
        sys.exit(1)


@main.command()
@with_common
@click.option("--port", default=8765, show_default=True)
def serve(spec_path, project_dir, scale, history, db_path, keep, port):
    """Keep the emulator running with generated sources, for interactive dbt use."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    from .dbt import DbtProject, emulator_output
    from .emulator import serve as start
    from .generate import Warehouse

    spec = Spec.load(spec_path)
    with start(port=port, db_path=db_path) as emu:
        wh = Warehouse(emu, spec, scale)
        counts = wh.advance_to(_start(spec, history))
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        click.echo(f"generated {sum(counts.values()):,} rows; simulated now = {wh.now}")
        click.echo(f"dbt build --project-dir {project.dir} --profiles-dir {project.profiles_dir} --target fauxflake")
        click.echo("Ctrl-C to stop.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass


@main.command()
@click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True)
@click.option("--connection", default=None, help="Name in ~/.snowflake/connections.toml (default connection if omitted).")
@click.option("--days", default=90, show_default=True, help="Trailing window of event days to measure.")
@click.option("--sample-pct", default=None, type=float, help="Block-sample this percentage of each table.")
@click.option("--k-anonymity", default=50, show_default=True, help="Smallest group whose value may be recorded.")
@click.option("--max-categories", default=50, show_default=True)
def profile(spec_path, connection, days, sample_pct, k_anonymity, max_categories):
    """Measure aggregate statistics from Snowflake and write them into the spec."""
    import snowflake.connector

    from .profile import profile_spec

    conn = snowflake.connector.connect(connection_name=connection) if connection else snowflake.connector.connect()
    changed = profile_spec(spec_path, conn, days=days, sample_pct=sample_pct,
                           k_anonymity=k_anonymity, max_categories=max_categories, log=click.echo)
    click.echo(f"updated {spec_path}: {', '.join(changed) or 'nothing'}")


@main.command()
@with_common
@click.option("--connection", required=True, help="Name in ~/.snowflake/connections.toml.")
@click.option("--days", default=2, show_default=True, help="Simulated days of incremental runs on both engines.")
def parity(spec_path, project_dir, scale, history, db_path, keep, connection, days):
    """Build on the emulator and on real Snowflake from identical synthetic data, and compare."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    import snowflake.connector

    from .dbt import DbtProject, emulator_output, snowflake_output
    from .emulator import serve
    from .generate import Warehouse
    from .parity import run

    spec = Spec.load(spec_path)
    database = spec.dbt.get("database", "ANALYTICS")
    conn = snowflake.connector.connect(connection_name=connection)
    with serve() as emu:
        wh = Warehouse(emu, spec, scale)
        local = DbtProject(project_dir, emulator_output(emu.port, database))
        remote = DbtProject(project_dir, snowflake_output(connection, database), name="snowflake")
        results = run(conn, wh, local, remote, _start(spec, history), days, log=click.echo)
    click.echo("")
    for r in results:
        mark = "same " if r.ok else "DIFF "
        click.echo(f"{mark} {r.model:<28} emulator={r.emulator_rows:,} snowflake={r.snowflake_rows:,} "
                   f"only_emulator={r.only_emulator:,} only_snowflake={r.only_snowflake:,} {r.note}")
    sys.exit(0 if all(r.ok for r in results) else 1)


@main.command()
@click.option("--connection", default=None, help="Record answers from this Snowflake connection into golden.json.")
def conformance(connection):
    """Score the emulator against answers recorded from real Snowflake."""
    import snowflake.connector

    from . import conformance as c
    from .emulator import serve

    if connection:
        recorded = c.record(snowflake.connector.connect(connection_name=connection))
        click.echo(f"recorded {len(recorded)} cases from Snowflake")
    expected, passed, failed = c.golden(), 0, []
    with serve() as emu:
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="CONFORMANCE", schema="PUBLIC"))
        for case in c.cases():
            actual = c.run(conn, case["sql"])
            if c.matches(expected[case["id"]], actual):
                passed += 1
            else:
                failed.append((case, actual))
    for case, actual in failed:
        click.echo(f"DIFF {case['id']}\n  snowflake: {json.dumps(expected[case['id']])[:300]}\n  emulator:  {json.dumps(actual)[:300]}")
    click.echo(f"\n{passed}/{passed + len(failed)} cases match Snowflake")


@main.command()
@click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True)
@click.option("--project-dir", default=".", show_default=True)
@click.option("--db-path", default=None, help="Where the run kept its data. Default: the project's .fauxflake/warehouse (see --keep).")
@click.option("--schema", default="CI", show_default=True)
@click.option("--limit", default=50, show_default=True, help="Rows to print.")
@click.option("-f", "--file", "sql_file", default=None, help="Read the SQL from a file ('-' for standard input).")
@click.option("-m", "--model", default=None, help="Run the compiled SQL of this model, as the last build wrote it.")
@click.argument("sql", required=False)
def query(spec_path, project_dir, db_path, schema, limit, sql_file, model, sql):
    """Run Snowflake SQL against the warehouse a previous run kept (`build --keep` or --db-path).

    The clock stands where that run left it, so CURRENT_DATE means what it meant to the models."""
    import os
    from pathlib import Path

    import snowflake.connector

    from . import emulator
    from .emulator import define_udfs
    from .emulator import serve as serve_emulator

    db_path = db_path or str(Path(project_dir) / ".fauxflake" / "warehouse")
    if not list(Path(db_path).glob("*.db")):
        raise click.ClickException(f"no warehouse in {db_path}: run `fauxflake build --keep` first")
    if model:
        found = [p for p in (Path(project_dir) / "target" / "emulator" / "compiled").rglob(f"{model}.sql")]
        if not found:
            raise click.ClickException(f"no compiled SQL for model {model!r}: build the project first")
        sql = found[0].read_text()
    elif sql_file:
        sql = sys.stdin.read() if sql_file == "-" else Path(sql_file).read_text()
    if not sql or not sql.strip():
        raise click.ClickException("nothing to run: pass SQL, --file or --model")
    spec = Spec.load(spec_path) if os.path.exists(spec_path) else None
    database = (spec.dbt.get("database") if spec else None) or "ANALYTICS"
    with serve_emulator(db_path=db_path, keep=True) as emu:
        if spec:
            define_udfs(emu, spec.udfs)
        for file in Path(db_path).glob("*.db"):
            emu.ensure_database(file.stem, schema)
        state = Path(db_path) / "fauxflake.json"
        if state.exists():  # the simulated time of the run that built this warehouse
            emulator.FROZEN_NOW = datetime.fromisoformat(json.loads(state.read_text())["now"])
        conn = snowflake.connector.connect(**emu.connect_kwargs(database=database, schema=schema.upper()))
        cur = conn.cursor()
        try:
            cur.execute(sql.strip().rstrip(";"))
        except snowflake.connector.errors.ProgrammingError as e:
            raise click.ClickException(str(e)) from e
        names = [d[0] for d in cur.description or []]
        rows = [["" if v is None else str(v) for v in row] for row in cur.fetchmany(limit)]
        more = cur.fetchone() is not None
        conn.close()
    widths = [min(60, max(len(n), *(len(r[i]) for r in rows))) if rows else len(n) for i, n in enumerate(names)]
    click.echo("  ".join(n.ljust(w) for n, w in zip(names, widths)))
    click.echo("  ".join("-" * w for w in widths))
    for row in rows:
        click.echo("  ".join(v[:60].ljust(w) for v, w in zip(row, widths)))
    click.echo(f"({len(rows)} row{'s' if len(rows) != 1 else ''}{', more not shown' if more else ''})")


@main.command()
@click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True)
@click.option("--project-dir", default=".", show_default=True)
@click.option("--database", default="ANALYTICS", show_default=True, help="Database dbt builds into.")
@click.option("--force", is_flag=True, help="Overwrite an existing spec.")
@click.option("--fit/--no-fit", default=True, show_default=True,
              help="Try the spec out (run the models, build the project) and learn from what fails.")
def init(spec_path, project_dir, database, force, fit):
    """Write a spec from the dbt project itself, then fit it until the project builds.
    Needs no Snowflake access."""
    from pathlib import Path

    import os

    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .init import production_target, write_spec

    path = Path(spec_path)
    if path.exists() and not force:
        raise click.ClickException(f"{path} already exists (use --force to overwrite)")
    import tempfile

    if not os.environ.get("FAUXFLAKE_TARGET") and production_target(project_dir):
        os.environ["FAUXFLAKE_TARGET"] = production_target(project_dir)

    # on disk, so that a project with thousands of models is bounded by DuckDB's buffer pool, not by RAM
    with tempfile.TemporaryDirectory(prefix="fauxflake-") as scratch, serve(db_path=scratch if fit else None) as emu:
        emulator_temp(scratch)
        notes = write_spec(DbtProject(project_dir, emulator_output(emu.port, database)), path, database, emu,
                           fit=fit, log=click.echo)
    spec = Spec.load(path)
    click.echo(f"wrote {path}: {len(spec.tables)} source tables, "
               f"{sum(len(t.columns) for t in spec.tables.values())} columns")
    for note in notes:
        click.echo(f"  note: {note}")
    click.echo("next: fauxflake build")


@main.command()
@click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True)
@click.option("--project-dir", default=".", show_default=True)
def fit(spec_path, project_dir):
    """Bring the spec up to date with the project. Keeps what you edited.

    New sources and columns the models read are added, then the project is run on generated
    data and the spec learns from what fails, as `init` does the first time."""
    from pathlib import Path

    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .init import update_spec

    path = Path(spec_path)
    if not path.exists():
        raise click.ClickException(f"{path} does not exist: run `fauxflake init` first")
    import tempfile

    import os

    from .init import production_target

    database = Spec.load(path).dbt.get("database", "ANALYTICS")  # also takes the target the spec records
    if not os.environ.get("FAUXFLAKE_TARGET") and production_target(project_dir):
        os.environ["FAUXFLAKE_TARGET"] = production_target(project_dir)  # the same target `init` fitted
    with tempfile.TemporaryDirectory(prefix="fauxflake-") as scratch, serve(db_path=scratch) as emu:
        emulator_temp(scratch)
        notes = update_spec(DbtProject(project_dir, emulator_output(emu.port, database)), path, database, emu, log=click.echo)
    spec = Spec.load(path)
    click.echo(f"updated {path}: {len(spec.tables)} source tables, {sum(len(t.columns) for t in spec.tables.values())} columns")
    for note in notes:
        click.echo(f"  note: {note}")


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.option("--base", default="origin/main", show_default=True, help="Git ref to compare the working tree against.")
@click.option("--output", default=None, help="Also write the Markdown report to this file.")
@click.option("--fail-on-change", is_flag=True, help="Exit 1 when any model output changed.")
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def diff(spec_path, project_dir, scale, history, db_path, keep, base, output, fail_on_change, dbt_args):
    """Build a base ref and the working tree on identical data and report what changed."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    from pathlib import Path

    from .diff import markdown, run
    from .emulator import serve
    from .generate import Warehouse

    spec = Spec.load(spec_path)
    with serve() as emu:
        wh = Warehouse(emu, spec, scale)
        rows = sum(wh.advance_to(_start(spec, history)).values())
        diffs = run(emu, Path(project_dir).resolve(), base, spec.dbt.get("database", "ANALYTICS"),
                    list(dbt_args), log=lambda m: click.echo(m, err=True))
    report = markdown(diffs, base, rows)
    click.echo(report)
    if output:
        Path(output).write_text(report + "\n")
    if fail_on_change and any(d.status != "same" for d in diffs):
        sys.exit(1)


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.option("--days", default=7, show_default=True, help="Simulated days per scenario.")
@click.option("--only", multiple=True, help="Scenario name; repeat to run several. Default: all.")
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def scenarios(spec_path, project_dir, scale, history, db_path, keep, days, only, dbt_args):
    """Replay bad days (late data, duplicates, nulls, spikes, outages) and report what happens."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    import contextlib
    import io

    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .generate import SCENARIOS, Warehouse, scenario
    from .simulate import simulate as run

    spec = Spec.load(spec_path)
    start = _start(spec, history)
    silent = False
    click.echo(f"{'scenario':<17} {'verdict':<16} detail")
    for name in only or SCENARIOS:
        failures: list = []
        sc = scenario(name, (start + timedelta(days=1)).date())
        with serve() as emu:
            wh = Warehouse(emu, spec, scale, sc)
            project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
            with contextlib.redirect_stdout(io.StringIO()):  # dbt's own output for failing runs
                drifts = run(wh, project, start, days, 24, list(dbt_args), log=lambda *_: None, failures=failures)
        drifted = [d.model for d in drifts if not d.ok]
        if drifted and not failures:
            verdict, detail, silent = "SILENT DRIFT", "wrong data, no test failed: " + ", ".join(drifted), True
        elif failures:
            verdict = "caught"
            detail = f"dbt build failed on {len(failures)} of {days} days" + (f"; drift in {', '.join(drifted)}" if drifted else "")
        else:
            verdict, detail = "survived", "builds passed, incrementals match a full refresh"
        click.echo(f"{name:<17} {verdict:<16} {detail}")
    if silent:
        sys.exit(1)


@main.command()
@with_common
def dev(spec_path, project_dir, scale, history, db_path, keep):
    """Rebuild on save and show how the output differs from the last commit."""
    db_path = _warehouse_dir(project_dir, db_path, keep)
    from pathlib import Path

    from .dev import DevSession
    from .emulator import serve
    from .generate import Warehouse

    spec = Spec.load(spec_path)
    with serve() as emu:
        rows = sum(Warehouse(emu, spec, scale).advance_to(_start(spec, history)).values())
        click.echo(f"generated {rows:,} source rows")
        session = DevSession(emu, Path(project_dir), spec.dbt.get("database", "ANALYTICS"), log=click.echo)
        try:
            session.start()
            session.watch()
        finally:
            session.close()


@main.command()
@click.option("--spec", "spec_path", default="fauxflake.yml", show_default=True)
@click.option("--project-dir", default=".", show_default=True)
def check(spec_path, project_dir):
    """Verify the spec still covers every source column the project reads."""
    import json

    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .init import check as find_problems
    from .init import unknown_functions

    spec = Spec.load(spec_path)
    database = spec.dbt.get("database", "ANALYTICS")
    with serve() as emu:
        project = DbtProject(project_dir, emulator_output(emu.port, database))
        project.invoke(["compile"], quiet=True)
        manifest = json.loads((project.target_path / "manifest.json").read_text())
        unknown = [name for name in unknown_functions(manifest, emu) if name not in spec.udfs]
    problems = find_problems(manifest, spec, database)
    problems += [f"{name}(): not a function the emulator knows, add a stand-in under `udfs` in the spec" for name in unknown]
    for problem in problems:
        click.echo(problem)
    click.echo(f"{len(problems)} problem{'s' if len(problems) != 1 else ''}")
    sys.exit(1 if problems else 0)
