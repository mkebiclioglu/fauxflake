from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta

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
]


def with_common(f):
    for opt in reversed(common):
        f = opt(f)
    return f


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Run dbt-snowflake CI on synthetic data, locally, on DuckDB."""


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def build(spec_path, project_dir, scale, history, dbt_args):
    """Generate sources and run `dbt build` once. Extra args go to dbt."""
    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .generate import Warehouse

    spec = Spec.load(spec_path)
    with serve() as emu:
        t0 = time.time()
        wh = Warehouse(emu, spec, scale)
        counts = wh.advance_to(_start(spec, history))
        click.echo(f"generated {sum(counts.values()):,} rows in {time.time() - t0:.1f}s")
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        ok = project.invoke(["build", *dbt_args])
    sys.exit(0 if ok else 1)


@main.command(context_settings={"ignore_unknown_options": True})
@with_common
@click.option("--days", default=7, show_default=True, help="Simulated days to advance.")
@click.option("--step-hours", default=24.0, show_default=True, help="Hours between dbt runs.")
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def simulate(spec_path, project_dir, scale, history, days, step_hours, dbt_args):
    """Advance the clock run by run, then compare incremental models to a full refresh."""
    from .dbt import DbtProject, emulator_output
    from .emulator import serve
    from .generate import Warehouse
    from .simulate import simulate as run

    spec = Spec.load(spec_path)
    with serve() as emu:
        wh = Warehouse(emu, spec, scale)
        project = DbtProject(project_dir, emulator_output(emu.port, spec.dbt.get("database", "ANALYTICS")))
        drifts = run(wh, project, _start(spec, history), days, step_hours, list(dbt_args), log=click.echo)
    click.echo("")
    for d in drifts:
        mark = "ok   " if d.ok else "DRIFT"
        click.echo(f"{mark} {d.model:<32} incremental={d.incremental_rows:,} rebuilt={d.rebuilt_rows:,} "
                   f"only_incremental={d.only_incremental:,} only_rebuilt={d.only_rebuilt:,}")
    if not all(d.ok for d in drifts):
        click.echo("\nIncremental models differ from a full refresh after simulated time passed.", err=True)
        sys.exit(1)


@main.command()
@with_common
@click.option("--port", default=8765, show_default=True)
def serve(spec_path, project_dir, scale, history, port):
    """Keep the emulator running with generated sources, for interactive dbt use."""
    from .dbt import DbtProject, emulator_output
    from .emulator import serve as start
    from .generate import Warehouse

    spec = Spec.load(spec_path)
    with start(port=port) as emu:
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
def parity(spec_path, project_dir, scale, history, connection, days):
    """Build on the emulator and on real Snowflake from identical synthetic data, and compare."""
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
