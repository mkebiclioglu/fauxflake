"""Run dbt in-process against the emulator, with a generated profile.

The dbt project is used untouched: fauxflake writes its own profiles.yml that
points the project's profile at the local endpoint through `dbt-snowflake`.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import yaml


def emulator_output(port: int, database: str, threads: int = 4) -> dict:
    return {
        "type": "snowflake", "account": "fauxflake", "user": "ci", "password": "ci",
        "host": "127.0.0.1", "port": port, "protocol": "http",
        "database": database.upper(), "schema": "{{ env_var('FAUXFLAKE_SCHEMA', 'CI') }}",
        "warehouse": "LOCAL", "threads": threads,
    }


def snowflake_output(connection_name: str, database: str, threads: int = 4) -> dict:
    """A dbt target for a real account, from ~/.snowflake/connections.toml (key-pair auth)."""
    import tomllib

    conn = tomllib.loads((Path.home() / ".snowflake" / "connections.toml").read_text())[connection_name]
    out = {
        "type": "snowflake", "account": conn["account"], "user": conn["user"],
        "database": database.upper(), "schema": "{{ env_var('FAUXFLAKE_SCHEMA', 'CI') }}",
        "threads": threads,
    }
    for src, dst in (("role", "role"), ("warehouse", "warehouse"), ("private_key_file", "private_key_path")):
        if conn.get(src):
            out[dst] = conn[src]
    return out


def _tolerate_exposures() -> None:
    """When an on-run-start hook fails, dbt marks every node as skipped and reads `node.schema` on
    each, which exposures do not have: the run dies with an AttributeError that hides the hook's
    error. Give exposures an empty schema so dbt reports what actually failed."""
    try:
        from dbt.contracts.graph.nodes import Exposure
    except ImportError:
        return
    if not hasattr(Exposure, "schema"):
        Exposure.schema = ""


def _close_connections() -> None:
    """dbt keeps a session open for the next command. Its temporary tables go when it ends, and the
    emulator it talks to may be gone by the time dbt closes it on its own: end it now."""
    try:
        from dbt.adapters.factory import cleanup_connections

        cleanup_connections()
    except Exception:  # noqa: BLE001, S110 - nothing was open
        pass


class DbtProject:
    def __init__(self, project_dir: str | Path, output: dict, name: str = "emulator"):
        self.dir = Path(project_dir).resolve()
        self.last_error = ""
        self._local = output.get("host") == "127.0.0.1"
        self._databases_ready = False
        project = yaml.safe_load((self.dir / "dbt_project.yml").read_text())
        self.profiles_dir = self.dir / ".fauxflake" / name
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self.target_path = self.dir / "target" / name
        # projects that branch on `target.name` can pick the name: `dbt: {target: ci}` in the spec
        self.target = os.environ.get("FAUXFLAKE_TARGET") or "fauxflake"
        profile = {project["profile"]: {"target": self.target, "outputs": {self.target: output}}}
        (self.profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profile, sort_keys=False))
        (self.dir / ".fauxflake" / ".gitignore").write_text("*\n")
        self._default_selector = False
        for name in ("selectors.yml", "selectors.yaml"):
            if (self.dir / name).exists():
                try:
                    selectors = (yaml.safe_load((self.dir / name).read_text()) or {}).get("selectors") or []
                    self._default_selector = any(str(sel.get("default", "")).strip().lower() not in ("", "false", "none") for sel in selectors)
                except yaml.YAMLError:
                    pass
        declared = any((self.dir / f).exists() for f in ("packages.yml", "dependencies.yml"))
        if declared and not (self.dir / "dbt_packages").exists():
            self.deps()  # so a fresh checkout works without a separate step

    def deps(self) -> bool:
        from dbt.cli.main import dbtRunner

        return bool(dbtRunner().invoke(["--quiet", "--no-version-check", "deps", "--project-dir", str(self.dir)]).success)

    def _create_databases(self) -> None:
        """On Snowflake the databases a project writes to already exist. Create them here."""
        from dbt.cli.main import dbtRunner

        from . import emulator

        self._databases_ready = True
        flags = ["--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir),
                 "--target", self.target, "--target-path", str(self.target_path)]
        if emulator.CURRENT is None or not dbtRunner().invoke(["--quiet", "--no-version-check", "parse", *flags]).success:
            return
        manifest = json.loads((self.target_path / "manifest.json").read_text())
        for node in [*manifest["nodes"].values(), *manifest["sources"].values()]:
            names = [str(node.get(k) or "").strip('"') for k in ("database", "schema")]
            if all(re.fullmatch(r"[\w$]+", n) for n in names):  # skips names dbt has not rendered yet
                emulator.CURRENT.ensure_database(*names)

    def invoke(self, args: list[str], schema: str = "CI", quiet: bool = False, silent: bool = False) -> bool:
        from dbt.cli.main import dbtRunner

        os.environ["FAUXFLAKE_SCHEMA"] = schema
        if self._local and not self._databases_ready and args and args[0] in ("build", "run", "seed", "snapshot", "test"):
            self._create_databases()
        os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        args = list(args)
        if self._local and args and args[0] in ("build", "run", "test") and self._default_selector \
                and not any(a in ("-s", "--select", "--selector", "-m", "--models") for a in args):
            # the project's default selector assumes a warehouse that already holds part of the project
            # (views built by another job, say). This one starts empty: build everything.
            args += ["--select", "fqn:*"]
        expected = os.environ.get("FAUXFLAKE_EXPECTED_FAILURES", "").split()
        if expected and args and args[0] in ("build", "test"):
            # tests known to fail on generated data (dbt.expected_failures in the spec) are left out by name
            at = args.index("--exclude") + 1 if "--exclude" in args else None
            args = args[:at] + expected + args[at:] if at else [*args, "--exclude", *expected]
        # the project may pin another dbt version than the one fauxflake ships: its SQL is what is under test
        cmd = ["--no-version-check"] + (["--log-level", "none"] if silent else ["--quiet"] if quiet else []) + args + [
            "--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir), "--target", self.target,
            "--target-path", str(self.target_path),
        ]
        _tolerate_exposures()
        result = dbtRunner().invoke(cmd)
        _close_connections()
        self.last_error = str(result.exception) if result.exception else ""
        return bool(result.success)

    def compile(self) -> dict:
        """Compile every model that compiles and return the manifest with their SQL. A model whose
        Jinja needs relations that do not exist yet fails alone instead of stopping the rest."""
        from dbt.cli.main import dbtRunner
        from dbt.task.compile import CompileTask

        os.environ["FAUXFLAKE_SCHEMA"] = "CI"
        os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        saved = CompileTask.raise_on_first_error, CompileTask._mark_dependent_errors
        CompileTask.raise_on_first_error = lambda self: False
        CompileTask._mark_dependent_errors = lambda self, *a, **k: None  # compiling needs no upstream
        try:
            result = dbtRunner().invoke([
                "--no-version-check", "--log-level", "none", "compile", "--select", "resource_type:model", "resource_type:snapshot",
                "--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir), "--target", self.target,
                "--target-path", str(self.target_path)])
        finally:
            CompileTask.raise_on_first_error, CompileTask._mark_dependent_errors = saved
            _close_connections()
        self.last_error = str(result.exception) if result.exception else ""
        manifest = json.loads((self.target_path / "manifest.json").read_text())
        for r in getattr(result.result, "results", None) or []:
            code = getattr(r.node, "compiled_code", None)
            node = manifest["nodes"].get(r.node.unique_id)
            if node is None:
                continue
            if code:
                node["compiled_code"] = code
            if str(r.status) in ("error", "RunStatus.Error") or not code:
                node["compile_error"] = str(r.message or "")
                node["compiled_code"] = None
        return manifest

    def models(self) -> list[dict]:
        manifest = json.loads((self.target_path / "manifest.json").read_text())
        return [n for n in manifest["nodes"].values()
                if n["resource_type"] == "model" and n["config"]["materialized"] != "ephemeral"]

    def snapshots(self) -> list[dict]:
        manifest = json.loads((self.target_path / "manifest.json").read_text())
        return [n for n in manifest["nodes"].values() if n["resource_type"] == "snapshot"]

    def incremental_models(self) -> list[dict]:
        return [
            n for n in self.models()
            if n["config"]["materialized"] == "incremental"
            and (n["config"].get("meta") or {}).get("fauxflake_drift", True)
        ]
