"""Run dbt in-process against the emulator, with a generated profile.

The dbt project is used untouched: fauxflake writes its own profiles.yml that
points the project's profile at the local endpoint through `dbt-snowflake`.
"""

from __future__ import annotations

import json
import os
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
        declared = any((self.dir / f).exists() for f in ("packages.yml", "dependencies.yml"))
        if declared and not (self.dir / "dbt_packages").exists():
            self.deps()  # so a fresh checkout works without a separate step

    def deps(self) -> bool:
        from dbt.cli.main import dbtRunner

        return bool(dbtRunner().invoke(["--quiet", "deps", "--project-dir", str(self.dir)]).success)

    def _create_databases(self) -> None:
        """On Snowflake the databases a project writes to already exist. Create them here."""
        from dbt.cli.main import dbtRunner

        from . import emulator

        self._databases_ready = True
        flags = ["--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir),
                 "--target", self.target, "--target-path", str(self.target_path)]
        if emulator.CURRENT is None or not dbtRunner().invoke(["--quiet", "parse", *flags]).success:
            return
        manifest = json.loads((self.target_path / "manifest.json").read_text())
        for node in [*manifest["nodes"].values(), *manifest["sources"].values()]:
            if node.get("database") and node.get("schema"):
                emulator.CURRENT.ensure_database(node["database"].strip('"'), node["schema"].strip('"'))

    def invoke(self, args: list[str], schema: str = "CI", quiet: bool = False) -> bool:
        from dbt.cli.main import dbtRunner

        os.environ["FAUXFLAKE_SCHEMA"] = schema
        if self._local and not self._databases_ready and args and args[0] in ("build", "run", "seed", "snapshot", "test"):
            self._create_databases()
        os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        cmd = (["--quiet"] if quiet else []) + list(args) + [
            "--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir), "--target", self.target,
            "--target-path", str(self.target_path),
        ]
        result = dbtRunner().invoke(cmd)
        self.last_error = str(result.exception) if result.exception else ""
        return bool(result.success)

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
