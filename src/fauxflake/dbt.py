"""Run dbt in-process against the emulator, with a generated profile.

The dbt project is used untouched: fauxflake writes its own profiles.yml that
points the project's profile at the local endpoint through `dbt-snowflake`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import yaml


class DbtProject:
    def __init__(self, project_dir: str | Path, port: int, database: str, threads: int = 4):
        self.dir = Path(project_dir).resolve()
        self.port, self.database = port, database.upper()
        project = yaml.safe_load((self.dir / "dbt_project.yml").read_text())
        self.profiles_dir = self.dir / ".fauxflake"
        self.profiles_dir.mkdir(exist_ok=True)
        profile = {
            project["profile"]: {
                "target": "fauxflake",
                "outputs": {
                    "fauxflake": {
                        "type": "snowflake",
                        "account": "fauxflake",
                        "user": "ci",
                        "password": "ci",
                        "host": "127.0.0.1",
                        "port": port,
                        "protocol": "http",
                        "database": self.database,
                        "schema": "{{ env_var('FAUXFLAKE_SCHEMA', 'CI') }}",
                        "warehouse": "LOCAL",
                        "threads": threads,
                    }
                },
            }
        }
        (self.profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profile, sort_keys=False))
        (self.profiles_dir / ".gitignore").write_text("*\n")

    def invoke(self, args: list[str], schema: str = "CI", quiet: bool = False) -> bool:
        from dbt.cli.main import dbtRunner

        os.environ["FAUXFLAKE_SCHEMA"] = schema
        os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        cmd = (["--quiet"] if quiet else []) + list(args) + [
            "--project-dir", str(self.dir), "--profiles-dir", str(self.profiles_dir), "--target", "fauxflake",
        ]
        return bool(dbtRunner().invoke(cmd).success)

    def incremental_models(self) -> list[dict]:
        manifest = json.loads((self.dir / "target" / "manifest.json").read_text())
        return [
            n for n in manifest["nodes"].values()
            if n["resource_type"] == "model"
            and n["config"]["materialized"] == "incremental"
            and (n["config"].get("meta") or {}).get("fauxflake_drift", True)
        ]
