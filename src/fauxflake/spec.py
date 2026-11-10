"""The spec: a small, reviewable description of what the source data looks like.

It contains aggregates only (volumes, quantiles, category frequencies, lateness),
never rows. `fauxflake profile` measures it from Snowflake; humans can edit it.
"""

from __future__ import annotations

import math
import os
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

DAY = 86400


@dataclass
class Table:
    name: str  # DB.SCHEMA.TABLE
    raw: dict
    columns: dict[str, dict] = field(default_factory=dict)

    @property
    def ident(self) -> str:
        return self.name.replace(".", "_")

    def role(self, gen: str) -> str | None:
        return next((c for c, d in self.columns.items() if d.get("gen") == gen), None)

    @property
    def key(self) -> tuple[str, dict]:
        for col, d in self.columns.items():
            if d.get("gen") in ("sequence", "hash_id"):
                return col, d
        raise ValueError(f"{self.name}: needs one column with gen: sequence or gen: hash_id")

    @property
    def lifecycle(self) -> dict | None:
        col = self.role("lifecycle")
        return self.columns[col] if col else None

    @property
    def lateness(self) -> dict:
        return self.raw.get("lateness") or {"p": [0, 1], "seconds": [0, 0]}

    def horizon_days(self, scenario=None) -> int:
        """How far back a row's event day can be and still produce a new arrival today."""
        seconds = max(self.lateness["seconds"]) + 700  # + redelivery delay of duplicates
        if scenario:
            seconds = seconds * max(1.0, scenario.lateness_factor) + scenario.outage_hours * 3600
        for step in (self.lifecycle or {}).get("steps", []):
            seconds += max(step["delay"]["seconds"])
        return math.ceil(seconds / DAY) + 1

    def calendar(self, start: date, end: date, seed: int, scale: float, scenario=None) -> list[tuple[date, int, int]]:
        """(day, cumulative rows before day, rows on day) for every event day in [start, end]."""
        vol = self.raw.get("volume", {})
        base = float(vol.get("rows_per_day", 1000))
        if self.raw.get("scale", True):
            base *= scale
        weekly = vol.get("weekly") or [1.0] * 7
        growth = float(vol.get("growth_per_year", 0.0))
        noise = float(vol.get("noise", 0.0))
        anchor = _date(vol.get("as_of", start))
        out, cum, day = [], 0, start
        while day <= end:
            rng = random.Random(f"{seed}:{self.name}:{day.isoformat()}")
            level = base * (1 + growth) ** ((day - anchor).days / 365.0) * weekly[day.weekday()]
            n = max(1, round(level * max(0.0, 1 + noise * rng.gauss(0, 1))))
            if scenario and scenario.start <= day <= scenario.end:
                n = round(n * scenario.volume_factor)
            out.append((day, cum, n))
            cum += n
            day += timedelta(days=1)
        return out


@dataclass
class Spec:
    seed: int
    start: date
    tables: dict[str, Table]
    dbt: dict
    path: Path | None = None
    udfs: dict[str, str] = field(default_factory=dict)  # name -> "(args) as <DuckDB SQL>"

    @classmethod
    def load(cls, path: str | Path) -> "Spec":
        raw = yaml.safe_load(Path(path).read_text())
        tables = {}
        for name, t in (raw.get("tables") or {}).items():
            name = name.upper()
            if name.count(".") != 2:
                raise ValueError(f"table {name!r} must be DATABASE.SCHEMA.TABLE")
            cols = {c.upper(): dict(d or {}) for c, d in (t.get("columns") or {}).items()}
            tables[name] = Table(name=name, raw=t, columns=cols)
        spec = cls(seed=int(raw.get("seed", 1)), start=_date(raw["start"]), tables=tables,
                   dbt=raw.get("dbt") or {}, path=Path(path),
                   udfs={k.lower(): v for k, v in (raw.get("udfs") or {}).items()})
        spec.validate()
        for var, value in (spec.dbt.get("env") or {}).items():
            os.environ.setdefault(var, str(value))  # placeholders for env vars the project requires
        if (raw.get("dbt") or {}).get("target"):
            os.environ.setdefault("FAUXFLAKE_TARGET", str(raw["dbt"]["target"]))  # for `target.name` checks
        return spec

    def validate(self) -> None:
        for t in self.tables.values():
            for col, d in t.columns.items():
                if d.get("gen") == "fk":
                    if d["ref"].upper() not in self.tables:
                        raise ValueError(f"{t.name}.{col}: unknown ref {d['ref']}")
                    self.tables[d["ref"].upper()].key  # a referenced table needs a key

    def ordered(self) -> list[Table]:
        """Parents before children."""
        seen: dict[str, Table] = {}

        def visit(t: Table) -> None:
            if t.name in seen:
                return
            for d in t.columns.values():
                if d.get("gen") == "fk" and d["ref"].upper() != t.name:
                    visit(self.tables[d["ref"].upper()])
            seen[t.name] = t

        for t in self.tables.values():
            visit(t)
        return list(seen.values())


def _date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v))
