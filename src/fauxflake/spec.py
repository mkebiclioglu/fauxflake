"""The spec: a small, reviewable description of what the source data looks like.

It contains aggregates only (volumes, quantiles, category frequencies, lateness),
never rows. `fauxflake profile` measures it from Snowflake; humans can edit it.
"""

from __future__ import annotations

import math
import os
import random
import re
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
        return re.sub(r"\W", "_", self.name)

    @property
    def sql(self) -> str:
        """The name as SQL: every part quoted, so a table called GROUP or ORDER is no problem."""
        return quoted(self.name)

    def role(self, gen: str) -> str | None:
        return next((c for c, d in self.columns.items() if d.get("gen") == gen), None)

    @property
    def key(self) -> tuple[str, dict]:
        for col, d in self.columns.items():
            if d.get("gen") in ("sequence", "hash_id"):
                return col, d
        raise ValueError(f"{self.name}: needs one column with gen: sequence or gen: hash_id")

    def grain_sizes(self) -> dict[str, int]:
        """The columns of the table's `grain` that take their values in turn, with how many each has: listed values,
        a pool's, or the things a table of readings is about (`of: 20` on its key: twenty servers, one row each a day)."""
        out = {}
        for c in self.raw.get("grain") or []:
            d = self.columns.get(c, {})
            if d.get("gen") == "categorical" and d.get("values"):
                out[c] = len(d["values"])
            elif d.get("gen") == "pool":
                out[c] = int(d.get("size", 200))
            elif d.get("gen") in ("sequence", "hash_id") and d.get("of"):
                out[c] = int(d["of"])
        return out

    def grain_rows(self) -> int:
        """Rows a day of a table that holds one row a period for each combination of its grain (0 when it has none)."""
        sizes = self.grain_sizes()
        if not sizes:
            return 0
        hourly = any(self.columns.get(c, {}).get("gen") in ("event_time", "time_offset") and self.columns[c].get("trunc") == "hour"
                     for c in self.raw.get("grain") or [])
        rows = 24 if hourly else 1
        for size in sizes.values():
            rows *= size
        return rows

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

    def calendar(self, start: date, end: date, seed: int, scale: float, scenario=None,
                 thin: tuple[date, float] | None = None, least: tuple[int, int] | None = None) -> list[tuple[date, int, int]]:
        """(day, cumulative rows before day, rows on day) for every event day in [start, end].
        `thin`: days before that date hold that share of the volume (the far end of a long history).
        `least`: (rows, days) - the table holds at least that many rows once that many days have passed,
        whatever the scale; what is missing is spread evenly over those days."""
        vol = self.raw.get("volume", {})
        base = float(vol.get("rows_per_day", 1000))
        if self.raw.get("scale", True):
            base *= scale
        weekly = vol.get("weekly") or [1.0] * 7
        growth = float(vol.get("growth_per_year", 0.0))
        noise = float(vol.get("noise", 0.0))
        anchor = _date(vol.get("as_of", start))
        out, cum, day, carry = [], 0, start, 0.0
        full = self.grain_rows()  # one row a period for each combination, whatever the scale: no day leaves one out
        # a table that another took over from, or that took over (`until_day`, `from_day`: days of the history):
        # it has rows only on the days it was in use
        opened, closed = int(self.raw.get("from_day") or 0), self.raw.get("until_day")
        first = start + timedelta(days=opened)
        window = first + timedelta(days=max(1, least[1]) - 1) if least else None
        last = max(end, window) if window else end
        while day <= last:
            rng = random.Random(f"{seed}:{self.name}:{day.isoformat()}")
            level = base * (1 + growth) ** ((day - anchor).days / 365.0) * weekly[day.weekday()]
            if thin and day < thin[0]:
                level *= thin[1]
            if day < first or (closed is not None and (day - start).days >= int(closed)):
                n = 0
            elif base < 1 or level < 1:
                # less than a row a day (a long history of a small table): a row every few days, the first on day one
                carry += level * max(0.0, 1 + noise * rng.gauss(0, 1))
                n = int(carry) or (1 if day == first else 0)
                carry -= int(carry)
                n = max(n, full)
            else:
                n = max(1, full, round(level * max(0.0, 1 + noise * rng.gauss(0, 1))))
            out.append((day, 0, n))
            day += timedelta(days=1)
        if least:
            days = max(1, least[1]) if closed is None else max(1, min(least[1], int(closed) - opened))
            short = least[0] - sum(n for _, _, n in out[opened:opened + days])
            if short > 0:
                out[opened:opened + days] = [(d, 0, n + (i + 1) * short // days - i * short // days)
                                             for i, (d, _, n) in enumerate(out[opened:opened + days])]
        done, cum = [], 0
        for day, _, n in out:
            if day > end:
                break
            if scenario and scenario.start <= day <= scenario.end:
                n = round(n * scenario.volume_factor)
            done.append((day, cum, n))
            cum += n
        return done


def quoted(name: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


@dataclass
class Spec:
    seed: int
    start: date
    tables: dict[str, Table]
    dbt: dict
    path: Path | None = None
    udfs: dict[str, str] = field(default_factory=dict)  # name -> "(args) as <DuckDB SQL>"
    # relations the project reads before it builds them (hard-coded names): DB.SCHEMA.NAME -> {columns: {COL: type}}
    existing: dict[str, dict] = field(default_factory=dict)
    history: int | None = None  # days of history present at the start, when the project's own date filters need more than the default
    recent: int | None = None  # the last days of that history that hold the full volume ...
    older: float | None = None  # ... and the share of it the days before them hold (a long history costs what a short one does)
    least_rows: int | None = None  # every table holds at least this many rows when a run begins, whatever the scale
    # the account around the databases, as SHOW lists it: {warehouses: [...], resource_monitors: [...], parameters: {...}}
    account: dict = field(default_factory=dict)

    @property
    def least(self) -> tuple[int, int] | None:
        return (self.least_rows, self.history or 60) if self.least_rows else None

    @property
    def thin(self) -> tuple[date, float] | None:
        if not (self.history and self.recent and self.older) or self.recent >= self.history:
            return None
        return self.start + timedelta(days=self.history - self.recent), float(self.older)

    @classmethod
    def load(cls, path: str | Path) -> "Spec":
        return cls.from_raw(yaml.safe_load(Path(path).read_text()), path)

    @classmethod
    def from_raw(cls, raw: dict, path: str | Path) -> "Spec":
        """A spec from its parsed YAML; `path` is where relative paths in it start from."""
        tables = {}
        for name, t in (raw.get("tables") or {}).items():
            name = name.upper()
            if name.count(".") != 2:
                raise ValueError(f"table {name!r} must be DATABASE.SCHEMA.TABLE")
            cols = {c.upper(): dict(d or {}) for c, d in (t.get("columns") or {}).items()}
            tables[name] = Table(name=name, raw=t, columns=cols)
        spec = cls(seed=int(raw.get("seed", 1)), start=_date(raw["start"]), tables=tables,
                   dbt=raw.get("dbt") or {}, path=Path(path),
                   udfs={k.lower(): v for k, v in (raw.get("udfs") or {}).items()},
                   existing={k.upper(): dict(v or {}) for k, v in (raw.get("existing") or {}).items()},
                   history=int(raw["history"]) if raw.get("history") else None,
                   recent=int(raw["recent"]) if raw.get("recent") else None,
                   older=float(raw["older"]) if raw.get("older") else None,
                   least_rows=int(raw["least_rows"]) if raw.get("least_rows") else None,
                   account=dict(raw.get("account") or {}))
        spec.validate()
        for var, value in (spec.dbt.get("env") or {}).items():
            os.environ.setdefault(var, str(value))  # placeholders for env vars the project requires
        from . import objects

        # cloud storage read by COPY INTO, served from local directories: `locations: {"s3://bucket/x": ./fixtures/x}`
        objects.LOCATIONS = {str(k): str((Path(path).parent / str(v)).resolve()) for k, v in (raw.get("locations") or {}).items()}
        # tables read back in time (AT, BEFORE) whose earlier states are kept from the first statement: `time_travel: [DB.S.T]`
        from . import history
        history.keep([str(t) for t in raw.get("time_travel") or []])
        # tests that state facts about production data the generated data does not share
        os.environ["FAUXFLAKE_EXPECTED_FAILURES"] = " ".join(spec.dbt.get("expected_failures") or [])
        # dbt commands run before each build: a project whose `dbt seed` makes its own sources (a seed's post-hook)
        os.environ["FAUXFLAKE_FIRST"] = "\n".join(str(c) for c in spec.dbt.get("first") or [])
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
        entered: set[str] = set()  # two tables that reference each other: a key is a function of the row number, so either order works
        pile: list[tuple[Table, bool]] = [(t, False) for t in reversed(list(self.tables.values()))]
        while pile:
            t, done = pile.pop()
            if done:
                seen.setdefault(t.name, t)
                continue
            if t.name in seen or t.name in entered:
                continue
            entered.add(t.name)
            pile.append((t, True))
            for d in t.columns.values():
                if d.get("gen") == "fk" and d["ref"].upper() != t.name:
                    pile.append((self.tables[d["ref"].upper()], False))
        return list(seen.values())


def _date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v))
