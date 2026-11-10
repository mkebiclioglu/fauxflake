"""Deterministic synthetic sources.

Every value is a pure function of (seed, table, column, row number). That one
property gives everything else:

* reproducible CI: the same spec always yields the same bytes;
* foreign keys without joins: a child picks a parent row *number* and applies the
  parent's key function;
* time travel: the warehouse at simulated time T is just
  `universe WHERE _loaded_at <= T`, so advancing the clock is an INSERT of the
  rows with `T_prev < _loaded_at <= T`. Late-arriving rows fall out naturally:
  they are rows whose event time is old but whose load time is in the window.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import duckdb

from . import emulator
from .spec import Spec, Table


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def piecewise(u: str, p: list[float], v: list[float]) -> str:
    """Inverse-CDF sampling: linear interpolation between (probability, value) knots."""
    if len(p) != len(v) or len(p) < 2:
        raise ValueError("distribution needs matching p and value lists with at least 2 knots")
    arms = []
    for i in range(1, len(p)):
        width = (p[i] - p[i - 1]) or 1e-12
        arms.append(f"when {u} < {p[i]!r} then {v[i-1]!r} + ({u} - {p[i-1]!r}) / {width!r} * {(v[i] - v[i-1])!r}")
    return f"(case {' '.join(arms)} else {v[-1]!r} end)"


def ms(seconds_expr: str) -> str:
    return f"to_milliseconds(floor(({seconds_expr}) * 1000)::bigint)"


@dataclass
class Scenario:
    """A bad day. Part of the deterministic universe, so the clock still behaves."""

    name: str
    start: date  # first and last affected event day
    end: date
    lateness_factor: float = 1.0
    duplicate_rate: float | None = None
    null_rate: float | None = None
    volume_factor: float = 1.0
    outage_hours: float = 0.0


SCENARIOS: dict[str, dict] = {
    "late-spike": {"lateness_factor": 4.0},      # a backlog: everything arrives four times later
    "duplicate-storm": {"duplicate_rate": 0.3},  # a replayed topic
    "null-surge": {"null_rate": 0.3},            # an upstream release drops fields
    "volume-spike": {"volume_factor": 10.0},     # launch day
    "empty-day": {"volume_factor": 0.0},         # nothing was produced
    "loader-outage": {"outage_hours": 30.0},     # the loader is down, then delivers everything at once
}


def scenario(name: str, first_day: date, days: int = 2) -> Scenario:
    return Scenario(name=name, start=first_day, end=first_day + timedelta(days=days - 1), **SCENARIOS[name])


class TableSQL:
    def __init__(self, spec: Spec, table: Table, scenario: Scenario | None = None):
        self.spec, self.t, self.sc = spec, table, scenario

    def during(self, bad: str, normal: str) -> str:
        """`bad` for rows whose event day is inside the scenario, `normal` otherwise."""
        if not self.sc:
            return normal
        return f"(case when day between date '{self.sc.start}' and date '{self.sc.end}' then {bad} else {normal} end)"

    def u(self, salt: str, *extra: str) -> str:
        args = "".join(f", {e}" for e in extra)
        return f"((hash(g, {lit(f'{self.spec.seed}:{self.t.name}:{salt}')}{args}) >> 11)::double / 9007199254740992.0)"

    def key_expr(self, table: Table, index: str) -> str:
        _col, d = table.key
        if d["gen"] == "sequence":
            return f"({index} + 1)::BIGINT"
        return f"md5(concat({lit(f'{self.spec.seed}:{table.name}:')}, {index}))"

    def column(self, col: str, d: dict) -> str:
        gen = d.get("gen")
        if gen in ("sequence", "hash_id"):
            expr = self.key_expr(self.t, "g")
        elif gen == "fk":
            parent = self.spec.tables[d["ref"].upper()]
            skew = float(d.get("skew", 1.0))
            idx = f"floor(pow({self.u(col)}, {skew!r}) * greatest(_pc_{col}, 1))::bigint"
            expr = self.key_expr(parent, idx)
        elif gen == "categorical":
            total = sum(d["values"].values())
            x, arms, acc = self.u(col), [], 0.0
            for value, weight in d["values"].items():
                acc += weight / total
                arms.append(f"when {x} < {acc!r} then {lit(value)}")
            expr = f"case {' '.join(arms)} else {lit(list(d['values'])[-1])} end"
        elif gen == "dist":
            expr = piecewise(self.u(col), d["p"], d["v"])
            if "round" in d:
                expr = f"round({expr}, {int(d['round'])})"
        elif gen == "template":
            parts = []
            for i, chunk in enumerate(d["template"].replace("{n}", "\0n\0").replace("{hex}", "\0hex\0").split("\0")):
                if chunk == "n":
                    parts.append("(g + 1)::varchar")
                elif chunk == "hex":
                    parts.append(f"substr(md5(concat({lit(f'{self.spec.seed}:{self.t.name}:{col}:')}, g)), 1, 12)")
                elif chunk:
                    parts.append(lit(chunk))
            expr = f"concat({', '.join(parts)})"
        elif gen == "time_offset":
            expr = f"_ev + {ms(piecewise(self.u(col), d['p'], d['seconds']))}"
        elif gen == "event_time":
            expr = "_ev"
        elif gen == "updated_at":
            expr = "_ts"
        elif gen == "loaded_at":
            expr = "_loaded"
        elif gen == "lifecycle":
            states = [d["initial"]] + [s["to"] for s in d["steps"]]
            expr = f"case k {' '.join(f'when {i} then {lit(s)}' for i, s in enumerate(states))} end"
        elif gen == "constant":
            expr = lit(d.get("value"))
        else:
            raise ValueError(f"{self.t.name}.{col}: unknown gen {gen!r}")
        rate = repr(float(d.get("null_rate") or 0.0))
        if self.sc and self.sc.null_rate is not None and gen in ("fk", "categorical", "dist", "template", "time_offset"):
            rate = self.during(repr(self.sc.null_rate), rate)
        if rate != "0.0":
            expr = f"case when {self.u(col + ':null')} < {rate} then NULL else {expr} end"
        if d.get("type"):
            expr = f"({expr})::{d['type']}"
        return f'{expr} as "{col}"'

    def select(self) -> str:
        """SELECT over global row numbers [$lo, $hi) keeping rows loaded in ($after, $until]."""
        t = self.t
        fks = [(c, d) for c, d in t.columns.items() if d.get("gen") == "fk"]
        joins = "".join(
            f" left join cal_{self.spec.tables[d['ref'].upper()].ident} pc_{c} on pc_{c}.day = c.day" for c, d in fks
        )
        pcs = "".join(f", coalesce(pc_{c}.cum, 0) as _pc_{c}" for c, _ in fks)

        intraday = t.raw.get("intraday")
        second_of_day = piecewise(self.u("ev"), intraday["p"], [h * 3600 for h in intraday["hours"]]) if intraday \
            else f"{self.u('ev')} * 86400"

        steps = (t.lifecycle or {}).get("steps", [])
        reached, times, where = [], ["_ev"], ["k.k = 0"]
        cond, offset = [], []
        for i, step in enumerate(steps, start=1):
            cond.append(f"{self.u(f'step{i}')} < {float(step.get('p', 1.0))!r}")
            offset.append(piecewise(self.u(f"delay{i}"), step["delay"]["p"], step["delay"]["seconds"]))
            reached.append(f", ({' and '.join(cond)}) as _r{i}, _ev + {ms(' + '.join(offset))} as _t{i}")
            times.append(f"_t{i}")
            where.append(f"(k.k = {i} and _r{i})")
        ts = "case k.k " + " ".join(f"when {i} then {x}" for i, x in enumerate(times)) + " end"

        late = piecewise(self.u("late", "k"), t.lateness["p"], t.lateness["seconds"])
        dup_rate = repr(float(t.raw.get("duplicate_rate", 0.0)))
        arrived = f"_l0 + {ms(f"dup.d * (1 + {self.u('redeliver', 'k')} * 600)")}"
        if self.sc:
            late = f"{late} * {self.during(repr(self.sc.lateness_factor), '1.0')}"
            if self.sc.duplicate_rate is not None:
                dup_rate = self.during(repr(self.sc.duplicate_rate), dup_rate)
            if self.sc.outage_hours:
                down = f"timestamp '{self.sc.start} 00:00:00'"
                up = f"({down} + {ms(repr(self.sc.outage_hours * 3600))})"
                arrived = f"(select case when x >= {down} and x < {up} then {up} else x end from (select {arrived} as x))"
        cols = ",\n  ".join(self.column(c, d) for c, d in t.columns.items())
        return f"""
with b as (
  select r.g, c.day{pcs}
  from range($lo, $hi) r(g)
  asof join (select * from cal_{t.ident} where n > 0) c on r.g >= c.cum{joins}
),
e as (select *, day::timestamp + {ms(second_of_day)} as _ev from b),
s as (select *{''.join(reached)} from e),
v as (select s.*, k.k, {ts} as _ts from s cross join range({len(steps) + 1}) k(k) where {' or '.join(where)}),
l as (select *, _ts + {ms(late)} as _l0 from v),
d as (
  select l.*, {arrived} as _loaded
  from l cross join range(2) dup(d)
  where dup.d = 0 or {self.u('dup', 'k')} < {dup_rate}
)
select
  {cols}
from d
where _loaded > $after and _loaded <= $until"""


class Warehouse:
    """The synthetic sources at a point in simulated time."""

    def __init__(self, emu: emulator.Emulator, spec: Spec, scale: float = 1.0, scenario: Scenario | None = None,
                 freeze_clock: bool = True):
        self.emu, self.spec, self.scale, self.scenario = emu, spec, scale, scenario
        self.freeze_clock = freeze_clock
        self.cur: duckdb.DuckDBPyConnection = emu.cursor()
        self.now: datetime | None = None

    def _calendars(self, until: datetime) -> dict[str, list[tuple[date, int, int]]]:
        cals = {}
        for t in self.spec.tables.values():
            cal = t.calendar(self.spec.start, until.date(), self.spec.seed, self.scale, self.scenario)
            self.cur.execute(f"create or replace temp table cal_{t.ident} (day date, cum bigint, n bigint)")
            self.cur.executemany(f"insert into cal_{t.ident} values (?, ?, ?)", cal)
            cals[t.name] = cal
        return cals

    def advance_to(self, until: datetime) -> dict[str, int]:
        """Move the clock forward, loading every row that arrives in (now, until]."""
        if self.now and until <= self.now:
            raise ValueError("the clock only moves forward")
        cals = self._calendars(until)
        counts = {}
        for t in self.spec.ordered():
            db, schema, _ = t.name.split(".")
            cal = cals[t.name]
            if self.now is None:
                self.emu.ensure_database(db, schema)
                first = 0
            else:
                first_day = self.now.date() - timedelta(days=t.horizon_days(self.scenario))
                first = max(0, (first_day - self.spec.start).days)
            params = {
                "lo": cal[first][1],
                "hi": cal[-1][1] + cal[-1][2],
                "after": self.now or datetime(1, 1, 1),
                "until": until,
            }
            select = TableSQL(self.spec, t, self.scenario).select()
            verb = f"create or replace table {t.name} as" if self.now is None else f"insert into {t.name}"
            counts[t.name] = self.cur.execute(f"{verb} {select}", params).fetchone()[0]
        self.now = until
        if self.freeze_clock:
            emulator.FROZEN_NOW = until
        return counts

    def row_counts(self) -> dict[str, int]:
        return {t: self.cur.execute(f"select count(*) from {t}").fetchone()[0] for t in self.spec.tables}
