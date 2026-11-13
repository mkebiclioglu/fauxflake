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

import json
import re
import zlib
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import duckdb

from . import emulator, objects
from .spec import Spec, Table, quoted


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
    new_column: str | None = None


SCENARIOS: dict[str, dict] = {
    "late-spike": {"lateness_factor": 4.0},      # a backlog: everything arrives four times later
    "duplicate-storm": {"duplicate_rate": 0.3},  # a replayed topic
    "null-surge": {"null_rate": 0.3},            # an upstream release drops fields
    "volume-spike": {"volume_factor": 10.0},     # launch day
    "empty-day": {"volume_factor": 0.0},         # nothing was produced
    "loader-outage": {"outage_hours": 30.0},     # the loader is down, then delivers everything at once
    "new-column": {"new_column": "ADDED_UPSTREAM"},  # the source grows a column overnight
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

    base = "g"  # what a column's randomness is keyed on: the row, or another column's value (`follows`)

    def salt(self, name: str) -> str:
        if self.base == "g":
            return f"{self.spec.seed}:{self.t.name}:{name}"
        # keyed on an entity's id: the same id must give the same attributes in every table
        return f"{self.spec.seed}:entity:{name.split('.')[-1].lower()}"

    def u(self, salt: str, *extra: str) -> str:
        args = "".join(f", {e}" for e in extra)
        # hashed twice: DuckDB combines the arguments of one hash() so lightly that two columns' draws for
        # the same row moved together (a third of the combinations of two categories never occurred)
        return f"((hash(hash({self.base}, {lit(self.salt(salt))}{args})) >> 11)::double / 9007199254740992.0)"

    def key_expr(self, table: Table, index: str) -> str:
        _col, d = table.key
        named = d.get("named") or []
        if named:
            # ids the project's SQL asks for by name (`where server_id = '93myk...'`): the first rows have them
            numeric = d["gen"] == "sequence"
            arms = " ".join(f"when {i} then {int(v) if numeric else lit(str(v))}" for i, v in enumerate(named))
            return f"(case {index} {arms} else {self._key_expr(table, d, index)} end)"
        return self._key_expr(table, d, index)

    def _key_expr(self, table: Table, d: dict, index: str) -> str:
        if d["gen"] == "sequence":
            # each table counts in its own range, as ids from different systems do: two tables'
            # keys never collide when a model unions them
            # (`domain` puts two tables that are keyed by the same id back in one range)
            offset = (zlib.crc32((d.get("domain") or table.name).upper().encode()) % 900 + 100) * 1_000_000_000
            return f"({index} + {offset + 1})::BIGINT"
        return f"md5(concat({lit(f'{self.spec.seed}:{(d.get('domain') or table.name).upper()}:')}, {index}))"

    def column(self, col: str, d: dict) -> str:
        return f'{self.expr(col, d)} as "{col}"'

    @staticmethod
    def path_segments(path: str) -> tuple[str, ...]:
        """`chunk.transactions[].hash` -> (chunk, transactions, [], hash): `[]` stands for "each element"."""
        out: list[str] = []
        for part in path.split("."):
            # `args~b64~json[]`: the key, then how its value is written (base64 of JSON text), then "each element"
            for piece in re.findall(r"~\w+|\[\]|[^~\[]+|\[", part):
                out.append(piece)
        return tuple(out)

    def _object(self, col: str, fields: dict[str, dict], keyed_by: str | None = None) -> str:
        """`keyed_by` names the field that identifies the entity the object describes (a user's
        id): the other fields are then a function of it, so the same id always has the same name.
        A path through `[]` describes the elements of an array: `items[].sku` is a list of objects."""
        key_expr = f"({self.expr(f'{col}.{keyed_by}', fields[keyed_by])})" if keyed_by in fields else None
        tree: dict = {}
        segments = {path: self.path_segments(path) for path in fields}
        nested = {s[:i] for s in segments.values() for i in range(1, len(s))}
        for path, d in fields.items():
            keys = segments[path]
            if keys and keys in nested and keys + ("~json",) in nested | set(segments.values()) and (
                    d.get("templates") or d.get("gen") == "categorical"):
                # text that some models match by pattern and others parse as JSON: some rows hold the one, some the other
                node = tree
                for key in keys:
                    node = node.setdefault(key, {})
                    if not isinstance(node, dict):
                        break
                else:
                    node["~text"] = ("leaf", path, d)
                continue
            if not keys or keys in nested:
                continue  # `header` next to `header.height`: the children say what the object holds
            node = tree
            for key in keys[:-1]:
                node = node.setdefault(key, {})
                if not isinstance(node, dict):
                    break
            else:
                node[keys[-1]] = ("leaf", path, d)

        def leaf(value: tuple) -> str:
            if key_expr and value[1] != keyed_by and self.base == "g":
                self.base = key_expr
                try:
                    return self._expr(f"{col}.{value[1]}", value[2])
                finally:
                    self.base = "g"
            return self.expr(f"{col}.{value[1]}", value[2])

        def build(node, where: str = "", depth: int = 0) -> str:  # noqa: ANN001
            if isinstance(node, tuple):
                return leaf(node)
            if isinstance(node.get("~json"), dict) and any(not key.startswith("~") for key in node):
                # read by key and also handed to PARSE_JSON: an object (parsing one gives it back), with the keys of both
                node = {**node["~json"], **{key: value for key, value in node.items() if key != "~json"}}
            for mark in ("~b64", "~json"):
                if mark in node:
                    # text holding JSON, or base64 of what is under it: what PARSE_JSON / BASE64_DECODE read back
                    inner = build(node[mark], where + mark, depth)
                    made = f"to_json({inner})::varchar" if mark == "~json" else f"to_base64(encode(({inner})::varchar))"
                    if "~text" in node:
                        return f"(case when {self.u(f'{col}.{where}:text')} < 0.5 then ({leaf(node['~text'])})::varchar else {made} end)"
                    return made
            if "[]" in node and len(node) > 1 and isinstance(node["[]"], dict):
                # flattened and read by key (`{"FunctionCall": {...}}`, `{"Transfer": {...}}`): an object with one
                # of the keys the SQL names, holding what the elements hold. One key each, as a value that is
                # one kind of thing or another is written; FLATTEN then gives one row for it, not one per kind.
                shape = node["[]"]
                kinds = {key: ({**shape, **value} if isinstance(value, dict) else shape) for key, value in node.items() if key != "[]"}
                pick = f"floor({self.u(f'{col}.{where}:kind')} * {len(kinds)})::bigint"
                arms = " ".join(f"when {i} then json_object({lit(key)}, {build(value, f'{where}.{key}', depth)})"
                                for i, (key, value) in enumerate(kinds.items()))
                return f"(case {pick} {arms} end)"
            if "[]" in node and len(node) > 1:
                node = {key: value for key, value in node.items() if key != "[]"}
            if "[]" in node:
                # one to three elements, each with its own values: what is flattened always gives rows
                count = f"floor({self.u(f'{col}.{where}:len')} * 3)::bigint"
                outer, index = self.base, f"_i{depth}"
                self.base = f"hash({outer}, {index})"
                try:
                    inner = build(node["[]"], where + "[]", depth + 1)
                finally:
                    self.base = outer
                return f"list_transform(range(1 + {count}), lambda {index}: {inner})"
            always, sometimes = [], []
            for key, value in node.items():
                part = f"{lit(key)}, {build(value, f'{where}.{key}', depth)}"
                rate = value[2].get("optional") if isinstance(value, tuple) else None
                if rate:  # a key the SQL tests for being there: present in some documents, absent in the others
                    sometimes.append(f"case when {self.u(f'{col}.{where}.{key}:there')} < {float(rate)!r} then '{{}}'::json "
                                     f"else json_object({part}) end")
                else:
                    always.append(part)
            made = f"json_object({', '.join(always)})"
            return f"json_merge_patch({made}, {', '.join(sometimes)})" if sometimes else made

        if not tree:
            return "'{}'::JSON"
        return f"to_json({build(tree)})::JSON" if "[]" in tree else f"{build(tree)}::JSON"

    def expr(self, col: str, d: dict) -> str:
        """SQL for one column. With `follows: OTHER`, the value is a function of OTHER's value
        instead of the row, so an id and its attributes stay consistent across rows."""
        other = d.get("follows")
        if other and self.base == "g":
            self.base = f"({self.expr(other, self.t.columns[other])})"
            try:
                return self._expr(col, d)
            finally:
                self.base = "g"
        return self._expr(col, d)

    def _expr(self, col: str, d: dict) -> str:
        gen = d.get("gen")
        if gen == "hash_id" and d.get("domain") and "." in col:
            # an id inside a document: one per element, in a range of its own that `link` values point into
            expr = f"md5(concat({lit(f'{self.spec.seed}:{d['domain']}:')}, {self.base}))"
        elif gen in ("sequence", "hash_id"):
            expr = self.key_expr(self.t, "g")
        elif gen == "link":
            # the id of another row's element: of one of the first rows, or (ids of the same table, which form
            # a hierarchy) of a row a little earlier and one level up, so that chains end and never loop
            draw = self.u(col)
            depth, slot = int(d.get("depth", 0)), int(d.get("slot", 0))
            if d.get("layered"):
                # the row before, one level up, and only from the first element of a list: an id has at most one
                # row referring to it through this field, as a child has one parent. The rest point nowhere.
                inside = re.findall(r"_i\d+", self.base)
                first = "".join(f" and {index} = 0" for index in dict.fromkeys(inside))
                target = f"(case when g % 3 > 0{first} then g - 1 else -1 - g * 16{f' - {inside[-1]}' if inside else ''} end)"
            else:
                target = f"floor({draw} * {int(d.get('first', 50))})::bigint"
            for level in range(depth):
                # the element of the row it points at: each referring field has its own (`slot`), so two fields
                # never name the same id
                target = f"hash({target}, {slot if level == depth - 1 else 0})"
            expr = f"md5(concat({lit(f'{self.spec.seed}:{d['domain']}:')}, {target}))"
        elif gen == "fk":
            parent = self.spec.tables[d["ref"].upper()]
            skew = float(d.get("skew", 1.0))
            idx = f"floor(pow({self.u(col)}, {skew!r}) * greatest(_pc_{col}, 1))::bigint"
            if d.get("first"):  # inside a document: one of the parent's first rows
                idx = f"floor({self.u(col)} * {int(d['first'])})::bigint"
            expr = self.key_expr(parent, idx)
        elif gen == "categorical":
            total = sum(d["values"].values())
            x, arms, acc = self.u(col), [], 0.0
            for value, weight in d["values"].items():
                acc += weight / total
                arms.append(f"when {x} < {acc!r} then {lit(value)}")
            expr = f"case {' '.join(arms)} else {lit(list(d['values'])[-1])} end"
        elif gen == "pool":
            # one of `size` values shared by every column that names the same pool: joins between them match
            n = f"floor({self.u(col)} * {int(d.get('size', 200))})::bigint"
            expr = f"({n} + 1)" if d.get("numeric") else f"concat({lit(str(d['pool']) + '_')}, {n})"
        elif gen == "dist":
            expr = piecewise(self.u(col), d["p"], d["v"])
            if d.get("unique") and self.base == "g":
                expr = "(g + 1)"  # a `unique` test covers it: one value per row
            if "round" in d:
                expr = f"round({expr}, {int(d['round'])})"
        elif gen == "template":
            def fill(template: str) -> str:
                parts = []
                for i, part in enumerate(d.get("parts") or []):
                    template = template.replace("{" + str(i) + "}", f"\0part{i}\0")
                for chunk in template.replace("{n}", "\0n\0").replace("{hex}", "\0hex\0").split("\0"):
                    if chunk.startswith("part") and chunk[4:].isdigit() and int(chunk[4:]) < len(d.get("parts") or []):
                        # another generator's value inside the text: an id a model parses back out of it
                        parts.append(f"({self._expr(f'{col}:{chunk}', d['parts'][int(chunk[4:])])})::varchar")
                    elif chunk == "n":
                        parts.append(f"({self.base} + 1)::varchar" if self.base == "g" else f"({self.base})::varchar")
                    elif chunk == "hex":
                        parts.append(f"substr(md5(concat({lit(self.salt(col) + ':')}, {self.base})), 1, 12)")
                    elif chunk:
                        parts.append(lit(chunk))
                return f"concat({', '.join(parts)})"

            shapes = d.get("templates") or [d["template"]]  # several shapes: each row takes one
            pick = f"floor({self.u(col + ':shape')} * {len(shapes)})::bigint"
            expr = fill(shapes[0]) if len(shapes) == 1 else \
                f"case {pick} {' '.join(f'when {i} then {fill(t)}' for i, t in enumerate(shapes))} end"
        elif gen == "time_offset":
            expr = f"_ev + {ms(piecewise(self.u(col), d['p'], d['seconds']))}"
            if d.get("unique") and self.base == "g":
                expr = "(timestamp '2000-01-01' + to_days(g::integer))"  # a `unique` test covers it: one day per row
            if d.get("epoch"):  # the moment as seconds since 1970, the way APIs often send it
                expr = f"epoch({expr})::bigint"
            if d.get("format"):  # a text column holding dates in this format
                # nanoseconds are written as microseconds and three zeros: DuckDB's strftime pads %n with NUL bytes
                expr = f"strftime({expr}, {lit(d['format'].replace('%n', '%f000'))})"
        elif gen == "ip":
            octet = lambda salt: f"(floor({self.u(col + ':' + salt)} * 250)::bigint + 1)"  # noqa: E731
            expr = f"concat({octet('a')}, '.', {octet('b')}, '.', {octet('c')}, {lit('.0/24') if d.get('cidr') else f'concat(chr(46), {octet(chr(100))})'})"
        elif gen == "object":
            expr = self._object(col, d["fields"], d.get("keyed_by"))
        elif gen == "array":
            n = f"floor({self.u(col)} * 4)::bigint"
            if d.get("fields"):  # an array of objects: {name: number | text}
                pairs = ", ".join(f"{lit(name)}, " + ("i + 1" if str(kind) == "number" else f"concat({lit(str(name) + '_')}, i)")
                                  for name, kind in d["fields"].items())
                expr = f"to_json(list_transform(range(1 + {n}), lambda i: json_object({pairs})))::JSON"
            elif d.get("values"):  # elements drawn from a known list
                known = "[" + ", ".join(lit(v) for v in d["values"]) + "]"
                expr = f"to_json(list_slice({known}, 1, {n}))::JSON"
            else:
                expr = f"to_json(list_transform(range({n}), lambda i: concat({lit(col.lower() + '_')}, i)))::JSON"
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
        if d.get("trunc") and gen in ("event_time", "time_offset", "updated_at") and not d.get("format") and not d.get("epoch"):
            expr = f"date_trunc({lit(str(d['trunc']))}, {expr})"  # reported per hour (or day): on the boundary
        rate = repr(float(d.get("null_rate") or 0.0))
        if self.sc and self.sc.null_rate is not None and gen in ("fk", "categorical", "dist", "pool", "template", "time_offset", "object", "array"):
            rate = self.during(repr(self.sc.null_rate), rate)
        if rate != "0.0":
            expr = f"case when {self.u(col + ':null')} < {rate} then NULL else {expr} end"
        if d.get("type"):
            expr = f"({expr})::{d['type']}"
        return expr

    def select(self, with_new_column: bool = False) -> str:
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
        if with_new_column:
            since = f"timestamp '{self.sc.start} 00:00:00'"
            cols += f',\n  case when _loaded >= {since} then \'v2\' end as "{self.sc.new_column}"'
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


def existing_view(name: str, columns: dict[str, str], replace: bool = False, table: bool = False) -> str:
    """An empty relation standing for one that exists before the run (the spec's `existing:`).
    A table once its full shape is known, so an incremental model finds what it would in production."""
    name = quoted(name)
    if table:
        return f"create table if not exists {name} ({', '.join(f'"{col}" {kind}' for col, kind in columns.items())})"
    cols = ", ".join(f'null::{kind} as "{col}"' for col, kind in columns.items()) or 'null::varchar as "_FX_EMPTY"'
    return f"create {'or replace view' if replace else 'view if not exists'} {name} as select {cols} where false"


class Warehouse:
    """The synthetic sources at a point in simulated time."""

    def __init__(self, emu: emulator.Emulator, spec: Spec, scale: float = 1.0, scenario: Scenario | None = None,
                 freeze_clock: bool = True):
        self.emu, self.spec, self.scale, self.scenario = emu, spec, scale, scenario
        self.freeze_clock = freeze_clock
        emulator.define_udfs(emu, spec.udfs)
        if spec.dbt.get("role"):  # the role policies see: `dbt: {role: ANALYST}`
            objects.set_role(emu.duck, str(spec.dbt["role"]))
        self.cur: duckdb.DuckDBPyConnection = emu.cursor()
        self.now: datetime | None = None

    def _calendars(self, until: datetime, only: set[str] | None = None) -> dict[str, list[tuple[date, int, int]]]:
        cals = {}
        for t in self.spec.tables.values():
            if only is not None and t.name not in only:
                continue
            cal = t.calendar(self.spec.start, until.date(), self.spec.seed, self.scale, self.scenario)
            self.cur.execute(f"create or replace temp table cal_{t.ident} (day date, cum bigint, n bigint)")
            self.cur.executemany(f"insert into cal_{t.ident} values (?, ?, ?)", cal)
            cals[t.name] = cal
        return cals

    def advance_to(self, until: datetime, only: set[str] | None = None) -> dict[str, int]:
        """Move the clock forward, loading every row that arrives in (now, until].
        `only` limits a first load to those tables (the rest are already there)."""
        if self.now and until <= self.now:
            raise ValueError("the clock only moves forward")
        cals = self._calendars(until, only)
        counts = {}
        for t in self.spec.ordered():
            if only is not None and t.name not in only:
                continue
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
            sc = self.scenario
            changed_at = datetime.combine(sc.start, datetime.min.time()) if sc and sc.new_column else None
            widened = bool(changed_at and until > changed_at)
            if widened and self.now is not None and self.now <= changed_at:
                self.cur.execute(f'alter table {t.sql} add column "{sc.new_column}" varchar')
            select = TableSQL(self.spec, t, sc).select(with_new_column=widened)
            verb = f"create or replace table {t.sql} as" if self.now is None else f"insert into {t.sql} by name"
            counts[t.name] = self.cur.execute(f"{verb} {select}", params).fetchone()[0]
            if t.raw.get("mode") == "upsert":
                self._keep_latest(t)
        if self.now is None:
            for name, rel in self.spec.existing.items():
                db, schema, _ = name.split(".")
                self.emu.ensure_database(db, schema)
                kind = self.cur.execute("select table_type from information_schema.tables where table_catalog = ? and "
                                        "table_schema = ? and table_name = ?", [db, schema, name.split(".")[2]]).fetchone()
                if kind is None:  # the project has not built it yet in this warehouse
                    emulator.stand_in(self.cur, name, existing_view(name, rel.get("columns") or {}, table=rel.get("as") == "table"))
        self.now = until
        if self.emu.db_path:  # `fauxflake query` reads the clock back from here
            (Path(self.emu.db_path) / "fauxflake.json").write_text(json.dumps({"now": until.isoformat()}))
        if self.freeze_clock:
            emulator.FROZEN_NOW = until
        return counts

    def _keep_latest(self, t: Table) -> None:
        """An upsert-style source holds one row per key: the newest version that has arrived."""
        key = t.key[0]
        order = ", ".join(f'"{c}" desc' for c in (t.role("updated_at"), t.role("loaded_at")) if c) or "rowid desc"
        self.cur.execute(f"""
            delete from {t.sql} where rowid in (
              select rowid from (select rowid, row_number() over (partition by "{key}" order by {order}) rn from {t.sql})
              where rn > 1)""")

    def row_counts(self) -> dict[str, int]:
        return {t: self.cur.execute(f"select count(*) from {quoted(t)}").fetchone()[0] for t in self.spec.tables}
