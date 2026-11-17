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
import math
import re
import zlib
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import duckdb

from . import emulator, objects
from .spec import Spec, Table, quoted


def begins_with(shapes: list[str], values: list[str]) -> dict[str, list[str]]:
    """The shapes of a text for each value of the category it begins with: those that start with the value
    (the longest one that fits), `<value> {hex}` when none does. Shapes that start with no value at all
    (texts a lesson asked for by name) are dealt out over the values in turn."""
    out: dict[str, list[str]] = {v: [] for v in values}
    loose = []
    for shape in shapes:
        owners = [v for v in values if shape.startswith(v) and not shape[len(v):len(v) + 1].isalnum()]
        if owners:
            out[max(owners, key=len)].append(shape)
        else:
            loose.append(shape)
    for v in values:
        if not out[v]:
            out[v].append(v + " {hex}")
    for i, shape in enumerate(loose):
        out[values[i % len(values)]].append(shape)
    return out


def named_values(gen: dict) -> list[str]:
    """The values a category holds by name: those of a `categorical`, or the texts without a made-up part among
    the shapes of a `template` (`meltano elt` beside `meltano add {hex}`)."""
    if gen.get("gen") == "categorical":
        return [str(v) for v in gen.get("values") or {} if isinstance(v, str)]
    if gen.get("gen") == "template":
        return [str(t) for t in gen.get("templates") or () if "{" not in str(t)]
    return []


ORGS = 24  # how many large organisations the generated addresses are at (`{org}`): org1 ... org24


def org_of(text: str) -> str:
    """SQL for the organisation the text `text` (SQL as well) belongs to: half of the texts are spread over a few
    large ones (what a list of accounts matches, what a count by domain shows), the others each at a small one
    nearly its own, as the long tail of company domains is."""
    return (f"concat('org', case when hash(concat({text}, '~')) % 2 = 0 then 1 + (hash({text}) % {ORGS})::bigint "
            f"else {ORGS + 1} + (hash({text}) % 1000000)::bigint end)")


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
    turn = None  # while a column follows the thing a table of readings is about: that thing's number

    def salt(self, name: str) -> str:
        if self.base in ("g", "(g - 1)"):  # the row, or (for a value that repeats the row before) the one before it
            return f"{self.spec.seed}:{self.t.name}:{name}"
        # keyed on an entity's id: the same id must give the same attributes in every table
        return f"{self.spec.seed}:entity:{name.split('.')[-1].lower()}"

    def u(self, salt: str, *extra: str) -> str:
        args = "".join(f", {e}" for e in extra)
        # hashed twice: DuckDB combines the arguments of one hash() so lightly that two columns' draws for
        # the same row moved together (a third of the combinations of two categories never occurred)
        return f"((hash(hash({self.base}, {lit(self.salt(salt))}{args})) >> 11)::double / 9007199254740992.0)"

    def linked(self) -> list[Table]:
        """The other tables that `link` values in this table's documents point into."""
        found: dict[str, Table] = {}

        def walk(gens: dict) -> None:
            for d in gens.values():
                if d.get("gen") == "link" and not d.get("layered"):
                    other = self.spec.tables.get(".".join(str(d.get("domain", "")).split(".")[:3]).upper())
                    if other is not None and other.name != self.t.name:
                        found[other.name] = other
                elif d.get("gen") == "object":
                    walk(d.get("fields") or {})

        walk(self.t.columns)
        return list(found.values())

    def in_order(self) -> bool:
        """A table marked as a log (`log: true`): its rows come one after the other, each later than the one before
        (blocks of a chain, entries of a ledger). Times and the numbers that grow with them follow the row order."""
        return bool(self.t.raw.get("log")) and self.base == "g"

    @staticmethod
    def log_moment() -> str:
        """The row's moment in a log: the day's rows spread evenly over the day, in order."""
        return "(day::timestamp + to_microseconds((_n * 86400000000 // greatest(_dn, 1))::bigint))"

    def log_fraction(self) -> str:
        """How far through the history the row is (0 to 1): what an ever-growing number of a log follows."""
        days = int(self.spec.history or 60)
        return (f"least(0.999999, greatest(0.0, (date_diff('day', date '{self.spec.start:%Y-%m-%d}', day) "
                f"+ _n::double / greatest(_dn, 1)) / {days}.0))")

    def grain(self) -> dict[str, int]:
        """The columns of the table's `grain` that take their values in turn (listed values, or a pool's), with how
        many each has: unique together with the period (the day, or the hour when the time column is cut to hours)."""
        return self.t.grain_sizes()

    def grain_time(self) -> tuple[str, str] | None:
        """The time column of the grain and its period (hour or day): the rows of a day fill its periods in turn."""
        for c in self.t.raw.get("grain") or []:
            d = self.t.columns.get(c, {})
            if d.get("gen") in ("event_time", "time_offset") and self.grain():
                return c, "hour" if d.get("trunc") == "hour" else "day"
        return None

    def grain_hour(self) -> str | None:
        found = self.grain_time()
        return found[0] if found and found[1] == "hour" else None

    def key_expr(self, table: Table, index: str) -> str:
        _col, d = table.key
        named = d.get("named") or []
        if d["gen"] == "sequence":  # a numeric key holds only the ids that are numbers
            named = [v for v in named if re.fullmatch(r"\d{1,18}", str(v))]
        if named:
            # ids the project's SQL asks for by name (`where server_id = '93myk...'`): the first rows have them
            numeric = d["gen"] == "sequence"
            arms = " ".join(f"when {i} then {int(v) if numeric else lit(str(v))}" for i, v in enumerate(named))
            return f"(case {index} {arms} else {self._key_expr(table, d, index)} end)"
        return self._key_expr(table, d, index)

    @staticmethod
    def ending(d: dict, head: str = "''") -> str:
        """What every value of a pool ends with (`email_7@org3.fauxmail.net`), as more arguments of its concat.
        `{org}` in it is one of a few organisations, always the same one for the same text before it (`head`)."""
        if not d.get("suffix"):
            return ""
        return "".join(f", {org_of(head)}" if piece == "{org}" else f", {lit(piece)}"
                       for piece in re.split(r"(\{org\})", str(d["suffix"])) if piece)

    def pooled(self, d: dict, n: str) -> str:
        """The n-th text of a pool: `plan_7`, or `email_7@org3.fauxmail.net` when the pool has an ending."""
        head = f"concat({lit(str(d['pool']) + '_')}, {n})"
        return f"concat({lit(str(d['pool']) + '_')}, {n}{self.ending(d, head)})"

    def _key_expr(self, table: Table, d: dict, index: str) -> str:
        if d.get("template") and d["gen"] == "hash_id":
            # an id with a shape (`{n}.{n}.{n}`): the row's number written across the `{n}`s, the last ones a digit
            # each, so every row has its own; `{hex}` is text that differs from row to row
            pieces = re.split(r"(\{n\}|\{hex\})", str(d["template"]))
            numbers = [i for i, piece in enumerate(pieces) if piece == "{n}"]
            out = []
            for i, piece in enumerate(pieces):
                if piece == "{n}":
                    place = len(numbers) - 1 - numbers.index(i)  # 0 for the last
                    digit = f"({index} // {10 ** place})" if i == numbers[0] else f"(({index} // {10 ** place}) % 10)"
                    out.append(f"{digit}::bigint::varchar")
                elif piece == "{hex}":
                    out.append(f"left(md5(concat({lit(f'{self.spec.seed}:{table.name.upper()}:{i}:')}, {index})), 12)")
                elif piece:
                    out.append(lit(piece))
            return f"concat({', '.join(out)})"
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

        def absent(value) -> float | None:  # noqa: ANN001
            """How often a key is left out: its own rate, or (a key with keys under it) the one all of them share."""
            if isinstance(value, tuple):
                return value[2].get("optional")
            rates = [absent(v) for v in value.values()] if isinstance(value, dict) else [None]
            return max(rates) if rates and all(rates) else None

        def leaf(value: tuple) -> str:
            if value[2].get("gen") == "previous" and value[2].get("of") in fields and self.base == "g":
                # what another field held in the row before: `prev_hash` is the hash of the block before
                self.base = "(g - 1)"
                try:
                    return self._expr(f"{col}.{value[2]['of']}", fields[value[2]["of"]])
                finally:
                    self.base = "g"
            if value[2].get("follows") in fields and value[2]["follows"] != value[1] and self.base == "g":
                # a field that belongs to another of the same document: one hostname for one installation
                other = value[2]["follows"]
                self.base = f"({self.expr(f'{col}.{other}', {k: v for k, v in fields[other].items() if k != 'follows'})})"
                try:
                    return self._expr(f"{col}.{value[1]}", value[2])
                finally:
                    self.base = "g"
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
                    if mark == "~json" and isinstance(node.get("~prefix"), tuple):
                        # `EVENT_JSON:{...}`: what the SQL strips off before it parses the rest
                        made = f"concat({lit(str(node['~prefix'][2].get('value', '')))}, {made})"
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
                rate = absent(value)
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
            sizes = self.grain()
            if self.t.columns[other].get("of") and other in sizes:
                # what it follows is the thing a table of readings is about: the things come in turn, and this
                # is the number of the row's own (the first ones take the values the column has by name)
                self.turn = f"((_n // {math.prod(list(sizes.values())[:list(sizes).index(other)])}) % {sizes[other]})::bigint"
            self.base = f"({self.expr(other, self.t.columns[other])})"
            try:
                return self._expr(col, d)
            finally:
                self.base, self.turn = "g", None
        return self._expr(col, d)

    def _expr(self, col: str, d: dict) -> str:
        gen = d.get("gen")
        if gen == "hash_id" and d.get("domain") and "." in col:
            # an id inside a document: one per element, in a range of its own that `link` values point into
            expr = f"md5(concat({lit(f'{self.spec.seed}:{d['domain']}:')}, {self.base}))"
        elif gen in ("sequence", "hash_id") and d.get("of") and col in self.grain() and self.base == "g" and "." not in col:
            # a table of readings: the same few things report period after period, each in its turn
            sizes = self.grain()
            stride = math.prod(list(sizes.values())[:list(sizes).index(col)])
            expr = self.key_expr(self.t, f"((_n // {stride}) % {sizes[col]})::bigint")
        elif gen in ("sequence", "hash_id"):
            expr = self.key_expr(self.t, "g")
        elif gen == "link":
            # the id of another row's element: of one of the first rows, or (ids of the same table, which form
            # a hierarchy) of a row a little earlier and one level up, so that chains end and never loop
            draw = self.u(col)
            depth, slot = int(d.get("depth", 0)), int(d.get("slot", 0))
            if d.get("layered") and d.get("tree") and depth == 1:
                # ids and the lists that refer to them, in one document: a small tree in every row. The elements of
                # another list (transactions) name the even-numbered ids, each of which names the id after it as its
                # child. An id has one parent at most, nothing loops, and most ids are reached.
                inside = list(dict.fromkeys(re.findall(r"_i\d+", self.base)))
                arrays = col.count("[]")
                own, last = (inside[0], inside[-1]) if arrays >= 2 and len(inside) >= 2 else \
                    ("0", inside[0]) if inside and col.endswith("[]") else (inside[0] if inside else "0", "0")
                wanted, index = (f"{last} = 0 and {own} % 2 = 0", f"{own} + 1") if d.get("within") else (f"{last} = 0", f"{own} * 2")
                expr = (f"md5(concat({lit(f'{self.spec.seed}:{d['domain']}:')}, case when {wanted} then hash(g, {index}) "
                        f"else hash(-1 - g, {own} * 16 + {last}) end))")
                depth = -1  # (done: skip the steps below)
            elif d.get("layered"):
                # the row before, one level up, and only from the first element of a list: an id has at most one
                # row referring to it through this field, as a child has one parent. The rest point nowhere.
                inside = re.findall(r"_i\d+", self.base)
                first = "".join(f" and {index} = 0" for index in dict.fromkeys(inside))
                target = f"(case when g % 3 > 0{first} then g - 1 else -1 - g * 16{f' - {inside[-1]}' if inside else ''} end)"
            else:
                target = f"floor({draw} * {int(d.get('first', 50))})::bigint"
                other = self.spec.tables.get(".".join(str(d.get("domain", "")).split(".")[:3]).upper())
                if other is not None and other.name != self.t.name:
                    # any of the rows the other table has by this row's day, not only its first few: every one of
                    # its ids is referred to (a transaction has receipts), as far as there are rows to refer
                    target = f"floor({draw} * greatest(_lc_{other.ident}, {int(d.get('first', 50))}))::bigint"
            for level in range(depth):
                # the element of the row it points at: each referring field has its own (`slot`), so two fields
                # never name the same id
                target = f"hash({target}, {slot if level == depth - 1 else 0})"
            if depth >= 0:
                expr = f"md5(concat({lit(f'{self.spec.seed}:{d['domain']}:')}, {target}))"
        elif gen == "fk":
            parent = self.spec.tables[d["ref"].upper()]
            skew = float(d.get("skew", 1.0))
            idx = f"floor(pow({self.u(col)}, {skew!r}) * greatest(_pc_{col}, 1))::bigint"
            if parent.key[1].get("of"):  # readings of a few things: one of them
                idx = f"floor({self.u(col)} * {int(parent.key[1]['of'])})::bigint"
            elif d.get("first"):  # inside a document: one of the parent's first rows
                idx = f"floor({self.u(col)} * {int(d['first'])})::bigint"
            elif d.get("per_parent") and self.base == "g":
                # at most so many rows for one parent (`having count(*) > 2` is a test): rows share parents in turn
                idx = f"((g // {max(int(d['per_parent']), 1)}) % greatest(_pe_{col}, 1))::bigint"
            expr = self.key_expr(parent, idx)
        elif gen == "categorical" and col in self.grain() and self.base == "g":
            sizes = self.grain()
            stride = math.prod(list(sizes.values())[:list(sizes).index(col)])
            known = "[" + ", ".join(lit(v) for v in d["values"]) + "]"
            expr = f"list_extract({known}, 1 + ((_n // {stride}) % {sizes[col]})::bigint)"
        elif gen == "categorical":
            total = sum(d["values"].values())
            x, arms, acc = self.u(col), [], 0.0
            for value, weight in d["values"].items():
                acc += weight / total
                arms.append(f"when {x} < {acc!r} then {lit(value)}")
            expr = f"case {' '.join(arms)} else {lit(list(d['values'])[-1])} end"
        elif gen == "row_of":
            # one row of a list the project wrote down (a seed); every column of the same `group` takes its value
            # from the same row, so the pairs that occur are the ones the list has
            known = "[" + ", ".join("NULL" if v is None else lit(str(v)) for v in d["values"]) + "]"
            expr = f"list_extract({known}, 1 + floor({self.u('row_of:' + str(d['group']))} * {len(d['values'])})::bigint)"
        elif gen == "pool" and col in self.grain() and self.base == "g":
            sizes = self.grain()
            n = f"((_n // {math.prod(list(sizes.values())[:list(sizes).index(col)])}) % {sizes[col]})::bigint"
            expr = f"({n} + 1)" if d.get("numeric") else self.pooled(d, n)
        elif gen == "pool":
            # one of `size` values shared by every column that names the same pool: joins between them match
            n = f"floor({self.u(col)} * {int(d.get('size', 200))})::bigint"
            expr = f"({n} + 1)" if d.get("numeric") else self.pooled(d, n)
        elif gen == "dist":
            # (in a log, a number filtered by ranges grows with time: a range is a stretch of consecutive rows)
            expr = piecewise(self.log_fraction() if self.in_order() and len(d["p"]) > 2 and "." not in col else self.u(col), d["p"], d["v"])
            if d.get("exactly") and not d.get("unique"):
                # numbers the SQL asks for by name (`where calendar_id = 2`): four rows in ten hold one of them
                known = "[" + ", ".join(str(int(v)) for v in d["exactly"]) + "]"
                pick = f"list_extract({known}, 1 + floor({self.u(col + ':which')} * {len(d['exactly'])})::bigint)"
                expr = f"(case when {self.u(col + ':exactly')} < 0.4 then {pick} else {expr} end)"
            if d.get("unique") and self.base == "g":
                expr = "(g + 1)"  # a `unique` test covers it: one value per row
            if "round" in d:
                expr = f"round({expr}, {int(d['round'])})"
            if str(d.get("type", "")).upper() in ("VARCHAR", "TEXT", "STRING") and "round" not in d:
                expr = f"({expr})::bigint"  # numbers kept as text (`account_number between '4000' and '4999'`): whole ones
        elif gen == "template":
            def fill(template: str) -> str:
                parts = []
                for i, part in enumerate(d.get("parts") or []):
                    template = template.replace("{" + str(i) + "}", f"\0part{i}\0")
                for chunk in template.replace("{n}", "\0n\0").replace("{hex}", "\0hex\0").replace("{org}", "\0org\0").split("\0"):
                    if chunk.startswith("part") and chunk[4:].isdigit() and int(chunk[4:]) < len(d.get("parts") or []):
                        # another generator's value inside the text: an id a model parses back out of it
                        parts.append(f"({self._expr(f'{col}:{chunk}', d['parts'][int(chunk[4:])])})::varchar")
                    elif chunk == "n":
                        # (inside a document the row has no number of its own: a small one, so that `12.4.0` reads as numbers)
                        parts.append(f"({self.base} + 1)::varchar" if self.base in ("g", "(g - 1)") else
                                     f"(hash({self.base}) % 1000)::varchar" if template.count("{n}") > 1 else f"({self.base})::varchar")
                    elif chunk == "hex":
                        parts.append(f"substr(md5(concat({lit(self.salt(col) + ':')}, {self.base})), 1, 12)")
                    elif chunk == "org":
                        # the organisation an address is at: the same one wherever the same name is written
                        parts.append(org_of(f"concat({', '.join(parts)})" if parts else "''"))
                    elif chunk:
                        parts.append(lit(chunk))
                return f"concat({', '.join(parts)})"

            shapes = d.get("templates") or [d["template"]]  # several shapes: each row takes one

            def one_of(some: list[str]) -> str:
                pick = f"floor({self.u(col + ':shape')} * {len(some)})::bigint"
                return fill(some[0]) if len(some) == 1 else \
                    f"case {pick} {' '.join(f'when {i} then {fill(t)}' for i, t in enumerate(some))} end"

            lead = self.t.columns.get(str(d.get("begins_with") or "")) if "." not in col else None
            if lead and named_values(lead) and not lead.get("begins_with"):
                # the text begins with the row's category (a command and its command category): each row takes a
                # shape that begins with the value its category column holds
                by_value = begins_with(shapes, named_values(lead))
                arms = " ".join(f"when {lit(v)} then {one_of(some)}" for v, some in by_value.items())
                expr = f"case ({self._expr(str(d['begins_with']), lead)})::varchar {arms} else {one_of(shapes)} end"
            else:
                expr = one_of(shapes)
            if d.get("named") and (self.base == "g" or self.turn) and "." not in col:
                # values another table or a seed holds, in a column no two rows share: the first rows have them
                # (the first things, where the column says something about the thing a row is a reading of)
                arms = " ".join(f"when {i} then {lit(str(v))}" for i, v in enumerate(d["named"]))
                expr = f"(case {self.turn or 'g'} {arms} else {expr} end)"
        elif gen == "time_offset":
            expr = f"_ev + {ms(piecewise(self.u(col), d['p'], d['seconds']))}"
            if self.in_order() and min(d["seconds"]) >= 0 and max(d["seconds"]) <= 7 * 86400:
                expr = self.log_moment()  # a log's own time (not an end date far ahead): in the order of the rows
            if d.get("unique") and self.base == "g":
                expr = "(timestamp '2000-01-01' + to_days(g::integer))"  # a `unique` test covers it: one day per row
                if d.get("every_days"):  # periods that follow one another from the start of the history
                    expr = (f"(timestamp '{self.spec.start:%Y-%m-%d}' + "
                            f"to_days((g * {int(d['every_days'])} + {int(d.get('plus_days', 0))})::integer))")
            if d.get("epoch"):  # the moment as seconds since 1970, the way APIs often send it
                expr = f"epoch({expr})::bigint"
                if d.get("scale"):  # read with to_timestamp(x, 3): milliseconds (6: micro-, 9: nanoseconds)
                    expr = f"({expr} * {10 ** int(d['scale'])} + floor({self.u(col + ':sub')} * {10 ** int(d['scale'])})::bigint)"
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
            expr = self.log_moment() if self.in_order() else "_ev"
        elif gen == "updated_at":
            expr = "_ts"
        elif gen == "loaded_at":
            expr = "_loaded"
        elif gen == "lifecycle":
            states = [d["initial"]] + [s["to"] for s in d["steps"]]
            expr = f"case k {' '.join(f'when {i} then {lit(s)}' for i, s in enumerate(states))} end"
        elif gen == "previous":
            other = self.t.columns.get(str(d.get("of")))
            if other is None or self.base != "g":
                expr = "NULL"
            else:  # a column's value in the row before
                self.base = "(g - 1)"
                try:
                    expr = self._expr(str(d["of"]), other)
                finally:
                    self.base = "g"
        elif gen == "constant":
            expr = lit(d.get("value"))
        else:
            raise ValueError(f"{self.t.name}.{col}: unknown gen {gen!r}")
        if gen in ("event_time", "time_offset") and self.base == "g" and (self.grain_time() or ("", ""))[0] == col \
                and not d.get("format") and not d.get("epoch"):
            # one row a period for each entity: the day's rows fill its hours in turn (or all belong to the day)
            expr = "day::timestamp" if self.grain_time()[1] == "day" else \
                f"(day::timestamp + to_hours(((_n // {math.prod(self.grain().values())}) % 24)::integer))"
        if d.get("trunc") and gen in ("event_time", "time_offset", "updated_at") and not d.get("format") and not d.get("epoch"):
            expr = f"date_trunc({lit(str(d['trunc']))}, {expr})"  # reported per hour (or day): on the boundary
        if d.get("length") and "." not in col:
            # an id of a given length (`length(server_id) = 26`): the value rewritten, the same way wherever it is
            # written, so what matched still matches and what differed still differs
            text = f"({expr})::varchar"
            expr = f"substr(md5({text}) || md5({text} || '+'), 1, {int(d['length'])})"
        rate = repr(float(d.get("null_rate") or 0.0))
        if self.sc and self.sc.null_rate is not None and gen in ("fk", "categorical", "dist", "pool", "template", "time_offset", "object", "array"):
            rate = self.during(repr(self.sc.null_rate), rate)
        if rate != "0.0":
            expr = f"case when {self.u(col + ':null')} < {rate} then NULL else {expr} end"
        if d.get("empty_unless") and gen == "array" and self.base == "g" and "." not in col:
            # a list that has something only where a rule about the row holds: empty in the other rows
            rule = re.sub(r"\{\{(\w+)\}\}", lambda m: f"({self.expr(m[1], self.t.columns[m[1]])})", str(d["empty_unless"]))
            expr = f"case when ({rule}) then {expr} else '[]'::JSON end"
        if d.get("null_unless") and self.base == "g" and "." not in col:
            # a rule about the row holds wherever the column has a value: no value in the rows it does not hold for
            rule = re.sub(r"\{\{(\w+)\}\}", lambda m: f"({self.expr(m[1], self.t.columns[m[1]])})", str(d["null_unless"]))
            expr = f"case when ({rule}) then {expr} end"
        if d.get("null_once") and self.base == "g":
            # one row has none: the root of a tree (`where parent_id is null`, asked for as a single value)
            expr = f"case when g = 0 then NULL else {expr} end"
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
        # parents there before the row's day, and (for rows that take parents in turn) by the end of it
        pcs = "".join(f", coalesce(pc_{c}.cum, 0) as _pc_{c}, coalesce(pc_{c}.cum, 0) + coalesce(pc_{c}.n, 0) as _pe_{c}" for c, _ in fks)
        # tables whose documents hold the ids this table's documents refer to: how many rows they had by the row's day
        for other in self.linked():
            joins += f" left join cal_{other.ident} lc_{other.ident} on lc_{other.ident}.day = c.day"
            pcs += f", coalesce(lc_{other.ident}.cum, 0) + coalesce(lc_{other.ident}.n, 0) as _lc_{other.ident}"

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
        if t.raw.get("log"):
            # a log arrives in the order it was written: what has not been loaded yet is its end, never a row in the
            # middle (a block missing from a chain)
            second_of_day = "(_n * 86400.0 / greatest(_dn, 1))"
            late = repr(float(min(t.lateness["seconds"])))
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
        # one row a day for each combination of the listed values: the rows of a day take them in turn, and
        # a day has no more rows than there are combinations
        grain = f" and _n < {math.prod(self.grain().values()) * (24 if self.grain_hour() else 1)}" if self.grain() else ""
        things = next((c for c in self.grain() if t.columns.get(c, {}).get("of")), None)
        if things and t.raw.get("gaps"):
            # readings miss a day now and then (a server that was down, a report that never arrived): the thing has
            # no row that day, which is what the models that fill the days in between are written for
            sizes = self.grain()
            turn = f"((_n // {math.prod(list(sizes.values())[:list(sizes).index(things)])}) % {sizes[things]})"
            grain += (f" and hash(concat({lit(f'{self.spec.seed}:{t.name}:gap:')}, day::varchar, ':', {turn})) % 1000 "
                      f">= {int(float(t.raw['gaps']) * 1000)}")
        if with_new_column:
            since = f"timestamp '{self.sc.start} 00:00:00'"
            cols += f',\n  case when _loaded >= {since} then \'v2\' end as "{self.sc.new_column}"'
        return f"""
with b as (
  select r.g, c.day, r.g - c.cum as _n, c.n as _dn{pcs}
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
where _loaded > $after and _loaded <= $until{grain}"""


def kept_columns(rel: dict) -> dict[str, str]:
    """`existing: {X: {columns: ..., keeps: [A, B]}}`: the columns X has from before, with their types."""
    columns = rel.get("columns") or {}
    return {c: columns[c] for c in rel.get("keeps") or [] if c in columns}


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
            cal = t.calendar(self.spec.start, until.date(), self.spec.seed, self.scale, self.scenario, self.spec.thin, self.spec.least)
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
                    emulator.stand_in(self.cur, name, existing_view(name, rel.get("columns") or {}, table=rel.get("as") == "table"),
                                      keeps=kept_columns(rel))
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
