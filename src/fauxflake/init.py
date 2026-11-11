"""Write a first spec from the dbt project alone: no Snowflake, no hand-written YAML.

A dbt project already says most of what the generator needs:

* which columns each source has, from how models select from it;
* their types, from casts and naming conventions;
* the grain, from `unique` tests;
* relationships, from `relationships` tests;
* categories, from `accepted_values` tests;
* the load timestamp, from source freshness config.

Tests are usually written on staging models, so they are traced back to source
columns through renames. Volumes and distributions are placeholders until
`fauxflake profile` measures them.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

from . import catalog

LOADED_AT = ["_LOADED_AT", "_FIVETRAN_SYNCED", "_AIRBYTE_EXTRACTED_AT", "_SDC_BATCHED_AT", "_ETL_LOADED_AT",
             "LOADED_AT", "_INGESTED_AT", "INGESTED_AT", "_PARTITIONTIME"]
EVENT_TIME = ["OCCURRED_AT", "EVENT_TIMESTAMP", "EVENT_TIME", "EVENT_AT", "ORDERED_AT", "CREATED_AT", "CREATED",
              "TIMESTAMP", "CREATED_DATE", "DATE"]
UPDATED_AT = ["UPDATED_AT", "MODIFIED_AT", "LAST_MODIFIED", "UPDATED", "_UPDATED_AT"]
AMOUNT = {"p": [0, 0.5, 0.9, 0.99, 1], "v": [0, 20, 100, 500, 5000]}
WEEK = {"p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}


FLATTEN_COLUMNS = {"SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"}


class SourceUse:
    """What the project's SQL reveals about one source table."""

    def __init__(self) -> None:
        self.columns: dict[str, str | None] = {}  # column -> type hint from a cast
        self.paths: dict[str, dict[str, str | None]] = {}  # variant column -> {"a.b": type hint}
        self.arrays: set[str] = set()  # variant columns that are flattened or indexed
        self.formats: dict[str, str] = {}  # text column -> strftime format it is parsed with
        self.json_text: set[str] = set()  # text columns holding JSON (passed to PARSE_JSON)
        self.texts: set[str] = set()  # columns compared with string literals
        self.ips: set[str] = set()  # text columns handed to PARSE_IP
        self.bounds: dict[str, set[float]] = {}  # column -> numbers the SQL compares it with
        self.splits: dict[str, dict] = {}  # text column -> {"seps": {"/": {0: "BIGINT"}}, "digits": False} from split()[i]::type
        self.numbers: set[str] = set()  # columns that are summed, averaged or used in arithmetic
        self.literals: dict[str, set[str]] = {}  # column -> string values the SQL compares it with
        self.patterns: dict[str, set[str]] = {}  # column -> LIKE patterns the SQL keeps rows by
        self.star = False

    def add(self, column: str, type_hint: str | None = None) -> None:
        column = column.upper()
        current = self.columns.get(column)
        # a cast to text says little (anything casts to text); a cast to a number or date says a lot
        if not current or (current.upper() in ("TEXT", "VARCHAR") and type_hint):
            self.columns[column] = type_hint or current


def _table_id(table: exp.Table) -> str:
    return ".".join(p.upper() for p in (table.catalog, table.db, table.name) if p)


_WRAPPERS = (exp.Cast, exp.TryCast, exp.Paren, exp.Lower, exp.Upper, exp.Trim)


def _parsed_as(node: exp.Expression) -> str | None:
    """TIMESTAMP or DATE when the value (possibly cast to text first) is handed to a function that
    reads it as a point in time: to_timestamp_ntz(v:header:timestamp::string)."""
    top = node
    while isinstance(top.parent, (exp.Cast, exp.TryCast, exp.Paren)) and (
            isinstance(top.parent, exp.Paren) or top.parent.to.is_type(*exp.DataType.TEXT_TYPES)):
        top = top.parent
    reader = top.parent
    if isinstance(reader, exp.TsOrDsToDate) and reader.this is top and not reader.args.get("format"):
        return "DATE"
    if isinstance(reader, exp.Anonymous) and reader.expressions and reader.expressions[0] is top and len(reader.expressions) == 1 \
            and re.fullmatch(r"(TRY_)?TO_TIMESTAMP(_NTZ|_LTZ|_TZ)?", reader.name.upper()):
        return "TIMESTAMP"
    return None


def _split_use(use: "SourceUse", column: str, call: exp.Expression) -> bool:
    """Record `split(col, sep)[i]::type` or `split_part(col, sep, i)::type`: which part of the text is
    read and what it is cast to. `right(split(col, '.')[0], 1)::number` says the part ends in digits."""
    if isinstance(call, exp.SplitPart):
        sep, index, part = call.args.get("delimiter"), call.args.get("part_index"), call
        base = 1
    else:
        sep, part, base = call.expression, call.parent if isinstance(call.parent, exp.Bracket) else None, 0
        index = part.expressions[0] if part is not None and part.expressions else None
    if not (isinstance(sep, exp.Literal) and sep.is_string and isinstance(index, exp.Literal) and index.is_int and int(index.name) >= base):
        return False
    outer, sliced = part.parent, False
    while isinstance(outer, (exp.Right, exp.Left, exp.Substring, exp.Trim, exp.Paren)):
        outer, sliced = outer.parent, True
    hint = outer.to.sql(dialect="duckdb") if isinstance(outer, (exp.Cast, exp.TryCast)) else None
    found = use.splits.setdefault(column, {"seps": {}, "digits": False})
    parts = found["seps"].setdefault(sep.name, {})
    position = int(index.name) - base
    parts[position] = (None if sliced else hint) or parts.get(position)
    found["digits"] |= sliced and hint_is_number(hint)
    return True


def scan_model(sql: str, sources: dict[str, SourceUse],
               models: dict[str, dict[str, tuple[str, str]]] | None = None,
               links: list[set[tuple[str, str]]] | None = None,
               joins: list[tuple[tuple[str, str], tuple[str, str]]] | None = None) -> dict[str, tuple[str, str]]:
    """Record source columns used by one model. Returns {output column: (source, source column)}
    for outputs that are a plain (possibly cast or renamed) source column."""
    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except sqlglot.errors.SqlglotError:
        return {}
    passthrough: dict[str, str] = {}  # CTE name -> source it is `select *` of
    computed: dict[str, set[str]] = {}  # CTE name -> columns it adds next to the star
    lineage: dict[str, dict[str, tuple[str, str]]] = {}  # CTE name -> its outputs
    last: dict[str, tuple[str, str]] = {}
    positional: dict[int, list] = {}  # select -> source column behind each projection, by position
    cte_positions: dict[str, list] = {}
    outputs_of: dict[int, dict] = {}
    for scope in traverse_scope(tree):
        select = scope.expression
        if isinstance(select, exp.SetOperation):
            # a UNION is known by its first branch: names and order come from there
            first = select
            while isinstance(first, exp.SetOperation):
                first = first.left.unnest()
            outputs = outputs_of.get(id(first), {})
            if isinstance(select.parent, exp.CTE):
                lineage[select.parent.alias.upper()] = outputs
                cte_positions[select.parent.alias.upper()] = positional.get(id(first), [])
            last = outputs
            continue
        if not isinstance(select, exp.Select):
            continue
        # which source (if any) each alias in this scope stands for, and prior renames
        direct: dict[str, str] = {}
        added: dict[str, set[str]] = {}
        renames: dict[str, dict[str, tuple[str, str]]] = {}
        selected = {alias: src for alias, (_, src) in scope.selected_sources.items()}
        # inside LATERAL FLATTEN(input => t.col) the outer table is visible too
        selected.update({a: src for a, src in getattr(scope, "lateral_sources", {}).items() if a not in selected})
        for alias, node in selected.items():
            if isinstance(node, exp.Table):
                if _table_id(node) in sources:
                    direct[alias.upper()] = _table_id(node)
                elif node.name.upper() in passthrough:
                    direct[alias.upper()] = passthrough[node.name.upper()]
                    added[alias.upper()] = computed.get(node.name.upper(), set())
                elif models and _table_id(node) in models and "*" in models[_table_id(node)]:
                    # a model that is `select * from source`: reading it is reading the source
                    through, computed_there = models[_table_id(node)]["*"]
                    direct[alias.upper()] = through
                    added[alias.upper()] = set(computed_there.split("|")) - {""}
                elif models and _table_id(node) in models:  # another model: follow its lineage
                    renames[alias.upper()] = models[_table_id(node)]
            else:  # a CTE or subquery scope
                name = alias.upper()
                if name in passthrough:
                    direct[name] = passthrough[name]
                    added[name] = computed.get(name, set())
                elif name in lineage:
                    renames[name] = lineage[name]
        # a lateral FLATTEN next to the table only contributes its own fixed columns
        laterals = {a.upper() for a, src in selected.items() if getattr(src, "is_udtf", False)}
        others = {a.upper() for a in selected} - set(direct) - laterals
        only = next(iter(direct.values())) if len(direct) == 1 and not renames and not others else None

        # `metadata$filename as _filename, split(_filename, '/')`: the second use is the alias, not a column
        aliases = {p.alias.upper(): p.this for p in select.expressions if isinstance(p, exp.Alias)
                   and not any(c.name.upper() == p.alias.upper() for c in p.this.find_all(exp.Column))}

        def resolve(col: exp.Column, depth: int = 0):
            table = col.table.upper()
            if not table and col.name.upper() in aliases and not isinstance(col.parent, exp.Alias):
                inner = aliases[col.name.upper()]
                while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                    inner = inner.this
                return resolve(inner, depth + 1) if isinstance(inner, exp.Column) and depth < 4 else None
            if not table and laterals and col.name.upper() in FLATTEN_COLUMNS:
                return None
            if table in direct or (not table and only):
                owner = table or next(iter(direct))
                if col.name.upper() in added.get(owner, ()):
                    return None  # computed in the CTE (a row_number, say), not a source column
                return (direct.get(table) or only), col.name.upper()
            if table in renames:
                return renames[table].get(col.name.upper())
            if not table and len(renames) == 1 and len(selected) == 1:
                return next(iter(renames.values())).get(col.name.upper())
            return None

        for col in scope.columns:
            hit = resolve(col)
            if hit:
                cast = col.parent if isinstance(col.parent, (exp.Cast, exp.TryCast)) else None
                use, parent = sources[hit[0]], col.parent
                use.add(hit[1], cast.to.sql(dialect="duckdb") if cast else None)
                if not cast and _parsed_as(col) and not use.columns.get(hit[1]):
                    use.columns[hit[1]] = _parsed_as(col)
                if isinstance(parent, (exp.Sum, exp.Avg, exp.Add, exp.Sub, exp.Mul, exp.Div)):
                    use.numbers.add(hit[1])
                if joins is not None:
                    # a.x = b.y: the two columns must share values, or the join matches nothing
                    top = col
                    while isinstance(top.parent, _WRAPPERS):
                        top = top.parent
                    if isinstance(top.parent, exp.EQ):
                        other = top.parent.expression if top.parent.this is top else top.parent.this
                        while isinstance(other, _WRAPPERS):
                            other = other.this
                        there = resolve(other) if isinstance(other, exp.Column) else None
                        if there and there[0] != hit[0]:
                            joins.append((hit, there))
                if isinstance(parent, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.Between)):
                    # `where block_number between 9820000 and 10000000`: the numbers that should select something
                    sides = [parent.args.get("low"), parent.args.get("high")] if isinstance(parent, exp.Between) else \
                        [parent.expression if parent.this is col else parent.this]
                    for side in sides:
                        negative = isinstance(side, exp.Neg)
                        number = side.this if negative else side
                        if isinstance(number, exp.Literal) and not number.is_string:
                            try:
                                use.bounds.setdefault(hit[1], set()).add(-float(number.name) if negative else float(number.name))
                            except ValueError:
                                pass
                if (isinstance(parent, exp.ParseIp) and parent.this is col) or (
                        isinstance(parent, exp.Anonymous) and parent.name.upper() == "PARSE_IP" and parent.expressions[0] is col):
                    use.ips.add(hit[1])
                if isinstance(parent, exp.Nullif) and parent.this is col and isinstance(parent.expression, exp.Literal) \
                        and parent.expression.is_string and not cast:
                    use.texts.add(hit[1])  # nullif(col, ''): text that can be empty
                if isinstance(parent, exp.ConvertTimezone) and col in (parent.args.get("source_tz"), parent.args.get("target_tz")):
                    use.literals.setdefault(hit[1], set()).update({"UTC", "America/Los_Angeles", "Europe/Berlin"})  # a time zone name
                    use.texts.add(hit[1])
                if isinstance(parent, exp.ParseJSON):
                    use.json_text.add(hit[1])  # parse_json(col):a.b  ->  col is text holding {"a": {"b": ...}}
                    col, parent = parent, parent.parent
                    if not (isinstance(parent, exp.JSONExtract) and parent.this is col):
                        use.paths.setdefault(hit[1], {})
                elif isinstance(parent, (exp.EQ, exp.NEQ, exp.Like, exp.ILike, exp.In)):
                    other = parent.expression if not isinstance(parent, exp.In) else (parent.expressions or [None])[0]
                    other = parent.this if other is col else other
                    if isinstance(other, exp.Literal) and other.is_string and not re.fullmatch(r"[\d\s:.\-/T+Z]*", other.name):
                        use.texts.add(hit[1])  # id = 'USLACKBOT': not a number
                        if isinstance(parent, (exp.Like, exp.ILike)) and parent.this is col and len(other.name) <= 60 \
                                and not isinstance(parent.parent, exp.Not) and re.search(r"[A-Za-z0-9]", other.name):
                            use.patterns.setdefault(hit[1], set()).add(other.name)  # name LIKE 'tap-%'
                        if isinstance(parent, (exp.EQ, exp.NEQ, exp.In)):
                            compared = parent.expressions if isinstance(parent, exp.In) else [other]
                            use.literals.setdefault(hit[1], set()).update(
                                v.name for v in compared if isinstance(v, exp.Literal) and v.is_string and len(v.name) <= 60)
                if isinstance(parent, exp.JSONExtract) and parent.this is col:
                    # v:a.b::int tells us the column is a variant holding {"a": {"b": <int>}}
                    keys = [k.name for k in parent.expression.expressions if isinstance(k, exp.JSONPathKey)]
                    outer = parent.parent
                    hint = outer.to.sql(dialect="duckdb") if isinstance(outer, (exp.Cast, exp.TryCast)) else None
                    hint = _parsed_as(parent) or hint
                    if keys:
                        known = use.paths.setdefault(hit[1], {})
                        previous = known.get(".".join(keys))
                        if previous and hint and {hint_is_number(previous), hint_is_number(hint)} == {True, False} \
                                and (hint if hint_is_number(previous) else previous).upper().startswith(("TIMESTAMP", "DATE", "EPOCH")):
                            hint = "EPOCH"  # cast to a number here, read as a point in time there: seconds since 1970
                        known[".".join(keys)] = "EPOCH" if previous == "EPOCH" else (hint or previous)
                    else:
                        use.arrays.add(hit[1])
                elif isinstance(parent, (exp.Split, exp.SplitPart)) and parent.this is col and _split_use(use, hit[1], parent):
                    pass  # split(col, '/')[0]::number: the text has parts, and this one is a number
                elif isinstance(parent, exp.Bracket) or col.find_ancestor(exp.Explode, exp.Lateral, exp.Unnest) is not None:
                    use.arrays.add(hit[1])
                elif isinstance(parent, (exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate)) and parent.this is col \
                        and isinstance(parent.args.get("format"), exp.Literal):
                    use.formats[hit[1]] = parent.args["format"].name  # a text column holding dates

        outputs: dict[str, tuple[str, str]] = {}
        stars = [p for p in select.expressions if isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))]
        positional[id(select)] = by_position = []
        for proj in select.expressions:
            inner = proj.unalias()
            while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                inner = inner.this
            hit = resolve(inner) if isinstance(inner, exp.Column) and not isinstance(inner.this, exp.Star) else None
            by_position.append(hit)
            if hit:
                outputs[proj.alias_or_name.upper()] = hit
        name = scope.expression.parent.alias.upper() if isinstance(scope.expression.parent, exp.CTE) else None
        if stars and only and not select.args.get("joins"):
            if name:  # `select *, row_number() ... as rn from source` still passes every column through
                passthrough[name] = only
                computed[name] = {p.alias_or_name.upper() for p in select.expressions if p not in stars} \
                    | added.get(next(iter(direct)), set())
            else:
                sources[only].star = True
                extra = {p.alias_or_name.upper() for p in select.expressions if p not in stars} | added.get(next(iter(direct)), set())
                outputs["*"] = (only, "|".join(sorted(extra)))  # the whole model passes the source through
        if stars and len(renames) == 1 and len(selected) == 1:
            outputs = {**next(iter(renames.values())), **outputs}
        if stars and len(select.expressions) == 1 and len(selected) == 1:
            inner_name = next(iter(selected)).upper()
            if inner_name in cte_positions:  # `select * from cte` keeps the CTE's column order
                by_position[:] = cte_positions[inner_name]
        outputs_of[id(select)] = outputs
        if name:
            lineage[name] = outputs
            cte_positions[name] = by_position
        last = outputs
    if links is not None:
        # UNION branches line up by position, and aligned columns must share a type
        def branches(node):
            node = node.unnest()
            return branches(node.left) + branches(node.right) if isinstance(node, exp.SetOperation) else [node]

        for union in tree.find_all(exp.SetOperation):
            if isinstance(union.parent, exp.SetOperation):
                continue
            rows = [positional.get(id(b), []) for b in branches(union)]
            for aligned in zip(*rows):
                group = {hit for hit in aligned if hit}
                if len(group) > 1:
                    links.append(group)
    return last


_KEYS = ("sequence", "hash_id")
_FREE = ("template", "dist", "categorical", "constant")  # generators with no meaning of their own


def _share_join_values(tables: dict[str, dict], joins: list, notes: list[str]) -> None:
    """Columns that models join on must draw from the same values. Group them, then:
    a key in the group makes the others reference it; named values are shared; otherwise
    every member draws from one common pool."""
    group_of: dict[tuple[str, str], set] = {}
    for a, b in joins:
        if not all(m[0] in tables and m[1] in tables[m[0]]["columns"] for m in (a, b)):
            continue
        merged = group_of.get(a, {a}) | group_of.get(b, {b})
        for m in merged:
            group_of[m] = merged
    for group in {id(g): g for g in group_of.values()}.values():
        members = sorted(group)
        gen = {m: tables[m[0]]["columns"][m[1]] for m in members}
        keys = [m for m in members if gen[m]["gen"] in _KEYS]
        if keys:
            parent = keys[0]
            for m in keys[1:]:  # two tables keyed by the same id (user, user_profile)
                gen[parent]["domain"] = gen[m]["domain"] = parent[0]
                if gen[m]["gen"] != gen[parent]["gen"]:
                    gen[m]["gen"] = gen[parent]["gen"]
            for m in members:
                if m[0] != parent[0] and gen[m]["gen"] in _FREE:
                    tables[m[0]]["columns"][m[1]] = {"gen": "fk", "ref": parent[0], "skew": 1.5}
                    notes.append(f"{m[0]}.{m[1]}: joined to {parent[0]}.{parent[1]} in a model, generated as a reference to it.")
            continue
        free = [m for m in members if gen[m]["gen"] in _FREE]
        if len(free) < 2:
            continue
        named = [gen[m] for m in free if gen[m]["gen"] == "categorical"]
        if named:
            values: dict = {}
            for d in named:
                for v, w in d["values"].items():
                    values.setdefault(v, w)
            for m in free:
                tables[m[0]]["columns"][m[1]] = {"gen": "categorical", "values": dict(values)}
        else:
            pool = free[0][1].lower()
            numeric = any(gen[m]["gen"] == "dist" for m in free)
            for m in free:
                tables[m[0]]["columns"][m[1]] = {"gen": "pool", "pool": pool, "size": 200, **({"numeric": True} if numeric else {})}
        notes.append(f"{', '.join('.'.join(m) for m in free)}: joined in a model, generated from the same values.")


def guess(column: str, hint: str | None) -> dict:
    """A generator for a column we know only by name and (maybe) a cast."""
    c, h = column.upper(), (hint or "").upper()
    if h in ("VARIANT", "JSON", "OBJECT", "STRUCT") or h.startswith(("OBJECT", "MAP")):
        return {"gen": "object", "fields": {}}  # col::variant: semi-structured, shape unknown
    if h == "ARRAY" or h.endswith("[]"):
        return {"gen": "array"}
    if h.startswith(("DECIMAL", "NUMERIC")) and not h.endswith(",0)"):
        return {"gen": "dist", "type": h, **AMOUNT}
    if h in ("DOUBLE", "FLOAT", "REAL"):
        return {"gen": "dist", "type": "DOUBLE", **AMOUNT}
    if h == "EPOCH":
        return {"gen": "time_offset", "epoch": True, **WEEK}
    if h in ("BIGINT", "INTEGER", "INT", "SMALLINT", "NUMBER") or h.endswith(",0)"):  # an explicit type beats a name
        return {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    if h == "BOOLEAN" or (not h and re.match(r"^(IS|HAS|WAS)_", c)):
        return {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.5, False: 0.5}}
    if h == "DATE" or (not h.startswith("TIMESTAMP") and c.endswith(("_DATE", "_DAY"))):
        return {"gen": "time_offset", "type": "DATE", **WEEK}
    if h.startswith("TIMESTAMP") or c.endswith(("_AT", "_TS", "_TIME", "_TIMESTAMP")):
        return {"gen": "time_offset", **({"type": "TIMESTAMPTZ"} if h == "TIMESTAMPTZ" else {}), **WEEK}
    if h in ("VARCHAR", "TEXT"):
        return {"gen": "template", "template": f"{column.lower()}_{{hex}}"}
    if h in ("BIGINT", "INTEGER", "INT", "SMALLINT") or h.endswith(",0)") or c.endswith(("_ID", "_COUNT", "_QTY", "_QUANTITY")) or c == "ID":
        return {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    if re.search(r"(AMOUNT|PRICE|TOTAL|COST|REVENUE|VALUE|BALANCE|RATE|SCORE)", c):
        return {"gen": "dist", "type": "DECIMAL(18,2)", **AMOUNT}
    if "EMAIL" in c:
        return {"gen": "template", "template": "user_{hex}@example.com"}
    return {"gen": "template", "template": f"{column.lower()}_{{hex}}"}


def within_bounds(gen: dict, bounds: set[float] | None) -> None:
    """Make a numeric column's values straddle the numbers the SQL compares it with, so that
    `where block_number between 9820000 and 10000000` keeps rows."""
    if not bounds or gen.get("gen") != "dist" or str(gen.get("type", "")).upper() not in ("BIGINT", "DOUBLE", ""):
        return
    lo, hi = min(bounds), max(bounds)
    if lo == hi:  # one threshold: values on both sides of it
        lo, hi = (0, max(2 * hi, 10)) if hi >= 0 else (2 * lo, 0)
    whole = str(gen.get("type", "")).upper() == "BIGINT"
    gen.update(p=[0, 1], v=[int(lo), int(hi)] if whole else [lo, hi])


def hint_is_number(hint: str | None) -> bool:
    return (hint or "").upper().startswith(("BIGINT", "INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT"))


def first(candidates: list[str], columns) -> str | None:
    return next((c for c in candidates if c in columns), None)


class Analysis:
    """What reading the project gives: a first spec, notes for the user, and where each model
    column comes from (so a failing test can be traced to the source column it is about)."""

    def __init__(self, spec: dict, notes: list[str], locate) -> None:  # noqa: ANN001
        self.spec, self.notes, self.locate = spec, notes, locate
        self.bounds: dict[tuple[str, str], set[float]] = {}


def build_spec(manifest: dict, database: str) -> tuple[dict, list[str]]:
    analysis = analyse(manifest, database)
    return analysis.spec, analysis.notes


def analyse(manifest: dict, database: str) -> Analysis:
    notes: list[str] = []
    src_nodes = manifest["sources"]
    ids = {uid: f'{s["database"]}.{s["schema"]}.{s.get("identifier") or s["name"]}'.upper().replace('"', "")
           for uid, s in src_nodes.items()}
    uses = {name: SourceUse() for name in ids.values()}
    for uid, s in src_nodes.items():
        for col, meta in (s.get("columns") or {}).items():
            uses[ids[uid]].add(col, (meta.get("data_type") or "").upper() or None)

    # how each model's outputs map back to source columns
    outputs: dict[str, dict[str, tuple[str, str]]] = {}
    by_relation: dict[str, dict[str, tuple[str, str]]] = {}
    links: list[set[tuple[str, str]]] = []
    joins: list[tuple[tuple[str, str], tuple[str, str]]] = []

    def scan(uid: str) -> None:
        node = manifest["nodes"].get(uid)
        if uid in outputs or not node or node["resource_type"] != "model":
            return
        outputs[uid] = {}  # guards against cycles
        for parent in node["depends_on"]["nodes"]:
            scan(parent)
        if node.get("compiled_code"):
            outputs[uid] = scan_model(node["compiled_code"], uses, by_relation, links, joins)
        if node.get("relation_name"):
            by_relation[node["relation_name"].replace('"', "").upper()] = outputs[uid]

    for uid in manifest["nodes"]:
        scan(uid)

    # what is known about one column of a union holds for the columns aligned with it
    for _ in range(3):
        for group in links:
            members = [(uses[s], c) for s, c in group if s in uses]
            if any(c in u.numbers for u, c in members):
                for u, c in members:
                    u.numbers.add(c)
            if any(c in u.texts for u, c in members):
                for u, c in members:
                    u.texts.add(c)
            hint = next((u.columns.get(c) for u, c in members if u.columns.get(c)), None)
            for u, c in members:
                if hint and not u.columns.get(c):
                    u.columns[c] = hint

    def locate(node_id: str, column: str) -> tuple[str, str] | None:
        if node_id in ids:
            return ids[node_id], column.upper()
        known = outputs.get(node_id, {})
        if column.upper() not in known and "*" in known and column.upper() not in known["*"][1].split("|"):
            return known["*"][0], column.upper()  # a pass-through model: the column is the source's
        return known.get(column.upper())

    keys: dict[str, str] = {}
    fks: dict[tuple[str, str], tuple[str, str]] = {}
    accepted: dict[tuple[str, str], list] = {}
    for node in manifest["nodes"].values():
        meta = node.get("test_metadata")
        if node["resource_type"] != "test" or not meta or not node.get("attached_node"):
            continue
        column = node.get("column_name") or meta["kwargs"].get("column_name")
        here = locate(node["attached_node"], column) if column else None
        if not here:
            continue
        if meta["name"] == "unique":
            keys.setdefault(here[0], here[1])
        elif meta["name"] == "accepted_values":
            kw = meta["kwargs"]
            accepted[here] = (kw.get("arguments") or kw).get("values") or []
        elif meta["name"] == "relationships":
            kw = meta["kwargs"].get("arguments") or meta["kwargs"]
            others = [n for n in node["depends_on"]["nodes"] if n != node["attached_node"]]
            there = locate(others[0], kw.get("field", "")) if others else None
            if there and there[0] != here[0]:
                fks[here] = there

    tables: dict[str, dict] = {}
    assumed: list[str] = []
    keyless: list[str] = []
    for uid, name in ids.items():
        use, src = uses[name], src_nodes[uid]
        cols = dict(use.columns)
        for col, kind in catalog.view(name, tuple(cols)).items():  # a Snowflake system view: its real columns
            if kind or not cols.get(col):
                cols[col] = kind
            if kind:
                use.texts.discard(col)
        if not cols:
            # only ever read with `select *`: `init` learns the columns by running the models (see fit.py)
            cols = {"_LOADED_AT": None}
        loaded = (src.get("loaded_at_field") or (src.get("config") or {}).get("loaded_at_field") or "").upper()
        loaded = loaded if loaded in cols else first(LOADED_AT, cols)
        key = keys.get(name) or first([f"{name.split('.')[-1].rstrip('S')}_ID", "ID"], cols)
        event = first([c for c in EVENT_TIME if c != loaded], cols)
        updated = first(UPDATED_AT, cols)
        if not keys.get(name):
            (assumed if key else keyless).append(f"{name}.{key}" if key else name)
        out: dict[str, dict] = {}
        for col, hint in cols.items():
            if col in use.texts and not hint:
                hint = "VARCHAR"
            if col in use.numbers and not hint and col not in use.texts:
                hint = "BIGINT"
            if use.bounds.get(col) and not hint and col not in use.texts:
                # compared with numbers (`between 40 and 60`, `> 0.5`): a number
                hint = "BIGINT" if all(float(b).is_integer() for b in use.bounds[col]) else "DOUBLE"
            if col == key:
                numeric = (hint or "").upper().startswith(("BIGINT", "INT", "DECIMAL", "NUMBER")) or (not hint and col.endswith("ID"))
                out[col] = {"gen": "sequence" if numeric else "hash_id"}
            elif (name, col) in fks:
                out[col] = {"gen": "fk", "ref": fks[(name, col)][0], "skew": 1.5}
            elif (name, col) in accepted:
                values = accepted[(name, col)]
                out[col] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
            elif 0 < len(use.literals.get(col, ())) <= 30 and not hint_is_number(hint):
                # `where status = 'open'` should select something: use the values the SQL names
                named = sorted(use.literals[col])
                share = round(0.8 / len(named), 4)
                out[col] = {"gen": "categorical", "values": {**{v: share for v in named}, f"other_{col.lower()}": 0.2}}
            elif col in use.patterns and not hint_is_number(hint):
                # `where name like 'tap-%'` should keep some rows: make values that fit each pattern
                shapes = sorted(p.replace("\\", "").replace("%", "{hex}").replace("_", "x") for p in use.patterns[col])
                out[col] = {"gen": "template", "templates": [*shapes, f"{col.lower()}_{{hex}}"]}
            elif col in use.ips and not hint_is_number(hint):
                out[col] = {"gen": "ip", "cidr": "/" in (use.splits.get(col) or {}).get("seps", {})}  # parse_ip(network, 'INET')
            elif col in use.splits and not hint_is_number(hint):
                # the SQL takes this text apart: give every part it reads the type it is cast to
                found = use.splits[col]
                (sep, wanted), *others = found["seps"].items()
                shape = ["{n}" if found["digits"] or hint_is_number(wanted.get(i)) else "{hex}" for i in range(max(max(wanted) + 1, 2))]
                # other separators the SQL splits on come after: 123/456.json
                out[col] = {"gen": "template", "template": sep.join(shape) + "".join(f"{other}x" for other, _ in others)}
            elif col in use.paths:
                out[col] = {"gen": "object", "fields": {path: guess(path.split(".")[-1], h) for path, h in sorted(use.paths[col].items())}}
                if "id" in out[col]["fields"]:
                    out[col]["keyed_by"] = "id"  # {"id": 7, "login": "x"}: the login belongs to the id
                if col in use.json_text:
                    out[col]["type"] = "VARCHAR"
            elif col in use.arrays:
                out[col] = {"gen": "array"}
            elif col in use.formats:
                out[col] = {"gen": "time_offset", "format": use.formats[col], **WEEK}
            elif col == loaded:
                out[col] = {"gen": "loaded_at"}
            elif col == event:
                out[col] = {"gen": "event_time"}
            elif col == updated:
                out[col] = {"gen": "updated_at"}
            else:
                out[col] = guess(col, hint)
            within_bounds(out[col], use.bounds.get(col))
        # author_id + author_username: the name belongs to the id, so it must not vary between rows
        for col, d in out.items():
            if col.endswith("_ID") and d["gen"] in ("dist", "fk", "template"):
                for sibling, sd in out.items():
                    if sibling != col and sibling.startswith(col[:-2]) and sd["gen"] in ("template", "categorical"):
                        sd["follows"] = col
        tables[name] = {
            "volume": {"rows_per_day": 100_000, "weekly": [1, 1, 1, 1, 1, 1, 1]},  # placeholder: 100 rows a day at the default scale
            "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]},  # placeholder
            "columns": out,
        }
    if assumed:
        notes.append(f"{len(assumed)} source(s) have no `unique` test; assumed keys: {', '.join(assumed[:12])}{' ...' if len(assumed) > 12 else ''}.")
    if keyless:
        notes.append(f"{len(keyless)} source(s) have no `unique` test and no id-like column, so no key: "
                     f"{', '.join(keyless[:12])}{' ...' if len(keyless) > 12 else ''}.")
    # a column named like another table's key is almost always a reference to it
    guessed: list[str] = []
    key_owner = {c: t for t, spec in tables.items() for c, d in spec["columns"].items() if d["gen"] in ("sequence", "hash_id")}
    for name, spec in tables.items():
        for col, d in spec["columns"].items():
            owner = key_owner.get(col)
            if owner and owner != name and d["gen"] not in ("sequence", "hash_id", "fk"):
                spec["columns"][col] = {"gen": "fk", "ref": owner, "skew": 1.5}
                guessed.append(f"{name}.{col} -> {owner}")
    if guessed:
        notes.append(f"{len(guessed)} column(s) are named like another table's key and assumed to reference it (no relationships "
                     f"test says so): {'; '.join(guessed[:6])}{' ...' if len(guessed) > 6 else ''}.")
    joined: list[str] = []
    _share_join_values(tables, joins, joined)
    if joined:
        notes.append(f"{len(joined)} group(s) of columns are joined in models and generated to match, for example: {' '.join(joined[:3])}")
    # a foreign key needs its parent to have a key
    for (child, col), (parent, pcol) in fks.items():
        if child in tables and parent in tables and tables[parent]["columns"].get(pcol, {}).get("gen") not in ("sequence", "hash_id"):
            notes.append(f"{child}.{col}: references {parent}.{pcol}, which is not that table's key; generated as a plain column.")
            tables[child]["columns"][col] = guess(col, None)
    # INFORMATION_SCHEMA is the warehouse describing itself: the emulator answers it, nothing to generate
    tables = {name: t for name, t in tables.items() if name.split(".")[1] != "INFORMATION_SCHEMA"}
    if len(tables) > 200:
        # hundreds of sources: a smaller placeholder keeps a build to minutes and a few gigabytes
        rows = 20_000 if len(tables) <= 600 else 10_000
        for table in tables.values():
            table["volume"]["rows_per_day"] = rows
        notes.append(f"{len(tables)} source tables: placeholder volumes are {rows} rows a day ({rows // 1000} at the default scale) "
                     "to keep builds quick. `fauxflake profile` replaces them with what production has.")
    analysis = Analysis({"seed": 42, "start": "2025-01-01", "dbt": {"database": database}, "tables": tables}, notes, locate)
    analysis.bounds = {(name, col): numbers for name, use in uses.items() for col, numbers in use.bounds.items()}
    return analysis


HEADER = """# Written by `fauxflake init` from the dbt project. Keys, relationships and categories come
# from your tests; columns and types from how models use each source. Volumes, lateness and
# distributions are placeholders: run `fauxflake profile` to measure them, or edit by hand.
"""


_MISSING_ENV = re.compile(r"Env var required but not provided: '(\w+)'")


def provide_env(project) -> dict[str, str]:
    """Projects often read database and schema names from the environment. Parse until dbt
    stops asking, giving each missing variable a placeholder named after itself."""
    import os

    provided: dict[str, str] = {}
    for _ in range(200):
        if project.invoke(["parse"], silent=True):  # the errors are expected: they name the variables
            break
        missing = _MISSING_ENV.search(project.last_error)
        if not missing:
            raise RuntimeError(f"dbt could not parse the project:\n{project.last_error}")
        var = missing.group(1)
        value = re.sub(r"^(DBT_)?(SNOWFLAKE_)?((DATABASE|SCHEMA|DB)_)?", "", var.upper()) or "FAUXFLAKE"
        if re.search(r"SALT|SECRET|PASSWORD|TOKEN|_KEY|HASH", var.upper()):
            value = "A1B2C3D4E5F60718"  # secrets are often read as hex: to_binary(env_var('SALT'), 'HEX')
        provided[var] = os.environ[var] = value
    return provided


def unknown_functions(manifest: dict, emu) -> dict[str, set[int]]:  # noqa: ANN001
    """Functions the models call that the emulator does not have: your account's UDFs.
    name -> the numbers of arguments they are called with."""
    calls: dict[str, set[int]] = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or not node.get("compiled_code"):
            continue
        try:
            tree = sqlglot.parse_one(node["compiled_code"], read="snowflake")
        except Exception:  # noqa: BLE001
            continue
        for call in tree.find_all(exp.Anonymous):
            owner = call.parent
            while isinstance(owner, exp.Dot):  # information_schema.task_history(...)
                owner = owner.parent
            if isinstance(owner, (exp.Window, exp.TableFromRows, exp.Lateral, exp.Table)) or not isinstance(call.this, str):
                continue
            calls.setdefault(call.name.lower(), set()).add(len(call.expressions))
    conn = emu._fs.connect(database="FAUXFLAKE_PROBE", schema="PUBLIC")
    missing: dict[str, set[int]] = {}
    try:
        for name, arities in sorted(calls.items()):
            if not re.fullmatch(r"[a-z_][a-z0-9_$]*", name):
                continue
            cur = conn.cursor()
            try:
                cur.execute(f"select {name}({', '.join(['null'] * min(arities))})")
            except Exception as e:  # noqa: BLE001
                if re.search(r"Function with name \S+ does not exist", str(e)):
                    missing[name] = arities
            finally:
                cur.close()
    finally:
        conn.close()
    return missing


def udf_stubs(missing: dict[str, set[int]]) -> dict[str, str]:
    """A placeholder for each unknown function, so the project builds: every call returns NULL."""
    return {name: ", ".join(f"({', '.join(f'a{i}' for i in range(n))}) as null::varchar" for n in sorted(arities))
            for name, arities in missing.items()}


def write_spec(project, path: Path, database: str, emu=None, fit: bool = True, log=print) -> list[str]:  # noqa: ANN001
    """`project` is a DbtProject pointed at a running emulator. With `fit`, the first spec is then
    tried out: models are run and the project is built, and what the failures teach goes into the spec."""
    env = provide_env(project)
    manifest = project.compile()  # best effort: models that fail to compile are skipped for now
    analysis = analyse(manifest, database)
    spec, notes = analysis.spec, analysis.notes
    stubs = udf_stubs(unknown_functions(manifest, emu)) if emu is not None else {}
    if stubs:
        spec["udfs"] = stubs
    if env:
        spec["dbt"]["env"] = env
        notes.insert(0, f"set placeholders for {len(env)} environment variable(s) the project requires (dbt.env in the spec)")
    path.write_text(HEADER + yaml.safe_dump(spec, sort_keys=False, default_flow_style=None, width=110))
    if fit and emu is not None:
        notes += _fit(project, path, database, emu, manifest, analysis, log)
    elif stubs:
        notes.insert(0, _stub_note(stubs))
    return notes


def update_spec(project, path: Path, database: str, emu, log=print) -> list[str]:  # noqa: ANN001
    """Bring an existing spec up to date with the project, keeping everything already in it:
    sources and columns the models have started to read are added, then the spec is fitted again."""
    text = path.read_text()
    header = "".join(line + "\n" for line in text.splitlines() if line.startswith("#"))
    spec = yaml.safe_load(text)
    provide_env(project)
    manifest = project.compile()
    analysis = analyse(manifest, database)
    added: list[str] = []
    tables = spec.setdefault("tables", {})
    for name, table in analysis.spec["tables"].items():
        if name not in tables:
            tables[name] = table
            added.append(name)
            continue
        columns = tables[name].setdefault("columns", {})
        for col, gen in table["columns"].items():
            if col not in columns:
                columns[col] = gen
                added.append(f"{name}.{col}")
    known = spec.get("udfs") or {}
    stubs = {k: v for k, v in udf_stubs(unknown_functions(manifest, emu)).items() if k not in known}
    if stubs:
        spec["udfs"] = {**known, **stubs}
    path.write_text(header + yaml.safe_dump(spec, sort_keys=False, default_flow_style=None, width=110))
    notes = [f"added from the project's SQL: {', '.join(added[:12])}{' ...' if len(added) > 12 else ''}."] if added else []
    return notes + _fit(project, path, database, emu, manifest, analysis, log)


def _stub_note(stubs) -> str:  # noqa: ANN001
    return (f"{len(stubs)} function(s) are neither Snowflake built-ins the emulator knows nor created by the project "
            f"({', '.join(sorted(stubs))}). Each has a placeholder under `udfs` that returns NULL: "
            "replace it with DuckDB SQL that does the same job.")


def _fit(project, path: Path, database: str, emu, manifest: dict, analysis: Analysis, log) -> list[str]:  # noqa: ANN001
    from . import objects
    from .fit import Fitter

    fitter = Fitter(emu, path, log, bounds=analysis.bounds)
    models = [n for n in manifest["nodes"].values() if n["resource_type"] == "model"]
    log(f"trying the spec out on {len(models)} models")
    fitter.generate()
    project.invoke(["seed"], silent=True)
    failures = fitter.learn_from_models(manifest, database)
    for _ in range(3):
        if not failures and all(n.get("compiled_code") for n in models):
            break
        # models whose Jinja inspects the warehouse (dbt_utils.star, union_relations, get_column_values)
        # compile differently, or only at all, once the relations they look at exist: they do now
        before, known = len(failures), len(fitter.learned)
        manifest = project.compile()
        models = [n for n in manifest["nodes"].values() if n["resource_type"] == "model"]
        failures = fitter.learn_from_models(manifest, database)
        if len(failures) > before * 0.9 and (len(fitter.learned) == known or before - len(failures) < 3):
            break  # compiling again changes little now: the build takes it from here
    fitter.drop_models(manifest)
    summary = {} if os.environ.get("FAUXFLAKE_FIT_NO_BUILD") else fitter.learn_from_build(project, analysis)
    # a function the project creates itself (in a hook) needs no placeholder
    state = emu._fs.results_cache.get(objects._KEY)
    created = set(state.functions) if state else set()
    stubs = {k: v for k, v in (fitter.spec.get("udfs") or {}).items() if k not in created}
    if stubs:
        fitter.spec["udfs"] = stubs
    else:
        fitter.spec.pop("udfs", None)
    fitter.save()
    notes = [_stub_note(stubs)] if stubs else []
    if fitter.learned:
        notes.append(f"learned {len(fitter.learned)} thing(s) by running the project, for example: " + " ".join(fitter.learned[:5]))
    counts = summary.get("counts") or {}
    built = sum(v for k, v in counts.items() if k.startswith(("model:", "snapshot:", "seed:")) and k.endswith(":success"))
    total = sum(v for k, v in counts.items() if k.startswith(("model:", "snapshot:", "seed:")))
    tests = sum(v for k, v in counts.items() if k.startswith("test:"))
    passed = sum(v for k, v in counts.items() if k.startswith("test:") and k.endswith((":pass", ":warn", ":success")))
    expected = (fitter.spec.get("dbt") or {}).get("expected_failures") or []
    if counts:
        notes.append(f"on the generated data: {built}/{total} models, seeds and snapshots build; {passed}/{tests} tests pass.")
    if expected:
        notes.append(f"{len(expected)} test(s) state facts about production data that the generated data does not share. They are "
                     f"listed under dbt.expected_failures in the spec and left out of the build: teach the spec the rule and remove "
                     f"them from the list, or keep them there. For example: {', '.join(expected[:3])}.")
    for uid, status, message in (summary.get("failed") or [])[:25]:
        lines = [line.strip() for line in message.splitlines() if line.strip()]
        first = next((line for line in lines if not re.match(r"(Database|Runtime|Compilation) Error", line)), lines[0] if lines else "")
        notes.append(f"{'FAILS' if status == 'fail' else 'ERROR'} {uid.split('.', 2)[-1]}: {first[:200]}")
    if len(summary.get("failed") or []) > 25:
        notes.append(f"... and {len(summary['failed']) - 25} more: run `fauxflake build` to see them all.")
    return notes


def check(manifest: dict, spec, database: str) -> list[str]:
    """Problems that would make the project fail on the generated data: sources or columns
    the models read that the spec does not generate."""
    wanted, _ = build_spec(manifest, database)
    problems = []
    for name, table in wanted["tables"].items():
        have = spec.tables.get(name)
        if have is None:
            problems.append(f"{name}: used by the project, missing from the spec")
            continue
        for col in table["columns"]:
            if col not in have.columns:
                problems.append(f"{name}.{col}: used by the project, missing from the spec")
    return problems
