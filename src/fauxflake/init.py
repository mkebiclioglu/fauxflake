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

import collections
import csv
import itertools
import json
import os
import copy
import re
from datetime import date, timedelta
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
        # time columns that bound a period other rows are looked up in (`on t.at between p.start and p.end`): "start" or "end"
        self.periods: dict[str, str] = {}
        self.path_prefix: dict[str, dict[str, str]] = {}  # variant column -> path -> text stripped off before the rest is parsed
        self.truncs: dict[str, str] = {}  # time column -> the unit a model cuts it to before matching on it (`date_trunc('hour', at)`)
        self.series: set[frozenset[str]] = set()  # columns a join matches on together (`on a.id = b.id and a.hour = b.hour`)
        self.read_bounds: dict[str, set[float]] = {}  # number column -> what the models that read the table itself compare it with
        self.windows: dict[str, set[float]] = {}  # number column -> the range the model that reads the table itself keeps (`between a and b`)
        self.depends: dict[str, str] = {}  # column -> the column that decides it (`group by id having count(distinct seats) = 1`)
        self.examples: dict[str, set[str]] = {}  # column -> texts that match the regular expressions it is tested with
        # text column -> separator -> position -> shapes that part is kept by (`split_part(request, ' ', 1) = 'POST'`)
        self.part_shapes: dict[str, dict[str, dict[int, set[str]]]] = {}
        self.path_part_shapes: dict[str, dict[str, dict[str, dict[int, set[str]]]]] = {}  # the same, for a text inside a document
        self.path_literals: dict[str, dict[str, set[str]]] = {}  # variant column -> {"a.b": values it is compared with}
        self.path_patterns: dict[str, dict[str, set[str]]] = {}  # variant column -> {"a.b": LIKE patterns that keep rows}
        self.path_optional: dict[str, set[str]] = {}  # variant column -> paths a CASE / IFF tests for being there
        self.nullable: set[str] = set()  # columns a WHERE keeps only when they are NULL
        self.null_tests: set[str] = set()  # columns the SQL asks `is null` / `is not null` of, anywhere
        self.future: set[str] = set()  # time columns compared with the present from above (`expire_at >= current_date`)
        self.embeds: dict[str, list[tuple[str, str, tuple[str, str]]]] = {}  # text column -> (before, after, the column whose value sits between them)
        self.star = False

    def add(self, column: str, type_hint: str | None = None) -> None:
        column = column.upper()
        current = self.columns.get(column)
        # a cast to text says little (anything casts to text); a cast to a number or date says a lot
        if not current or (current.upper() in ("TEXT", "VARCHAR") and type_hint):
            self.columns[column] = type_hint or current


_DATE_LITERAL = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:[ T]00:00:00(?:\.0+)?)?")
# what the emulator answers for the session: a column compared with one of these holds that value
CONTEXT = {"CURRENT_ACCOUNT": "FAUXFLAKE", "CURRENT_ACCOUNT_NAME": "FAUXFLAKE", "CURRENT_ORGANIZATION_NAME": "FAUXFLAKE",
           "CURRENT_REGION": "LOCAL", "CURRENT_WAREHOUSE": "LOCAL"}
_PATHS = "\0paths"  # in a model's outputs: {column: (source, source column, path inside its document)}


def named_value(node: exp.Expression | None) -> str | None:
    """The text a comparison names: a string literal, or what the session answers (CURRENT_ACCOUNT_NAME())."""
    if isinstance(node, exp.Literal):
        return node.name if node.is_string and len(node.name) <= 60 else None
    if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
        return CONTEXT.get(node.sql_name().upper()) if not any(v for v in node.args.values()) else None
    if isinstance(node, exp.Anonymous) and not node.expressions:
        return CONTEXT.get(node.name.upper())
    return None


def path_text(path: tuple[str, ...]) -> str:
    """(chunk, transactions, [], hash) -> `chunk.transactions[].hash`."""
    out = ""
    for key in path:
        out += key if key == "[]" or key.startswith("~") else (("." if out else "") + key)
    return out


def _table_id(table: exp.Table) -> str:
    return ".".join(p.upper() for p in (table.catalog, table.db, table.name) if p)


def _belongs_inside(col: exp.Column, select: exp.Select, sources: dict) -> bool:
    """An unqualified column written in a subquery belongs to what that subquery reads. When that is a
    relation the project builds (`select max(ts) from {{ this }} where kind = ...`), the name says
    nothing about the tables the outer query reads."""
    inner = col.find_ancestor(exp.Select)
    if inner is None or inner is select:
        return False
    tables = [t for part in (inner.args.get("from") or inner.args.get("from_"), *(inner.args.get("joins") or ()))
              if part is not None for t in ([part.this] if isinstance(part.this, exp.Table) else [])]
    return any(t.db and _table_id(t) not in sources for t in tables)


def _key_pairs(join: exp.Join) -> list[exp.EQ]:
    """The `a.x = b.y` of a join's ON that match a column of one relation with a column of another."""
    on = join.args.get("on")
    return [e for e in (on.find_all(exp.EQ) if on is not None else [])
            if isinstance(e.this, exp.Column) and isinstance(e.expression, exp.Column) and e.this.table and e.expression.table
            and e.this.table.upper() != e.expression.table.upper()]


def _key_width(join: exp.Join) -> int:
    return len(_key_pairs(join))


def _like_patterns(test: exp.Expression) -> list[str]:
    """The patterns of `x like '%a%'` or `x like any ('%a%', '%b%')` that say something."""
    wanted = test.expression
    wanted = wanted.this if isinstance(wanted, exp.Any) else wanted
    wanted = wanted.unnest() if isinstance(wanted, (exp.Paren, exp.Subquery)) else wanted
    return [one.name for one in (wanted.expressions if isinstance(wanted, (exp.Tuple, exp.Array)) else [wanted])
            if isinstance(one, exp.Literal) and one.is_string and len(one.name) <= 60 and re.search(r"[^%_\s]", one.name)]


_REPLACE = tuple(getattr(exp, name) for name in ("Replace",) if hasattr(exp, name))
_REGEX_TESTS = tuple(getattr(exp, name) for name in ("RegexpLike", "RegexpILike", "RegexpExtract", "RegexpInstr", "RegexpCount")
                     if hasattr(exp, name))
_ARRAY_READERS = tuple(getattr(exp, name) for name in (
    "ArrayToString", "ArraySize", "ArrayContains", "ArrayOverlaps", "ArrayDistinct", "ArraySlice", "ArrayPosition", "ArrayConcat",
    "SortArray", "ArraySort", "ArrayCompact", "ArrayReverse", "ArrayMin", "ArrayMax", "ArrayFirst", "ArrayLast")
    if hasattr(exp, name))


def _is_condition(col: exp.Column) -> bool:
    """`where not is_deleted`, `iff(active, ...)`, `won = true`: the column is read as a truth value."""
    parent = col.parent
    if isinstance(parent, (exp.Not, exp.And, exp.Or, exp.Where, exp.Having)):
        return True
    if isinstance(parent, exp.If) and parent.this is col:
        return True
    if isinstance(parent, (exp.EQ, exp.NEQ, exp.Is)):
        other = parent.expression if parent.this is col else parent.this
        return isinstance(other, exp.Boolean)
    return False


_WRAPPERS = (exp.Cast, exp.TryCast, exp.Paren, exp.Lower, exp.Upper, exp.Trim)
_SAME_VALUE = (exp.Lower, exp.Upper, exp.Trim, exp.Initcap, exp.Nullif, exp.Lag, exp.Lead, exp.FirstValue, exp.LastValue, exp.Max,
               exp.Min, exp.AnyValue, exp.IgnoreNulls, exp.RespectNulls, exp.Window) + tuple(
    getattr(exp, name) for name in ("Replace",) if hasattr(exp, name))


def _parsed_as(node: exp.Expression) -> str | None:
    """TIMESTAMP or DATE when the value (possibly cast to text first) is handed to a function that
    reads it as a point in time: to_timestamp_ntz(v:header:timestamp::string)."""
    top, as_number = node, False
    while isinstance(top.parent, (exp.Cast, exp.TryCast, exp.Paren)):
        if not isinstance(top.parent, exp.Paren) and not top.parent.to.is_type(*exp.DataType.TEXT_TYPES):
            if not top.parent.to.is_type(*exp.DataType.NUMERIC_TYPES):
                break
            as_number = True  # to_timestamp(ts::int): seconds since 1970
        top = top.parent
    reader = top.parent
    if as_number:
        named = reader.name.upper() if isinstance(reader, exp.Anonymous) else reader.sql_name().upper() if isinstance(reader, exp.Func) else ""
        first = reader.expressions[0] if isinstance(reader, exp.Anonymous) and reader.expressions else getattr(reader, "this", None)
        return "EPOCH" if first is top and re.fullmatch(r"(TRY_)?TO_TIMESTAMP(_NTZ|_LTZ|_TZ)?|UNIX_TO_TIME", named) else None
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
    if isinstance(outer, (exp.ToNumber, exp.ToDouble)):
        hint = "BIGINT"  # try_to_decimal(split_part(version, '.', 1)): that piece is a number
    found = use.splits.setdefault(column, {"seps": {}, "digits": False})
    parts = found["seps"].setdefault(sep.name, {})
    position = int(index.name) - base
    parts[position] = (None if sliced else hint) or parts.get(position)
    found["digits"] |= sliced and hint_is_number(hint)
    return True


def scan_model(sql: str, sources: dict[str, SourceUse],
               models: dict[str, dict[str, tuple[str, str]]] | None = None,
               links: list[set[tuple[str, str]]] | None = None,
               joins: list[tuple[tuple[str, str], tuple[str, str]]] | None = None,
               dates: list[tuple[str, date]] | None = None,
               lookups: collections.Counter | None = None) -> dict[str, tuple[str, str]]:
    """Record source columns used by one model. Returns {output column: (source, source column)}
    for outputs that are a plain (possibly cast or renamed) source column."""
    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except sqlglot.errors.SqlglotError:
        return {}
    if dates is not None:
        # dates the SQL compares with: `= '2022-04-01'` has to fall inside the simulated history,
        # `< '2023-01-01'` after its start, `>= '2021-09-01'` before its end
        for test in tree.find_all(exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Between, exp.In):
            sides = [test.args.get("low"), test.args.get("high")] if isinstance(test, exp.Between) else \
                test.expressions if isinstance(test, exp.In) else [test.this, test.expression]
            for i, side in enumerate(sides):
                while isinstance(side, (exp.Cast, exp.TryCast, exp.Paren, exp.TsOrDsToDate, exp.Date)) and side.this is not None:
                    side = side.this
                m = _DATE_LITERAL.fullmatch(side.name) if isinstance(side, exp.Literal) and side.is_string else None
                if not m or not 2015 <= int(m.group(1)) <= 2035:
                    continue
                try:
                    day = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                except ValueError:
                    continue
                how = "in" if isinstance(test, (exp.EQ, exp.In)) else ("after" if i == 0 else "before") if isinstance(test, exp.Between) \
                    else "before" if isinstance(test, (exp.LT, exp.LTE)) == (i == 1) else "after"
                dates.append((how, day))
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
        explicit: dict[str, dict] = {}  # next to a model's star: the columns it wrote out under another name
        renames: dict[str, dict[str, tuple[str, str]]] = {}
        selected = {alias: src for alias, (_, src) in scope.selected_sources.items()}
        # inside LATERAL FLATTEN(input => t.col) the outer table is visible too
        selected.update({a: src for a, src in getattr(scope, "lateral_sources", {}).items() if a not in selected})
        for alias, node in selected.items():
            if isinstance(node, exp.Table):
                if _table_id(node) in sources and not (models and _table_id(node) in models):
                    # (a relation declared as a source that a model builds is read as that model, below)
                    direct[alias.upper()] = _table_id(node)
                elif node.name.upper() in passthrough:
                    direct[alias.upper()] = passthrough[node.name.upper()]
                    added[alias.upper()] = computed.get(node.name.upper(), set())
                elif models and _table_id(node) in models and "*" in models[_table_id(node)]:
                    # a model that is `select * from source`: reading it is reading the source
                    through, computed_there = models[_table_id(node)]["*"]
                    direct[alias.upper()] = through
                    added[alias.upper()] = set(computed_there.split("|")) - {""}
                    explicit[alias.upper()] = {k: v for k, v in models[_table_id(node)].items() if k not in ("*", _PATHS)}
                elif models and _table_id(node) in models:  # another model: follow its lineage
                    renames[alias.upper()] = models[_table_id(node)]
                elif not node.db and node.name.upper() in lineage:
                    # `with ACCT as (...) ... join acct`: one CTE, however the two spellings are capitalised
                    renames[alias.upper()] = lineage[node.name.upper()]
            else:  # a CTE or subquery scope
                # `from base_receipts r`: known by the CTE's name, used by its alias
                written = scope.selected_sources.get(alias, (None, None))[0]
                name, alias_name = (written.name.upper() if isinstance(written, exp.Table) else alias.upper()), alias.upper()
                if name not in passthrough and name not in lineage:
                    name = alias_name
                if name in passthrough:
                    direct[alias_name] = passthrough[name]
                    added[alias_name] = computed.get(name, set())
                elif name in lineage:
                    renames[alias_name] = lineage[name]
                elif id(getattr(node, "expression", None)) in outputs_of:
                    renames[alias_name] = outputs_of[id(node.expression)]  # `(select ...) as t`: what the subquery wrote
        # the generated tables this SELECT reads by their own name (not through a model or a CTE)
        read_here = {_table_id(node) for node in selected.values() if isinstance(node, exp.Table) and _table_id(node) in sources}
        # a lateral FLATTEN next to the table only contributes its own fixed columns
        laterals = {a.upper() for a, src in selected.items() if getattr(src, "is_udtf", False)}
        others = {a.upper() for a in selected} - set(direct) - laterals
        only = next(iter(direct.values())) if len(direct) == 1 and not renames and not others else None

        # `metadata$filename as _filename, split(_filename, '/')`: the second use is the alias, not a column
        aliases = {p.alias.upper(): p.this for p in select.expressions if isinstance(p, exp.Alias)
                   and not any(c.name.upper() == p.alias.upper() for c in p.this.find_all(exp.Column))}

        def resolve(col: exp.Column, depth: int = 0):
            table = col.table.upper()
            if not table and _belongs_inside(col, select, sources):
                return None
            if not table and col.name.upper() in aliases and not isinstance(col.parent, exp.Alias):
                inner = aliases[col.name.upper()]
                while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                    inner = inner.this
                return resolve(inner, depth + 1) if isinstance(inner, exp.Column) and depth < 4 else None
            if not table and laterals and col.name.upper() in FLATTEN_COLUMNS:
                return None
            if table in direct or (not table and only):
                owner = table or next(iter(direct))
                if col.name.upper() in explicit.get(owner, ()):
                    return explicit[owner][col.name.upper()]  # `o.id as sfid, o.*`: sfid is the source's id
                if col.name.upper() in added.get(owner, ()):
                    return None  # computed in the CTE (a row_number, say), not a source column
                return (direct.get(table) or only), col.name.upper()
            if table in renames:
                return through_star(renames[table], col.name.upper())
            if not table and len(renames) == 1 and len(selected) == 1:
                return through_star(next(iter(renames.values())), col.name.upper())
            if not table:  # next to a join: the one relation known to write a column of that name
                having = [r for r in renames.values() if col.name.upper() in r and col.name.upper() != _PATHS]
                return having[0][col.name.upper()] if len(having) == 1 else None
            return None

        def through_star(known: dict, name: str):  # noqa: ANN202
            """What a CTE says of a column: one it wrote out, or one of the source it passes through whole
            (`select s.*, p.sku from source s join products p ...`)."""
            hit = known.get(name)
            if hit is None and "*" in known and name not in (_PATHS, "*") and name not in known["*"][1].split("|"):
                return known["*"][0], name
            return hit

        # where a value sits inside a source's document, followed through models, aliases and FLATTEN
        path_maps = {a: r[_PATHS] for a, r in renames.items() if _PATHS in r}
        plain = [a.upper() for a in selected if a.upper() not in laterals]
        flatten_inputs: dict[str, exp.Expression] = {}
        for alias, src in selected.items():
            lateral = getattr(src, "expression", None)
            # `lateral flatten(...)` or `table(flatten(...))`: the same thing, written two ways
            call = lateral.this if isinstance(lateral, (exp.Lateral, exp.TableFromRows)) else None
            if isinstance(call, exp.Explode):
                given = call.this.expression if isinstance(call.this, exp.Kwarg) else call.this
                named = next((k.expression for k in call.find_all(exp.Kwarg) if k.this.name.upper() == "INPUT"), None)
                flatten_inputs[alias.upper() or "_FLATTENED"] = named if named is not None else given

        def into(base, keys):  # noqa: ANN001, ANN202
            """Follow `keys` into what is known of a value: a place in a source's document, or an
            object / array a model put together (OBJECT_CONSTRUCT, ARRAY_AGG) from such places."""
            for key in keys:
                if isinstance(base, dict):
                    base = base.get(key)
                elif isinstance(base, list):
                    base = base[0] if key == "[]" else None
                elif isinstance(base, tuple):
                    base = (base[0], base[1], base[2] + (key,))
                if base is None:
                    return None
            return base

        def resolve_path(node: exp.Expression, depth: int = 0):  # noqa: ANN202
            if depth > 12 or node is None:
                return None
            while isinstance(node, (exp.Cast, exp.TryCast, exp.Paren, exp.WithinGroup)):
                node = node.this
            if isinstance(node, (exp.ParseJSON, exp.TryBase64DecodeString, exp.Base64DecodeString)):
                # text inside a document that holds JSON, or base64 of something: `~json` / `~b64` say how the
                # value is written; what is read under them is its content
                base = resolve_path(node.this, depth + 1)
                mark = "~json" if isinstance(node, exp.ParseJSON) else "~b64"
                if isinstance(base, tuple) and base[2] and base[2][-1] != mark and "{}" not in base[2]:
                    return base[0], base[1], base[2] + (mark,)
                return base
            if isinstance(node, exp.JSONExtract):
                keys = []
                for k in node.expression.expressions if isinstance(node.expression, exp.JSONPath) else [None]:
                    if isinstance(k, exp.JSONPathKey):
                        keys.append(str(k.this))
                    elif isinstance(k, exp.JSONPathSubscript):
                        keys.append("[]")
                    elif not isinstance(k, exp.JSONPathRoot):
                        return None
                return into(resolve_path(node.this, depth + 1), keys)
            if isinstance(node, exp.Bracket):
                index = node.expressions[0] if node.expressions else None
                if not isinstance(index, exp.Literal):
                    return None
                taken = node.this
                for _ in range(2):  # `split(log, ' ') as parts ... parts[1]`: through the alias, and casts
                    while isinstance(taken, (exp.Cast, exp.TryCast, exp.Paren)):
                        taken = taken.this
                    if isinstance(taken, exp.Column) and not taken.table and taken.name.upper() in aliases:
                        taken = aliases[taken.name.upper()]
                if isinstance(taken, exp.Split) and index.is_int and isinstance(taken.expression, exp.Literal) \
                        and taken.expression.is_string and taken.expression.name:
                    # split(log, ' ')[1]: that piece of the text
                    base = resolve_path(taken.this, depth + 1)
                    if not isinstance(base, tuple) or base[0] not in sources or "{}" in base[2] or "~between" in base[2]:
                        return None
                    if "~part" in base[2]:  # a piece of a piece: its first part is still what a cast reads
                        return base if int(index.name) == 0 else None
                    return base[0], base[1], base[2] + ("~part", taken.expression.name, str(int(index.name)))
                return into(resolve_path(node.this, depth + 1), [index.name if index.is_string else "[]"])
            if isinstance(node, exp.If):  # iff(kind = 'a', v:msg, v): what the first answer that is known holds
                for value in (node.args.get("true"), node.args.get("false")):
                    found = resolve_path(value, depth + 1) if value is not None else None
                    if found is not None:
                        return found
                return None
            if isinstance(node, exp.Case):
                # case when a then 'fixed' else coalesce(x, y) end: what the first answer that is a column holds
                for value in [*(branch.args.get("true") for branch in node.args.get("ifs") or []), node.args.get("default")]:
                    found = resolve_path(value, depth + 1) if value is not None else None
                    if found is not None:
                        return found
                return None
            if isinstance(node, exp.Coalesce):
                for value in [node.this, *node.expressions]:
                    found = resolve_path(value, depth + 1)
                    if found is not None:
                        return found
                return None
            if isinstance(node, exp.SplitPart) and isinstance(node.this, exp.SplitPart):
                # split_part(split_part(text, '(<@', 2), '>)', 1): what the text holds between two marks
                inner, marks = node.this, []
                for call, wanted in ((inner, "2"), (node, "1")):
                    mark, index = call.args.get("delimiter"), call.args.get("part_index")
                    if isinstance(mark, exp.Literal) and mark.is_string and isinstance(index, exp.Literal) and index.name == wanted:
                        marks.append(mark.name)
                base = resolve_path(inner.this, depth + 1)
                if len(marks) == 2 and isinstance(base, tuple) and not base[2]:
                    return base[0], base[1], ("~between", marks[0], marks[1])
                return None
            if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
                # date_trunc('hour', started_at): still that time, on the hour
                base = resolve_path(node.this, depth + 1)
                unit = node.args.get("unit")
                if isinstance(base, tuple) and not base[2] and base[0] in sources and unit is not None and unit.name.lower() in ("hour", "day"):
                    sources[base[0]].truncs.setdefault(base[1], unit.name.lower())
                return base
            if isinstance(node, exp.SplitPart):
                # split_part(request, ' ', 1): that piece of the text
                mark, index = node.args.get("delimiter"), node.args.get("part_index")
                base = resolve_path(node.this, depth + 1)
                if isinstance(base, tuple) and not any(m in base[2] for m in ("{}", "~part", "~between")) and isinstance(mark, exp.Literal) \
                        and mark.is_string and mark.name and isinstance(index, exp.Literal) and index.is_int and int(index.name) >= 1:
                    return base[0], base[1], base[2] + ("~part", mark.name, str(int(index.name) - 1))
                return None
            if isinstance(node, _REPLACE) and isinstance(node.expression, exp.Literal) and node.expression.is_string \
                    and node.expression.name and (node.args.get("replacement") is None or node.args["replacement"].name == ""):
                # try_parse_json(replace(value, 'EVENT_JSON:')): the text is that mark, then JSON (used only where
                # the rest is in fact parsed)
                base = resolve_path(node.this, depth + 1)
                if isinstance(base, tuple) and base[2] and base[0] in sources and "{}" not in base[2]:
                    sources[base[0]].path_prefix.setdefault(base[1], {})[path_text(base[2])] = node.expression.name
                return base
            if isinstance(node, _SAME_VALUE):
                # lower(x), replace(x, 'overage-', ''), lag(x) over (...): still one of x's values, give or take
                # its spelling, so what the result is compared with is what x should hold
                return resolve_path(node.this, depth + 1)
            if isinstance(node, exp.Struct):  # object_construct('k', v, ...)
                made = {e.this.name: resolve_path(e.expression, depth + 1) for e in node.expressions
                        if isinstance(e, exp.PropertyEQ) and isinstance(e.this, exp.Literal)}
                made = {k: v for k, v in made.items() if v is not None}
                return made or None
            if isinstance(node, exp.ArrayAgg):
                inner = resolve_path(node.this, depth + 1)
                return [inner] if inner is not None else None
            if isinstance(node, exp.Array) and node.expressions:
                inner = resolve_path(node.expressions[0], depth + 1)
                return [inner] if inner is not None else None
            if not isinstance(node, exp.Column) or isinstance(node.this, exp.Star):
                return None
            table, name = node.table.upper(), node.name.upper()
            if table in flatten_inputs or (not table and name in FLATTEN_COLUMNS and flatten_inputs):
                which = table if table in flatten_inputs else next(iter(flatten_inputs)) if len(flatten_inputs) == 1 else None
                base = resolve_path(flatten_inputs[which], depth + 1) if which and name in ("VALUE", "THIS", "KEY") else None
                if not base:
                    return None
                # `{}` stands for "the name of each key": what a FLATTEN over an object calls KEY
                return base if name == "THIS" else into(base, ["{}" if name == "KEY" else "[]"])
            if not table and name in aliases and not isinstance(node.parent, exp.Alias):
                return resolve_path(aliases[name], depth + 1)
            owner = table or (plain[0] if len(plain) == 1 else "")
            if not owner:
                # an unqualified column next to a join: the one relation known to write it (were it also a
                # column of a table read whole beside it, the SQL would not say which it meant)
                having = [a for a in plain if name in path_maps.get(a, ()) or name in renames.get(a, ())]
                owner = having[0] if len(having) == 1 else ""
            if name in path_maps.get(owner, ()):
                return path_maps[owner][name]
            hit = resolve(node)
            if hit and len(hit) == 2 and isinstance(hit[1], str):
                return hit[0], hit[1], ()
            if not table and owner in direct and name not in added.get(owner, ()):
                return direct[owner], name, ()
            if renames.get(owner, {}).get(name) and name != _PATHS:
                return (*renames[owner][name], ())
            return None

        for node in select.find_all(exp.JSONExtract, exp.Bracket, exp.Column):
            if isinstance(node.parent, (exp.JSONExtract, exp.Bracket)) and node.parent.this is node:
                continue  # part of a longer path: that one is recorded
            if node.find_ancestor(exp.Select) is not select:
                continue
            found = resolve_path(node)
            if not isinstance(found, tuple) or found[0] not in sources:
                continue
            top = node
            while isinstance(top.parent, exp.Paren):
                top = top.parent
            reader = top.parent
            cast = reader if isinstance(reader, (exp.Cast, exp.TryCast)) else None
            hint = _parsed_as(top) or (cast.to.sql(dialect="duckdb") if cast else None)
            outer = cast.parent if cast is not None else reader
            flattened = isinstance(outer, (exp.Explode, exp.ArraySize)) or (
                isinstance(outer, exp.Kwarg) and isinstance(outer.parent, exp.Explode))
            if found[2][:1] == ("~between",):
                # joined to another column: the text mentions that column's values between the two marks
                side = cast if cast is not None else top
                while isinstance(side.parent, _WRAPPERS):
                    side = side.parent
                if isinstance(side.parent, exp.EQ):
                    other = side.parent.expression if side.parent.this is side else side.parent.this
                    there = resolve_path(other) if not isinstance(other, exp.Literal) else None
                    if isinstance(there, tuple) and there[0] in sources and not there[2]:
                        sources[found[0]].embeds.setdefault(found[1], []).append((found[2][1], found[2][2], (there[0], there[1])))
                continue
            if "~part" in found[2]:
                # a piece of a text compared with something: the text should have such a piece there
                at = found[2].index("~part")
                if len(found[2]) != at + 3:
                    continue
                inside, found = found[2][:at], (found[0], found[1], found[2][at:])  # (the text may sit inside a document)
                held = cast if cast is not None else top
                while isinstance(held.parent, _WRAPPERS):
                    held = held.parent
                test, shapes = held.parent, set()
                if isinstance(test, (exp.EQ, exp.In)) and held in (test.this, test.args.get("expression")) \
                        and not isinstance(test.parent, exp.Not):
                    values = test.expressions if isinstance(test, exp.In) else [test.expression if test.this is held else test.this]
                    shapes = {v for v in map(named_value, values) if v}
                elif isinstance(test, (exp.Like, exp.ILike)) and test.this is held and not isinstance(test.parent, exp.Not):
                    shapes = {like_shape(one) for one in _like_patterns(test)}
                elif isinstance(test, exp.RegexpLike) and test.this is held and isinstance(test.expression, exp.Literal) \
                        and not isinstance(test.parent, exp.Not):
                    shapes = set(regex_examples(test.expression.name))
                if inside and cast is not None and hint_is_number(cast.to.sql(dialect="duckdb")):
                    shapes.add("{n}")  # split(log, ' ')[2]::number (for a column of its own, `splits` records the cast)
                shapes = {v for v in shapes if found[2][1] not in v and len(v) <= 60}  # a piece cannot hold the separator
                if shapes and not inside:
                    sources[found[0]].part_shapes.setdefault(found[1], {}).setdefault(found[2][1], {}).setdefault(
                        int(found[2][2]), set()).update(shapes)
                    sources[found[0]].texts.add(found[1])
                elif shapes:
                    sources[found[0]].path_part_shapes.setdefault(found[1], {}).setdefault(path_text(inside), {}).setdefault(
                        found[2][1], {}).setdefault(int(found[2][2]), set()).update(shapes)
                if inside:
                    sources[found[0]].paths.setdefault(found[1], {}).setdefault(path_text(inside), None)
                continue
            use, text = sources[found[0]], path_text(found[2])
            if joins is not None and "{}" not in found[2]:
                # a.x = b.y, where either side may sit inside a document: the two must share values
                side = cast if cast is not None else top
                while isinstance(side.parent, _WRAPPERS):
                    side = side.parent
                if isinstance(side.parent, exp.EQ):
                    other = side.parent.expression if side.parent.this is side else side.parent.this
                    there = resolve_path(other) if not isinstance(other, exp.Literal) else None
                    if isinstance(there, tuple) and there[0] in sources and "{}" not in there[2] and "~between" not in there[2] and "~part" not in there[2] \
                            and (there[0], there[1], there[2]) != found:
                        mine, theirs = (found[0], found[1], text), (there[0], there[1], path_text(there[2]))
                        if found[2] or there[2] or not resolve(node):  # two plain columns of sources are recorded below
                            joins.append((mine, theirs))
                        join = side.find_ancestor(exp.Join)
                        if lookups is not None and join is not None and join.find_ancestor(exp.Select) is select:
                            # `from a join b on a.x = b.y`: b is looked up by y; a row of a should find one b, not thirty
                            joined = (join.this.alias_or_name or "").upper()
                            tables_of = lambda e: {c.table.upper() for c in e.find_all(exp.Column)}  # noqa: E731
                            if tables_of(side) == {joined} and joined not in tables_of(other) and _key_width(join) == 1:
                                # (matched on two columns, neither is the table's key by itself)
                                lookups[mine] += 1  # (the other side is visited too, and counts nothing)
                                lookups[theirs] -= 1
            if not found[2]:
                if flattened:
                    use.arrays.add(found[1])
                # a source column reached through other models: what it is compared with counts as much
                # as in the model that reads the source
                held = cast if cast is not None else top
                while isinstance(held.parent, (*_WRAPPERS, exp.Max, exp.Min)):  # `having max(expire_at) >= current_date`
                    held = held.parent
                test = held.parent
                if isinstance(test, exp.Is) and isinstance(test.expression, exp.Null) and test.this is held:
                    use.null_tests.add(found[1])
                if isinstance(test, exp.Is) and isinstance(test.expression, exp.Null) and test.this is held \
                        and not isinstance(test.parent, exp.Not) and test.find_ancestor(exp.Where) is not None \
                        and test.find_ancestor(exp.Or, exp.If, exp.Case) is None:
                    use.nullable.add(found[1])  # `where converted_at is null`: some rows have none
                if isinstance(test, (exp.GT, exp.GTE)) and test.this is held or isinstance(test, (exp.LT, exp.LTE)) and test.expression is held:
                    now = test.expression if test.this is held else test.this
                    if isinstance(now, (exp.CurrentDate, exp.CurrentTimestamp)) or (
                            isinstance(now, exp.Anonymous) and now.name.upper() in ("SYSDATE", "GETDATE", "LOCALTIMESTAMP")):
                        use.future.add(found[1])  # `expire_at >= current_date`: some of these lie ahead
                if isinstance(test, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.Between)):
                    sides = [test.args.get("low"), test.args.get("high")] if isinstance(test, exp.Between) else \
                        [test.expression if test.this is held else test.this]
                    for side in sides:
                        negative = isinstance(side, exp.Neg)
                        number = side.this if negative else side
                        if isinstance(number, exp.Literal) and not number.is_string:
                            try:
                                use.bounds.setdefault(found[1], set()).add(-float(number.name) if negative else float(number.name))
                            except ValueError:
                                pass
                if isinstance(test, (exp.EQ, exp.In)) and held in (test.this, test.args.get("expression")) \
                        and not isinstance(test.parent, exp.Not):
                    values = test.expressions if isinstance(test, exp.In) else [test.expression if test.this is held else test.this]
                    named = {v for v in map(named_value, values) if v is not None and not re.fullmatch(r"[\d\s:.\-/T+Z]*", v)}
                    if named:
                        use.literals.setdefault(found[1], set()).update(named)
                        use.texts.add(found[1])
                if isinstance(test, (exp.Like, exp.ILike)) and test.this is held and not isinstance(test.parent, exp.Not) \
                        and _like_patterns(test):
                    use.patterns.setdefault(found[1], set()).update(_like_patterns(test))
                    use.texts.add(found[1])
                continue
            known = use.paths.setdefault(found[1], {})
            listed = cast.parent if cast is not None and cast.to.is_type("variant") else reader
            if isinstance(listed, _ARRAY_READERS) and listed.this in (cast, top):
                flattened = True  # array_sort(doc['credits']::variant), array_size(doc:tags): a list
            if flattened:
                known.setdefault(text + "[]", None)
                continue
            compared = cast if cast is not None else top
            while isinstance(compared.parent, _WRAPPERS):
                compared = compared.parent
            test = compared.parent
            named: set[str] = set()
            if isinstance(test, (exp.EQ, exp.In)) and compared in (test.this, test.args.get("expression")) \
                    and not isinstance(test.parent, exp.Not):
                values = test.expressions if isinstance(test, exp.In) else [test.expression if test.this is compared else test.this]
                named = {v for v in map(named_value, values) if v is not None}
            if "{}" in found[2]:
                # flatten(obj) ... where key = 'FunctionCall': the object has a key of that name
                if found[2][-1] == "{}":
                    for value in named:
                        known.setdefault(path_text(found[2][:-1] + (value,)), None)
                continue
            if isinstance(test, _REGEX_TESTS) and test.this is compared and isinstance(test.expression, exp.Literal) \
                    and not isinstance(test.parent, exp.Not):
                # regexp_substr(v:status, 'Success'): texts the expression finds
                flag = test.args.get("flag") or test.args.get("parameters")
                named |= set(regex_examples(test.expression.name, whole=isinstance(test, exp.RegexpLike),
                                            ignore_case=isinstance(test, exp.RegexpILike) or (flag is not None and "i" in flag.name)))
            previous = known.get(text)
            if previous and hint and previous != "[]" and {hint_is_number(previous), hint_is_number(hint)} == {True, False} \
                    and (hint if hint_is_number(previous) else previous).upper().startswith(("TIMESTAMP", "DATE", "EPOCH")):
                hint = "EPOCH"  # `point[0]::float` and `point[1]::timestamp`: numbers, one of them seconds since 1970
            if previous == "EPOCH":
                hint = "EPOCH"
            if (hint or text not in known) and not (known.get(text) and str(hint or "").upper() in ("TEXT", "VARCHAR", "STRING")):
                # (a cast to text says little, and never unsays a cast to a number or a date: a surrogate key
                # casts every column it hashes to text)
                known[text] = hint or known.get(text)
            if named:
                use.path_literals.setdefault(found[1], {}).setdefault(text, set()).update(named)
            if isinstance(test, exp.Is) and isinstance(test.expression, exp.Null) and test.this is compared \
                    and test.find_ancestor(exp.If, exp.Case) is not None:
                # iff(status:Failure is not null, 'Fail', 'Success'): both answers occur only if the key is sometimes absent
                use.path_optional.setdefault(found[1], set()).add(text)
            if isinstance(test, (exp.Like, exp.ILike)) and test.this is compared and not isinstance(test.parent, exp.Not) \
                    and _like_patterns(test):
                use.path_patterns.setdefault(found[1], {}).setdefault(text, set()).update(_like_patterns(test))

        if joins is not None:
            for join in select.args.get("joins") or []:
                for named in join.args.get("using") or []:
                    places = []
                    for owner in plain:
                        there = resolve_path(exp.column(named.name, table=owner))
                        if isinstance(there, tuple) and there[0] in sources and "{}" not in there[2] and "~between" not in there[2] and "~part" not in there[2]:
                            places.append((there[0], there[1], path_text(there[2])))
                    for one, two in zip(places, places[1:]):
                        if one != two:
                            joins.append((one, two))

        for join in select.args.get("joins") or []:
            # `join releases r on i.created_at >= r.window_start and i.created_at <= r.release_date`: each row of `i`
            # is looked up in the periods of `r`, which follow one another (a row in two periods would be counted twice)
            pairs = _key_pairs(join)
            if len(pairs) >= 2:
                # matched on two columns or more: together they name one row of each side (an entity and an hour)
                sides: dict[tuple[str, str], set[str]] = {}
                for column in (c for pair in pairs for c in (pair.this, pair.expression)):
                    spot = resolve_path(column)
                    if isinstance(spot, tuple) and not spot[2] and spot[0] in sources:
                        sides.setdefault((column.table.upper(), spot[0]), set()).add(spot[1])
                joined = (join.this.alias_or_name or "").upper()
                for (alias, source), columns in sides.items():
                    if len(columns) >= 2 and alias == joined:  # the side that is looked up, not the rows looking
                        sources[source].series.add(frozenset(columns))
            on = join.args.get("on")
            lower, upper = {}, {}  # table that bounds from below / above -> the bounding expressions
            keyed = on is not None and any(
                isinstance(e.this, exp.Column) and isinstance(e.expression, exp.Column) and e.this.table != e.expression.table
                for e in on.find_all(exp.EQ))  # `on a.id = b.id and a.day between b.valid_from and b.valid_to`: periods of one id
            for test in (on.find_all(exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between) if on is not None and not keyed else []):
                if isinstance(test, exp.Between):
                    pairs = [(test.args.get("low"), lower), (test.args.get("high"), upper)]
                    probe = test.this
                else:
                    above = isinstance(test, (exp.GT, exp.GTE))
                    pairs, probe = [(test.expression, lower if above else upper)], test.this
                for bound, role in pairs:
                    owners = {c.table.upper() for c in bound.find_all(exp.Column)} if bound is not None else set()
                    probes = {c.table.upper() for c in probe.find_all(exp.Column)}
                    # what is looked up is a time the sources hold (an issue's creation), not a calendar's day: a
                    # calendar joined to periods spreads each period over its days, which is another thing
                    looked_up = [resolve_path(c) for c in probe.find_all(exp.Column)]
                    if not looked_up or not all(isinstance(spot, tuple) and spot[0] in sources for spot in looked_up):
                        continue
                    if any(_CALENDAR.search(spot[0].rsplit(".", 1)[-1]) or spot[1] in _CALENDAR_DAYS for spot in looked_up):
                        continue  # (a calendar that is itself a generated table: `join dates d on d.date between start and end`)
                    if len(owners) == 1 and probes and not owners & probes and "" not in owners:
                        role.setdefault(next(iter(owners)), []).append(bound)
            for owner in set(lower) & set(upper):
                for role, bounds in (("start", lower[owner]), ("end", upper[owner])):
                    for column in (c for bound in bounds for c in bound.find_all(exp.Column)):
                        spot = resolve_path(column)
                        if isinstance(spot, tuple) and not spot[2] and spot[0] in sources:
                            sources[spot[0]].periods.setdefault(spot[1], role)

        having, group = select.args.get("having"), select.args.get("group")
        if having is not None and group is not None and len(group.expressions) == 1:
            # `group by license_id having count(distinct seats) = 1`: rows are kept when a license has one number
            # of seats, so the seats are decided by the license
            by = group.expressions[0]
            if isinstance(by, exp.Literal) and by.is_int and 0 < int(by.name) <= len(select.expressions):
                by = select.expressions[int(by.name) - 1].unalias()
            key = resolve_path(by)
            for test in having.find_all(exp.EQ):
                count, one = test.this, test.expression
                if not (isinstance(count, exp.Count) and isinstance(count.this, exp.Distinct) and isinstance(one, exp.Literal)
                        and one.name == "1" and len(count.this.expressions) == 1 and isinstance(key, tuple) and not key[2]):
                    continue
                decided = resolve_path(count.this.expressions[0])
                if isinstance(decided, tuple) and not decided[2] and decided[0] == key[0] and decided[1] != key[1] and decided[0] in sources:
                    sources[decided[0]].depends.setdefault(decided[1], key[1])

        if joins is not None:
            # `where server_id in (select id from servers)`: a join by another name
            for test in select.find_all(exp.In):
                query = test.args.get("query")
                inner = query.unnest() if query is not None else None
                if not isinstance(inner, exp.Select) or isinstance(test.parent, exp.Not) or test.find_ancestor(exp.Select) is not select:
                    continue
                mine, theirs = resolve_path(test.this), (positional.get(id(inner)) or [None])[0]
                if isinstance(mine, tuple) and mine[0] in sources and "{}" not in mine[2] and "~between" not in mine[2] and "~part" not in mine[2] and theirs:
                    one, two = (mine[0], mine[1], path_text(mine[2])), (theirs if len(theirs) == 3 else (*theirs, ""))
                    if one != two:
                        joins.append((one, two))

        for col in scope.columns:
            hit = resolve(col)
            if hit:
                cast = col.parent if isinstance(col.parent, (exp.Cast, exp.TryCast)) else None
                use, parent = sources[hit[0]], col.parent
                use.add(hit[1], cast.to.sql(dialect="duckdb") if cast else "BOOLEAN" if _is_condition(col) else None)
                if not cast and _parsed_as(col) and not use.columns.get(hit[1]):
                    use.columns[hit[1]] = _parsed_as(col)
                elif cast and _parsed_as(col) == "EPOCH":
                    use.columns[hit[1]] = "EPOCH"  # cast to a number only to be read as a point in time
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
                    read = []
                    for side in sides:
                        negative = isinstance(side, exp.Neg)
                        number = side.this if negative else side
                        if isinstance(number, exp.Literal) and not number.is_string:
                            try:
                                read.append(-float(number.name) if negative else float(number.name))
                            except ValueError:
                                pass
                    use.bounds.setdefault(hit[1], set()).update(read)
                    if hit[0] in read_here:
                        use.read_bounds.setdefault(hit[1], set()).update(read)
                    if isinstance(parent, exp.Between) and len(read) == 2 and hit[0] in read_here and not isinstance(parent.parent, exp.Not):
                        # the model that reads the table itself takes only this range: nothing outside it gets any further
                        use.windows.setdefault(hit[1], set()).update(read)
                if (isinstance(parent, exp.ParseIp) and parent.this is col) or (
                        isinstance(parent, exp.Anonymous) and parent.name.upper() == "PARSE_IP" and parent.expressions[0] is col):
                    use.ips.add(hit[1])
                if isinstance(parent, exp.Nullif) and parent.this is col and isinstance(parent.expression, exp.Literal) \
                        and parent.expression.is_string and not cast:
                    use.texts.add(hit[1])  # nullif(col, ''): text that can be empty
                if isinstance(parent, exp.ConvertTimezone) and col in (parent.args.get("source_tz"), parent.args.get("target_tz")):
                    use.literals.setdefault(hit[1], set()).update({"UTC", "America/Los_Angeles", "Europe/Berlin"})  # a time zone name
                    use.texts.add(hit[1])
                tested = col
                while isinstance(tested.parent, _WRAPPERS):
                    tested = tested.parent
                regex = tested.parent
                if isinstance(regex, _REGEX_TESTS) \
                        and regex.this is tested and isinstance(regex.expression, exp.Literal) and not isinstance(regex.parent, exp.Not):
                    # rlike(uri, '^/[0-9]+/.*'): some rows should pass
                    flag = regex.args.get("flag") or regex.args.get("parameters")
                    made = regex_examples(regex.expression.name, whole=isinstance(regex, exp.RegexpLike),
                                          ignore_case=isinstance(regex, exp.RegexpILike) or (flag is not None and "i" in flag.name))
                    if made:
                        use.examples.setdefault(hit[1], set()).update(made)
                        use.texts.add(hit[1])
                if isinstance(parent, exp.ParseJSON):
                    use.json_text.add(hit[1])  # parse_json(col):a.b  ->  col is text holding {"a": {"b": ...}}
                    col, parent = parent, parent.parent
                    if not (isinstance(parent, exp.JSONExtract) and parent.this is col):
                        use.paths.setdefault(hit[1], {})
                elif isinstance(parent, (exp.EQ, exp.NEQ, exp.Like, exp.ILike, exp.In)):
                    if isinstance(parent, (exp.Like, exp.ILike)) and parent.this is col and isinstance(parent.expression, exp.Any) \
                            and not isinstance(parent.parent, exp.Not) and _like_patterns(parent):
                        use.patterns.setdefault(hit[1], set()).update(_like_patterns(parent))  # name LIKE ANY ('tap-%', 'target-%')
                        use.texts.add(hit[1])
                    other = parent.expression if not isinstance(parent, exp.In) else (parent.expressions or [None])[0]
                    other = parent.this if other is col else other
                    if isinstance(parent, exp.EQ) and not isinstance(other, exp.Literal) and named_value(other):
                        use.literals.setdefault(hit[1], set()).add(named_value(other))  # account_name = current_account_name()
                        use.texts.add(hit[1])
                    if isinstance(other, exp.Literal) and other.is_string and not re.fullmatch(r"[\d\s:.\-/T+Z]*", other.name):
                        use.texts.add(hit[1])  # id = 'USLACKBOT': not a number
                        if isinstance(parent, (exp.Like, exp.ILike)) and parent.this is col and len(other.name) <= 60 \
                                and not isinstance(parent.parent, exp.Not) and re.search(r"[A-Za-z0-9]", other.name):
                            use.patterns.setdefault(hit[1], set()).add(other.name)  # name LIKE 'tap-%'
                        # `label != '(not set)'` names a value to leave out: not one to generate rows with
                        if isinstance(parent, (exp.EQ, exp.In)) and not isinstance(parent.parent, exp.Not):
                            compared = parent.expressions if isinstance(parent, exp.In) else [other]
                            use.literals.setdefault(hit[1], set()).update(
                                v.name for v in compared if isinstance(v, exp.Literal) and v.is_string and len(v.name) <= 60)
                if isinstance(parent, exp.JSONExtract) and parent.this is col:
                    # v:a.b::int tells us the column is a variant holding {"a": {"b": <int>}}
                    keys = [k.name for k in parent.expression.expressions if isinstance(k, exp.JSONPathKey)]
                    if any(isinstance(k, exp.JSONPathSubscript) for k in parent.expression.expressions):
                        keys = []  # v:items[0].sku: recorded above, as a path through an array
                        use.paths.setdefault(hit[1], {})
                    outer = parent.parent
                    hint = outer.to.sql(dialect="duckdb") if isinstance(outer, (exp.Cast, exp.TryCast)) else None
                    hint = _parsed_as(parent) or hint
                    if keys:
                        known = use.paths.setdefault(hit[1], {})
                        previous = known.get(".".join(keys))
                        if previous and hint and {hint_is_number(previous), hint_is_number(hint)} == {True, False} \
                                and (hint if hint_is_number(previous) else previous).upper().startswith(("TIMESTAMP", "DATE", "EPOCH")):
                            hint = "EPOCH"  # cast to a number here, read as a point in time there: seconds since 1970
                        weak = previous and str(hint or "").upper() in ("TEXT", "VARCHAR", "STRING")
                        known[".".join(keys)] = "EPOCH" if previous == "EPOCH" else (previous if weak else hint or previous)
                    elif not any(isinstance(k, exp.JSONPathSubscript) for k in parent.expression.expressions):
                        use.arrays.add(hit[1])
                elif isinstance(parent, (exp.Split, exp.SplitPart)) and parent.this is col and _split_use(use, hit[1], parent):
                    pass  # split(col, '/')[0]::number: the text has parts, and this one is a number
                elif isinstance(parent, exp.Bracket) or col.find_ancestor(exp.Explode, exp.Lateral, exp.Unnest) is not None \
                        or (isinstance(parent, _ARRAY_READERS) and parent.this is col):
                    use.arrays.add(hit[1])  # array_to_string(tags, ','), array_size(tags): a list
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
            placed = None if hit or isinstance(proj, exp.Star) else resolve_path(proj.unalias())
            by_position.append(hit or ((placed[0], placed[1], path_text(placed[2])) if isinstance(placed, tuple) and placed[0] in sources
                                       and "{}" not in placed[2] and "~between" not in placed[2] and "~part" not in placed[2] else None))
            if hit:
                outputs[proj.alias_or_name.upper()] = hit
        paths_out: dict[str, tuple[str, str, tuple[str, ...]]] = {}
        if stars and len(plain) == 1 and plain[0] in path_maps:
            paths_out.update(path_maps[plain[0]])  # `select *` (next to a FLATTEN or not) keeps what is known
        for proj in select.expressions:
            if isinstance(proj, exp.Star) or (isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)):
                continue
            found = resolve_path(proj.unalias())
            if isinstance(found, (dict, list)) or (
                    found and found[0] in sources and (found[2] or proj.alias_or_name.upper() not in outputs)):
                paths_out[proj.alias_or_name.upper()] = found
        if paths_out:
            outputs[_PATHS] = paths_out
        holder = scope.expression.parent
        # a CTE, or `(select * from source where kind = 'INV') inv`: known by that name from here on
        name = holder.alias.upper() if isinstance(holder, exp.CTE) or (isinstance(holder, exp.Subquery) and holder.alias) else None
        if stars and only and not select.args.get("joins"):
            if name:  # `select *, row_number() ... as rn from source` still passes every column through
                passthrough[name] = only
                computed[name] = {p.alias_or_name.upper() for p in select.expressions if p not in stars} \
                    | added.get(next(iter(direct)), set())
            else:
                sources[only].star = True
                extra = {p.alias_or_name.upper() for p in select.expressions if p not in stars} | added.get(next(iter(direct)), set())
                outputs["*"] = (only, "|".join(sorted(extra)))  # the whole model passes the source through
        elif len(stars) == 1 and isinstance(stars[0], exp.Column) and stars[0].table.upper() in direct \
                and (name or scope.is_root) and not isinstance(select.parent, exp.SetOperation):
            # `select o.id as sfid, o.* from source o join (latest per id) oo on ...`: still every column of `o`
            owner = stars[0].table.upper()
            sources[direct[owner]].star = True
            extra = {p.alias_or_name.upper() for p in select.expressions if p not in stars} | added.get(owner, set())
            outputs["*"] = (direct[owner], "|".join(sorted(extra - set(outputs))))
        if stars and len(renames) == 1 and len(selected) == 1:
            outputs = {**next(iter(renames.values())), **outputs}
            if paths_out:
                outputs[_PATHS] = paths_out
        else:
            # `select base.*, other.x from base join other ...`: everything known of `base` passes through
            for star in stars:
                through = renames.get(star.table.upper()) if isinstance(star, exp.Column) and star.table else None
                if through:
                    outputs = {**{k: v for k, v in through.items() if k != _PATHS}, **outputs}
                    if through.get(_PATHS):
                        outputs[_PATHS] = {**through[_PATHS], **(outputs.get(_PATHS) or {})}
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
            parts = branches(union)
            names = [[p.alias_or_name.upper() for p in b.expressions] if isinstance(b, exp.Select) else [] for b in parts]
            if any(name and name != other[i] and name in other for one in names for other in names if other is not one
                   for i, name in enumerate(one[:len(other)])):
                # the same column sits at different places in two branches: lists written out from two relations whose
                # columns are in different orders (dbt_utils.star on a table generated here and one built by dbt).
                # In the real warehouse the two have one order; position says nothing about these
                continue
            rows = [positional.get(id(b), []) for b in parts]
            for aligned in zip(*rows):
                group = {hit for hit in aligned if hit}
                if len(group) > 1:
                    links.append(group)
    return last


_KEYS = ("sequence", "hash_id")
_CALENDAR = re.compile(r"(^|_)(DATES?|DAYS?|CALENDAR|DATE_SPINE|DIM_DATE|TIME_SPINE)(_|$)")
_CALENDAR_DAYS = {"DATE", "DAY", "DATE_DAY", "CALENDAR_DATE", "DATE_ACTUAL", "DT"}
_FREE = ("template", "dist", "categorical", "constant")  # generators with no meaning of their own


def _share_join_values(tables: dict[str, dict], joins: list, notes: list[str], lookups=None) -> None:  # noqa: ANN001
    """Columns that models join on must draw from the same values. Group them, then:
    a key in the group makes the others reference it; named values are shared; otherwise
    every member draws from one common pool. A member is a column, or a place inside a
    document column (`payload`, `items[].sku`)."""

    def place(m):  # noqa: ANN001, ANN202
        return (m[0], m[1], m[2] if len(m) > 2 else "")

    def holder(m):  # noqa: ANN001, ANN202
        """The dict that holds the member's generator, and its key there."""
        column = tables.get(m[0], {}).get("columns", {}).get(m[1])
        if column is None:
            return None
        if not m[2]:
            return tables[m[0]]["columns"], m[1]
        fields = column.get("fields") if column.get("gen") == "object" else None
        return (fields, m[2]) if fields is not None and m[2] in fields else None

    group_of: dict[tuple[str, str, str], set] = {}
    for a, b in joins:
        a, b = place(a), place(b)
        if holder(a) is None or holder(b) is None:
            continue
        merged = group_of.get(a, {a}) | group_of.get(b, {b})
        for m in merged:
            group_of[m] = merged
    for group in {id(g): g for g in group_of.values()}.values():
        members = sorted(group)
        gen = {m: holder(m)[0][holder(m)[1]] for m in members}

        def put(m, fresh: dict) -> None:  # noqa: ANN001
            where, key = holder(m)
            where[key] = fresh

        def name(m) -> str:  # noqa: ANN001
            return ".".join(x for x in m if x)

        keys = [m for m in members if gen[m]["gen"] in _KEYS]
        if keys:
            parent = keys[0]
            for m in keys[1:]:  # two tables keyed by the same id (user, user_profile)
                gen[parent]["domain"] = gen[m]["domain"] = parent[0]
                if gen[m]["gen"] != gen[parent]["gen"]:
                    gen[m]["gen"] = gen[parent]["gen"]
            for m in members:
                if gen[m]["gen"] not in _FREE or (m[0] == parent[0] and not m[2]):
                    continue
                if m[2]:  # inside a document: one of the parent's first rows, which every day's load has
                    put(m, {"gen": "fk", "ref": parent[0], "first": 50})
                elif m[0] != parent[0]:
                    put(m, {"gen": "fk", "ref": parent[0], "skew": 1.5})
                else:
                    continue
                notes.append(f"{name(m)}: joined to {parent[0]}.{parent[1]} in a model, generated as a reference to it.")
            continue
        free = [m for m in members if gen[m]["gen"] in _FREE]
        if len(free) < 2:
            continue
        # inside documents: `receipt.receipt_id` joined to the elements of `outcome.receipt_ids[]` is an id and
        # the references to it. An id is one per element; drawing both from a small pool would pair every
        # row with hundreds of others.
        lists = [m for m in free if m[2].endswith("[]")]
        ids = [m for m in free if m[2] and not m[2].endswith("[]")
               and re.search(r"(^|_)(id|hash|key|uuid|guid)$", m[2].replace("[]", "").split(".")[-1].lower())]
        if lists and len(ids) == 1 and all(gen[m]["gen"] == "template" for m in free):
            key = ids[0]
            domain = f"{key[0]}.{key[1]}.{key[2]}"
            put(key, {"gen": "hash_id", "domain": domain})
            held_in = key[2][: key[2].rfind("[]") + 2] if "[]" in key[2] else ""  # the list whose elements have the id
            for slot, m in enumerate(m for m in free if m != key):
                put(m, {"gen": "link", "domain": domain, "depth": key[2].count("[]"),
                        **({"layered": True, "slot": slot, "tree": True, **({"within": True} if held_in and m[2].startswith(held_in) else {})}
                           if m[0] == key[0] else {})})
            notes.append(f"{name(key)}: an id that {', '.join(name(m) for m in free if m != key)} refer to; generated as one.")
            continue
        named = [gen[m] for m in free if gen[m]["gen"] == "categorical"]
        if named:
            values: dict = {}
            for d in named:
                for v, w in d["values"].items():
                    values.setdefault(v, w)
            for m in free:
                put(m, {"gen": "categorical", "values": dict(values)})
        else:
            pool = (free[0][2].replace("[]", "").split(".")[-1] or free[0][1]).lower()
            numeric = any(gen[m]["gen"] == "dist" for m in free)

            def belongs_to_an_id(m) -> bool:  # noqa: ANN001
                """`author_username` next to `author_id`, or `login` in a document keyed by its `id`."""
                if not m[2]:
                    return bool(gen[m].get("follows"))
                column = tables[m[0]]["columns"][m[1]]
                return bool(column.get("keyed_by")) and m[2] != column["keyed_by"] and "." not in m[2] and "[" not in m[2]

            if not numeric and all(belongs_to_an_id(m) for m in free):
                # names that each belong to an id: the name is written from the id, so one id has one name in
                # every table, two ids never share one, and the join on the name pairs the same ids
                for m in free:
                    put(m, {"gen": "template", "template": f"{pool}_{{n}}", **({"follows": gen[m]["follows"]} if gen[m].get("follows") else {})})
                notes.append(f"{', '.join(name(m) for m in free)}: joined in a model; each is the name of an id, generated from the id.")
                continue
            # the side that is looked up (`join b on a.x = b.y`) holds each value once, as the table of the
            # things themselves does; the others refer to its values. Both drawn at random, every row of a
            # would meet several of b, and a model's rows would multiply at each join.
            looked_up = max(free, key=lambda m: (lookups or {}).get(m, 0))
            one_each = looked_up if (lookups or {}).get(looked_up, 0) > 0 and not looked_up[2] and len(
                [m for m in free if (lookups or {}).get(m, 0) == (lookups or {}).get(looked_up, 0)]) == 1 else None
            for m in free:
                if m == one_each:
                    put(m, {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000], "unique": True} if numeric
                        else {"gen": "template", "template": f"{pool}_{{n}}"})
                else:
                    put(m, {"gen": "pool", "pool": pool, "size": 200, **({"numeric": True} if numeric else {})})
        notes.append(f"{', '.join(name(m) for m in free)}: joined in a model, generated from the same values.")


def _share_union_values(tables: dict[str, dict], links: list) -> None:
    """`select author_id, author_username from merge_requests union select ... from issues`: the two tables
    describe the same people. Where a join made one side's column a shared pool, or a name written from its id,
    the columns lined up with it in a UNION are generated the same way: one id has one name in both."""

    def holder(m):  # noqa: ANN001, ANN202
        column = tables.get(m[0], {}).get("columns", {}).get(m[1])
        if column is None:
            return None
        if not m[2]:
            return tables[m[0]]["columns"], m[1]
        fields = column.get("fields") if column.get("gen") == "object" else None
        return (fields, m[2]) if fields is not None and m[2] in fields else None

    def of_an_id(m, gen: dict) -> bool:  # noqa: ANN001
        if not m[2]:
            return bool(gen.get("follows"))
        column = tables[m[0]]["columns"][m[1]]
        return bool(column.get("keyed_by")) and m[2] != column["keyed_by"] and "." not in m[2] and "[" not in m[2]

    for _ in range(2):  # a decision can travel through two unions
        for group in links:
            members = [(m[0], m[1], m[2] if len(m) > 2 else "") for m in group]
            held = {m: holder(m) for m in members}
            gens = {m: held[m][0][held[m][1]] for m in members if held[m]}
            named = next((g for g in gens.values() if g.get("gen") == "template" and "{n}" in str(g.get("template")) and "templates" not in g), None)
            pooled = next((g for g in gens.values() if g.get("gen") == "pool"), None)
            for m, gen in gens.items():
                if gen.get("gen") != "template" or "templates" in gen or "parts" in gen or gen.get("format"):
                    continue
                where, key = held[m]
                if named and gen is not named and "{n}" not in str(gen.get("template")) and of_an_id(m, gen):
                    where[key] = {"gen": "template", "template": named["template"], **({"follows": gen["follows"]} if gen.get("follows") else {})}
                elif pooled and not named:
                    where[key] = dict(pooled)


_FLAGS = {"ISDELETED", "ISWON", "ISCLOSED", "ISACTIVE", "ISCONVERTED", "ISARCHIVED", "ISPRIVATE"}
# CREATEDDATE, CLOSEDATE, TRANDATE: a date, written without the underscore (not UPDATE, CANDIDATE, VALIDATE ...)
_RUN_ON_DATE = re.compile(r"(?<!UP)(?<!CANDI)(?<!VALI)(?<!MAN)(?<!CONSOLI)(?<!LIQUI)DATE$")


def guess(column: str, hint: str | None) -> dict:
    """A generator for a column we know only by name and (maybe) a cast."""
    c, h = column.upper(), (hint or "").upper()
    if h in ("VARIANT", "JSON", "OBJECT", "STRUCT") or h.startswith(("OBJECT", "MAP")):
        return {"gen": "object", "fields": {}}  # col::variant: semi-structured, shape unknown
    if h == "ARRAY" or h.endswith("[]"):
        return {"gen": "array"}
    if h.startswith(("DECIMAL", "NUMERIC")) and not h.endswith(",0)"):
        size = re.search(r"\((\d+)\s*,\s*(\d+)\)", h)
        whole = int(size.group(1)) - int(size.group(2)) if size else 9
        if whole < 4:  # DECIMAL(2,2), DECIMAL(5,2): values that fit the digits before the point
            top = round(10 ** max(whole, 0) - 10 ** -int(size.group(2)), int(size.group(2)))
            return {"gen": "dist", "type": h, "p": [0, 0.5, 1], "v": [0, round(top / 4, int(size.group(2))), top]}
        return {"gen": "dist", "type": h, **AMOUNT}
    if h in ("DOUBLE", "FLOAT", "REAL"):
        return {"gen": "dist", "type": "DOUBLE", **AMOUNT}
    if h == "EPOCH":
        return {"gen": "time_offset", "epoch": True, **WEEK}
    if h in ("BIGINT", "INTEGER", "INT", "SMALLINT", "NUMBER") or h.endswith(",0)"):  # an explicit type beats a name
        digits = re.search(r"\((\d+)\s*,\s*0\)", h)
        top = min(1000, 10 ** int(digits.group(1)) - 1) if digits else 1000
        return {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1 if top > 1 else 0, top]}
    c = re.sub(r"__P?C$", "", c)  # Salesforce's suffix for a custom field: the name before it says what it holds
    if h == "BOOLEAN" or (not h and (re.match(r"^(IS|HAS|WAS)_", c) or c in _FLAGS)):
        return {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.5, False: 0.5}}
    if h == "DATE" or (not h.startswith("TIMESTAMP") and (c.endswith(("_DATE", "_DAY")) or _RUN_ON_DATE.search(c))):
        return {"gen": "time_offset", "type": "DATE", **WEEK}
    if h.startswith("TIMESTAMP") or c.endswith(("_AT", "_TS", "_TIME", "_TIMESTAMP")) or c in ("SYSTEMMODSTAMP", "TIMESTAMP", "DATETIME"):
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


def regex_examples(pattern: str, count: int = 4, ignore_case: bool = False, whole: bool = True) -> list[str]:
    """A few texts the regular expression matches, for a column the SQL keeps rows of by it:
    `^/[1-9]{1,3}[.][0-9]{1,3}/.*` -> `/1.0/a`, ... Empty when the pattern is beyond this (it is checked)."""
    import re._constants as c
    import re._parser as parser

    import warnings

    pattern = pattern.replace("\\\\", "\\")  # written '\\d' in a SQL string, read as \d
    for name, inside in (("alpha", "a-zA-Z"), ("digit", "0-9"), ("alnum", "a-zA-Z0-9"), ("upper", "A-Z"), ("lower", "a-z"),
                         ("space", " "), ("punct", ".,;-"), ("xdigit", "0-9a-f")):
        pattern = pattern.replace(f"[:{name}:]", inside)
    flags = re.IGNORECASE if ignore_case else 0
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            compiled, parsed = re.compile(pattern, flags), parser.parse(pattern, flags)
    except Exception:  # noqa: BLE001
        return []
    classes = {c.CATEGORY_DIGIT: "0123456789", c.CATEGORY_WORD: "abcdefgh", c.CATEGORY_SPACE: " ",
               c.CATEGORY_NOT_DIGIT: "abc", c.CATEGORY_NOT_WORD: "-", c.CATEGORY_NOT_SPACE: "abc"}

    def member(item, i: int) -> str:  # noqa: ANN001
        op, av = item
        if op is c.LITERAL:
            return chr(av)
        if op is c.RANGE:
            return chr(av[0] + i % (av[1] - av[0] + 1))
        if op is c.CATEGORY:
            return classes[av][i % len(classes[av])]
        raise ValueError(op)

    def sample(items, i: int, before: str = "") -> str:  # noqa: ANN001
        out = []
        for op, av in items:
            if op is c.LITERAL:
                out.append(chr(av))
            elif op is c.NOT_LITERAL:
                out.append("x" if av != ord("x") else "y")
            elif op is c.ANY:
                # after digits, a dot: `[0-9]+.[0-9]+` is a version or a decimal written with an unescaped `.`
                out.append("." if ("".join(out) or before)[-1:].isdigit() else "abcd"[i % 4])
            elif op is c.IN:
                if av and av[0][0] is c.NEGATE:
                    out.append(next(ch for ch in "a1-_ x/Z" if not re.fullmatch("[" + "".join(
                        re.escape(member(m, k)) for m in av[1:] for k in range(26 if m[0] is c.RANGE else 10)) + "]", ch)))
                else:
                    out.append(member(av[i % len(av)], i))
            elif op in (c.MAX_REPEAT, c.MIN_REPEAT, getattr(c, "POSSESSIVE_REPEAT", None)):
                low, high, inner = av
                times = min(max(low, 1 if high else 0) + (i % 2 if high > max(low, 1) else 0), 6)
                for k in range(times):
                    out.append(sample(inner, i + k, (before + "".join(out))[-1:] if times == 1 and low == high else ""))
            elif op is c.SUBPATTERN:
                out.append(sample(av[3], i, (before + "".join(out))[-1:]))
            elif op is c.BRANCH:
                out.append(sample(av[1][i % len(av[1])], i, (before + "".join(out))[-1:]))
            elif op in (c.AT, c.ASSERT, c.ASSERT_NOT):
                continue
            else:
                raise ValueError(op)
        return "".join(out)

    found: list[str] = []
    for i in range(count * 3):
        try:
            text = sample(parsed, i)
        except (ValueError, StopIteration, KeyError):
            return []
        if text and text not in found and (compiled.fullmatch(text) if whole else compiled.search(text)) \
                and len(text) <= 60 and "{" not in text and "}" not in text:
            found.append(text)
        if len(found) == count:
            break
    return found


def part_templates(shapes: dict[str, dict[int, set[str]]], casts: dict[str, dict[int, str | None]]) -> list[str]:
    """Templates for a text the SQL takes apart and keeps rows of by its pieces: `POST {hex}/api/v1/ack`.
    One separator is honoured (the one with most pieces named); every named shape of a piece appears."""
    sep = max(shapes, key=lambda k: (len(shapes[k]), k))
    named, cast = shapes[sep], casts.get(sep, {})
    width = max(max(named) + 1, max(cast, default=0) + 1, 2)
    choices = [sorted(named.get(i, ())) for i in range(width)]
    out = []
    for k in range(max(len(c) for c in choices)):
        out.append(sep.join(c[k % len(c)] if c else ("{n}" if hint_is_number(cast.get(i)) else "{hex}") for i, c in enumerate(choices)))
    free = sep.join("{n}" if hint_is_number(cast.get(i)) else "{hex}" for i in range(width))
    return [*out, free]


def flattened_objects(paths: dict[str, str | None]) -> dict[str, str | None]:
    """`flatten(input => doc:query)` beside `doc:query:success`: what is flattened is an object, not a list (a
    place cannot be both), and its "elements" are the values of its keys. What the SQL reads in an element
    (`value:data:resultType`) is read in the keys that hold objects (`query.body.data.resultType`)."""
    out = dict(paths)
    for path in sorted(paths):
        at = path.find("[]")
        while at != -1:
            base, rest = path[:at], path[at + 2:]
            keys = {other[len(base) + 1:].split(".")[0].split("[")[0] for other in paths
                    if base and other.startswith(base + ".") and other != path}
            if keys:
                out.pop(path, None)
                holders = [k for k in sorted(keys) if any(o.startswith(f"{base}.{k}.") or o.startswith(f"{base}.{k}[") for o in paths)]
                for k in holders if rest else []:
                    out.setdefault(f"{base}.{k}{rest}", paths[path])
                break
            at = path.find("[]", at + 2)
    # a place the SQL casts to a number, a date or a boolean and also takes apart (a `factor` that is a number in
    # one shape of the document and a list of sub-locations in another): generated as the plain value. Taking a
    # number apart gives no rows; casting a list to a number is an error
    plain = [p for p, h in out.items() if h and h != "[]" and (hint_is_number(h) or h.upper().startswith(("TIMESTAMP", "DATE", "BOOL", "EPOCH")))]
    for path in plain:
        for other in [o for o in out if o.startswith(path + "[") or o.startswith(path + ".")]:
            out.pop(other)
    return out


def like_shape(pattern: str) -> str:
    """A template whose values match the LIKE pattern: `%.pool%.near` -> `{hex}.pool{hex}.near`."""
    return pattern.replace("\\", "").replace("%", "{hex}").replace("_", "x")


def history_window(dates: list[tuple[str, date]], default: date = date(2025, 1, 1), days: int = 60) -> tuple[date, int | None]:
    """Where the generated history starts and how many days it spans, so that the dates the SQL names
    select something: a day it asks for by name is inside, an upper limit comes after the start, a lower
    limit before the end. (None for the span when the default does.)"""
    inside = sorted(d for how, d in dates if how == "in")
    before = sorted(d for how, d in dates if how == "before")
    after = sorted(d for how, d in dates if how == "after")
    start = default
    if inside:
        start = min(start, inside[0].replace(day=1))
    if before and before[0] <= start + timedelta(days=7):
        start = (before[0] - timedelta(days=45)).replace(day=1)
    end = start + timedelta(days=days)
    if inside:
        end = max(end, inside[-1] + timedelta(days=31))
    if after and after[-1] >= end - timedelta(days=7):
        end = after[-1] + timedelta(days=45)
    span = min((end - start).days, 1500)  # four years at most: beyond that the volume is the cost
    return start, (span if (start, span) != (default, days) else None)


def field_gen(path: str, hint: str | None, literals: set[str] | None = None, patterns: set[str] | None = None) -> dict:
    """A generator for one value inside a document: `items[].sku` is named `sku`; `tags[]` holds `tag`s."""
    name = re.sub(r"~\w+|\[\]", "", path).split(".")[-1] or "value"
    if patterns and not hint_is_number(hint):
        # `like '%.pool%.near'` keeps some rows, and the values compared by name are there too
        return {"gen": "template", "templates": [*sorted(like_shape(p) for p in patterns), *sorted(literals or ()),
                                                  f"{name.lower()}_{{hex}}"]}
    if literals and not hint_is_number(hint):
        named = sorted(literals)
        share = round(0.8 / len(named), 4)
        return {"gen": "categorical", "values": {**{v: share for v in named}, f"other_{name.lower()}": 0.2}}
    if re.sub(r"~\w+$", "", path).endswith("[]") and (hint or "").upper() in ("", "ARRAY", "VARIANT", "JSON"):
        return {"gen": "template", "template": f"{name.lower()}_{{hex}}"}  # what each element holds, when nothing says more
    return guess(name, hint)


LASTS = {"p": [0, 0.5, 1], "seconds": [30 * 86400, 365 * 86400, 3 * 365 * 86400]}  # a month to three years


def periods_have_length(columns: dict[str, dict]) -> list[str]:
    """`term_start_date` and `term_end_date`, both known only by name: the end comes after the start,
    by a month to a few years, as subscriptions, contracts and campaigns do. Any end follows any start of
    the same row, wherever the two are kept (`current_period_start` beside `metadata:"license-end-date"`):
    models take a period's start from one place and its end from another. Returns the ends changed."""
    def times(holder: dict, prefix: str = ""):  # noqa: ANN202
        for name, d in holder.items():
            if d.get("gen") == "time_offset":
                yield prefix + name, d
            elif d.get("gen") == "object":
                yield from times(d.get("fields") or {}, f"{prefix}{name}.")

    def named(label: str, word: str) -> bool:
        last = re.sub(r"~\w+|\[\]", "", label).split(".")[-1].upper().replace("-", "_")
        return bool(re.search(rf"(?<![A-Z]){word}(?:S|ED)?(?![A-Z])|{word}DATE", last))

    found = list(times(columns))
    starts = [d for label, d in found if named(label, "START")]
    changed = []
    for label, d in found:
        if named(label, "END") and not d.get("unique") and any(
                d.get("seconds") == start.get("seconds") and d.get("p") == start.get("p") for start in starts):
            d.update(copy.deepcopy(LASTS))
            changed.append(label)
    return changed


def unique_together(table: dict, names: list[str]) -> str | None:
    """Make the combination of these columns unique in a generated table, changing as little as it can:
    a column nothing is known about gets a value of its own per row; a day with listed values becomes the
    table's grain (one row a day for each value); a time becomes one day per row. Says what it did."""
    columns = table.get("columns") or {}
    gens = {n: columns.get(n) for n in names}
    if not names or any(g is None for g in gens.values()):
        return None
    free = lambda n, g: g["gen"] == "template" and g.get("template") == f"{n.lower()}_{{hex}}" and not g.get("templates")  # noqa: E731
    grain = table.get("grain") or []
    if any(g["gen"] in ("sequence", "hash_id") or g.get("unique") or g.get("per_parent") == 1 or (free(n, g) and not g.get("follows"))
           for n, g in gens.items()) or (grain and set(names) >= set(grain)):
        return None  # one of them is unique by itself already, or the table's grain is among them
    for n, g in gens.items():
        if g["gen"] == "dist" and str(g.get("type", "BIGINT")).upper().startswith(("BIGINT", "INT")):
            columns[n] = {**g, "unique": True}
            return f"{n} has a value of its own in every row"
    for n, g in gens.items():
        if g["gen"] in ("template", "pool") and not g.get("templates") and not g.get("parts") and not g.get("format"):
            columns[n] = {"gen": "template", "template": f"{n.lower()}_{{hex}}"}
            return f"{n} has a value of its own in every row"
    days = [n for n, g in gens.items() if g["gen"] == "event_time" and str(g.get("type", "")).upper() == "DATE"]
    listed = [n for n, g in gens.items() if g["gen"] == "categorical" and g.get("values")]
    if len(days) == 1 and listed and len(days) + len(listed) == len(names):
        table["grain"] = [days[0], *listed]
        return f"one row a day for each value of {', '.join(listed)}"
    for n, g in gens.items():
        if g["gen"] == "time_offset" and not g.get("format") and not g.get("epoch"):
            columns[n] = {"p": [0, 0.5, 1], "seconds": [0, 86400, 604800], **g, "unique": True, "every_days": 1}
            return f"{n} is one day per row"
    for n, g in gens.items():
        if g["gen"] == "fk":
            g["per_parent"] = 1
            return f"{n} refers to each parent once"
    return None


def within_bounds(gen: dict, bounds: set[float] | None, exact: bool = False) -> None:
    """Make a numeric column's values straddle the numbers the SQL compares it with, so that
    `where block_number between 9820000 and 10000000` keeps rows."""
    if not bounds or gen.get("gen") != "dist" or str(gen.get("type", "")).upper() not in ("BIGINT", "DOUBLE", ""):
        return
    lo, hi = min(bounds), max(bounds)
    whole = str(gen.get("type", "")).upper() == "BIGINT"
    if lo == hi:  # one threshold: values on both sides of it
        lo, hi = (0, max(2 * hi, 10)) if hi >= 0 else (2 * lo, 0)
    elif not (exact and len(bounds) == 2):  # (`exact`: the two ends of the one range a reader keeps)
        # several thresholds (`between 9820000 and 10000000` here, `>= 75000000` there; or `> 0` here and
        # `> 1000000` there): as many values between each pair as beyond the last, so every one of the comparisons keeps rows
        knots = sorted(bounds)
        # (a short scale stays short: scores compared with 7 and 9 go to 10, not to 19)
        knots.append(knots[-1] + (max((knots[-1] - knots[0]) / len(knots), 10) if knots[-1] - knots[0] > 10 else 1))
        gen.update(p=[round(i / (len(knots) - 1), 6) for i in range(len(knots))], v=[int(k) if whole else k for k in knots])
        return
    gen.update(p=[0, 1], v=[int(lo), int(hi)] if whole else [lo, hi])


def hint_is_number(hint: str | None) -> bool:
    return (hint or "").upper().startswith(("BIGINT", "INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT"))


def first(candidates: list[str], columns) -> str | None:
    return next((c for c in candidates if c in columns), None)


class Analysis:
    """What reading the project gives: a first spec, notes for the user, and where each model
    column comes from (so a failing test can be traced to the source column it is about)."""

    def __init__(self, spec: dict, notes: list[str], locate, place=None) -> None:  # noqa: ANN001
        self.spec, self.notes, self.locate = spec, notes, locate
        # like `locate`, also for a column that is a value inside a source's document: (table, column, `a.b[].c`)
        self.place = place or (lambda node_id, column: (lambda hit: (*hit, "") if hit else None)(locate(node_id, column)))
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

    # seeds are read like sources (so what is joined to them is known), and not generated: their rows are in the project
    seed_values: dict[tuple[str, str], list[str]] = {}
    seed_rows: dict[str, list[dict]] = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "seed" or not node.get("relation_name"):
            continue
        relation = node["relation_name"].replace('"', "").upper()
        path = Path(node.get("root_path") or "") / (node.get("original_file_path") or "")
        try:
            with path.open(newline="", encoding="utf-8-sig") as handle:
                rows = list(itertools.islice(csv.DictReader(handle), 5000))
        except (OSError, UnicodeDecodeError, csv.Error):
            continue
        uses.setdefault(relation, SourceUse())
        seed_rows[relation] = [{str(k).upper(): v for k, v in row.items() if k} for row in rows]
        for column in (rows[0] if rows else {}):
            if column:
                seed_values[(relation, column.upper())] = list(dict.fromkeys(r[column] for r in rows if r.get(column) not in (None, "")))

    # how each model's outputs map back to source columns
    outputs: dict[str, dict[str, tuple[str, str]]] = {}
    by_relation: dict[str, dict[str, tuple[str, str]]] = {}
    links: list[set[tuple[str, str]]] = []
    joins: list[tuple[tuple[str, str], tuple[str, str]]] = []
    dates: list[tuple[str, date]] = []
    lookups: collections.Counter = collections.Counter()

    def scan(uid: str) -> None:
        node = manifest["nodes"].get(uid)
        if uid in outputs or not node or node["resource_type"] not in ("model", "snapshot"):
            return
        outputs[uid] = {}  # guards against cycles
        for parent in node["depends_on"]["nodes"]:
            scan(parent)
        if node.get("compiled_code"):
            outputs[uid] = scan_model(node["compiled_code"], uses, by_relation, links, joins, dates, lookups)
        if node.get("relation_name"):
            by_relation[node["relation_name"].replace('"', "").upper()] = outputs[uid]

    for uid in manifest["nodes"]:
        scan(uid)
    # once more, now that every model's outputs are known: a model that reads another by a written-out name
    # (no ref(), so dbt orders nothing) may have been read before the one it reads
    outputs.clear()
    links.clear(), joins.clear(), dates.clear(), lookups.clear()
    for uid in manifest["nodes"]:
        scan(uid)
    # a test the project wrote out as a query joins what must match (a tracking plan with the events that
    # arrive): the joins count; what it filters on does not (it looks for the rows that should not be there)
    scratch = {name: SourceUse() for name in uses}
    for node in manifest["nodes"].values():
        if node["resource_type"] == "test" and not node.get("test_metadata") and node.get("compiled_code"):
            try:
                scan_model(node["compiled_code"], scratch, by_relation, None, joins)
            except Exception:  # noqa: BLE001 - a test that does not parse teaches nothing
                pass

    # what is known about one column of a union holds for the columns aligned with it
    for _ in range(3):
        for group in links:
            members = [(uses[m[0]], m[1]) for m in group if len(m) == 2 and m[0] in uses]
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
            # what a model downstream of the union compares the column with applies to every branch
            for kind in ("literals", "patterns", "bounds"):
                shared = set().union(*(getattr(u, kind).get(c, set()) for u, c in members))
                for u, c in members if shared else []:
                    getattr(u, kind).setdefault(c, set()).update(shared)

    def locate(node_id: str, column: str) -> tuple[str, str] | None:
        if node_id in ids:
            return ids[node_id], column.upper()
        known = outputs.get(node_id, {})
        if column.upper() not in known and "*" in known and column.upper() not in known["*"][1].split("|"):
            return known["*"][0], column.upper()  # a pass-through model: the column is the source's
        return known.get(column.upper())

    def place(node_id: str, column: str) -> tuple[str, str, str] | None:
        hit = locate(node_id, column)
        if hit:
            return hit[0], hit[1], ""
        found = (outputs.get(node_id, {}).get(_PATHS) or {}).get(column.upper())
        if isinstance(found, tuple) and "{}" not in found[2]:
            return found[0], found[1], path_text(found[2])
        return None

    keys: dict[str, str] = {}
    ordered: list[tuple[tuple[str, str, str], tuple[str, str, str]]] = []  # (the later, the earlier) of two times a test compares
    together: list[tuple[str, list[str]]] = []  # (table, columns a test says are unique together)
    fks: dict[tuple[str, str], tuple[str, str]] = {}
    accepted: dict[tuple[str, str], list] = {}
    for node in manifest["nodes"].values():
        meta = node.get("test_metadata")
        if node["resource_type"] != "test" or not meta:
            continue
        # a test written under a source has no `attached_node`: what it tests is the (last) thing it depends on
        tested = node.get("attached_node") or next(iter(reversed(node.get("depends_on", {}).get("nodes") or [])), None)
        if not tested:
            continue
        node = {**node, "attached_node": tested}
        column = node.get("column_name") or meta["kwargs"].get("column_name")
        if meta["name"] == "unique_combination_of_columns":
            listed = (meta["kwargs"].get("arguments") or meta["kwargs"]).get("combination_of_columns") or []
            spots = [locate(node["attached_node"], str(c)) for c in listed if re.fullmatch(r"\w+", str(c))]
            if spots and all(spots) and len(spots) == len(listed) and len({spot[0] for spot in spots}) == 1:
                together.append((spots[0][0], [spot[1] for spot in spots]))
            continue
        if meta["name"] == "expression_is_true" and not column:
            # `expression: "release_date > rc1_date"`: one of the two comes after the other
            try:
                said = sqlglot.parse_one(str((meta["kwargs"].get("arguments") or meta["kwargs"]).get("expression") or ""), read="snowflake")
            except Exception:  # noqa: BLE001
                said = None
            if isinstance(said, (exp.GT, exp.GTE, exp.LT, exp.LTE)) and isinstance(said.this, exp.Column) and isinstance(said.expression, exp.Column):
                later, earlier = (said.this, said.expression) if isinstance(said, (exp.GT, exp.GTE)) else (said.expression, said.this)
                ends = [place(node["attached_node"], c.name) for c in (later, earlier)]
                if all(ends) and ends[0][0] == ends[1][0] and ends[0] != ends[1]:
                    ordered.append((ends[0], ends[1]))
            continue
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
            other = manifest["nodes"].get(others[0], {}) if others else {}
            listed = seed_values.get((str(other.get("relation_name") or "").replace('"', "").upper(), str(kw.get("field", "")).upper()))
            if other.get("resource_type") == "seed" and listed:
                accepted.setdefault(here, listed[:200])  # every value must be one the seed lists: the project wrote them down

    tables: dict[str, dict] = {}
    assumed: list[str] = []
    keyless: list[str] = []
    for uid, name in ids.items():
        use, src = uses[name], src_nodes[uid]
        cols = dict(use.columns)
        declared = {c.upper(): str(meta.get("data_type") or "").upper() for c, meta in (src.get("columns") or {}).items()}
        for col, kind in catalog.view(name, tuple(cols)).items():  # a Snowflake system view: its real columns
            if kind or not cols.get(col):
                cols[col] = kind
            if kind:
                use.texts.discard(col)
                declared[col] = str(kind).upper()
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
                if col in use.splits and not numeric:
                    # a key the SQL takes apart (`split_part(version, '.', 2)::int`): ids of that shape, each one once
                    (sep, wanted), *_ = use.splits[col]["seps"].items()
                    out[col] = {"gen": "hash_id", "template": sep.join(
                        "{n}" if use.splits[col]["digits"] or hint_is_number(wanted.get(i)) else "{hex}" for i in range(max(max(wanted) + 1, 2)))}
            elif (name, col) in fks:
                out[col] = {"gen": "fk", "ref": fks[(name, col)][0], "skew": 1.5}
            elif (name, col) in accepted:
                values = accepted[(name, col)]
                out[col] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
            elif 0 < len(use.literals.get(col, ())) <= 30 and not hint_is_number(hint):
                # `where status = 'open'` should select something: use the values the SQL names
                named = sorted(use.literals[col] | set(sorted(use.examples.get(col, ()))[:12]))
                share = round(0.8 / len(named), 4)
                out[col] = {"gen": "categorical", "values": {**{v: share for v in named}, f"other_{col.lower()}": 0.2}}
            elif (col in use.patterns or col in use.examples) and not hint_is_number(hint):
                # `where name like 'tap-%'` should keep some rows: make values that fit each pattern
                shapes = sorted(like_shape(p) for p in use.patterns.get(col, ())) + sorted(use.examples.get(col, ()))[:24]
                out[col] = {"gen": "template", "templates": [*shapes, f"{col.lower()}_{{hex}}"]}
            elif col in use.part_shapes and not hint_is_number(hint):
                out[col] = {"gen": "template", "templates": part_templates(use.part_shapes[col], (use.splits.get(col) or {}).get("seps", {}))}
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
                use.paths[col] = flattened_objects(use.paths[col])
                out[col] = {"gen": "object", "fields": {
                    path: field_gen(path, h, use.path_literals.get(col, {}).get(path), use.path_patterns.get(col, {}).get(path))
                    for path, h in sorted(use.paths[col].items())
                    # (an element that has elements of its own is a list, not a value: `points[]` beside `points[][]`)
                    if not (path.endswith("[]") and h is None and any(other.startswith(path + "[") for other in use.paths[col]))}}
                for path, seps in use.path_part_shapes.get(col, {}).items():
                    # a text in the document that models take apart and keep rows of by its pieces
                    held = out[col]["fields"].get(path)
                    if held is None or held.get("gen") not in ("template", "categorical"):
                        continue
                    known = list(held.get("templates") or ([held["template"]] if held.get("template") else sorted(map(str, held.get("values") or {}))))
                    made = part_templates(seps, {})[:-1]
                    # `log like '@%'` beside pieces read off it: the mark opens the first piece
                    marks = [t[: -len("{hex}")] for t in known if t.endswith("{hex}") and re.fullmatch(r"[^\w\s{}]+", t[: -len("{hex}")])]
                    made = [mark + text for mark in marks[:2] for text in made] or made
                    out[col]["fields"][path] = {"gen": "template", "templates": list(dict.fromkeys([*made, *known]))}
                for path, mark in use.path_prefix.get(col, {}).items():
                    if any(field.startswith(path + "~json") for field in out[col]["fields"]):
                        out[col]["fields"][path + "~prefix"] = {"gen": "constant", "value": mark}
                for path in use.path_optional.get(col, ()):
                    # the key itself, or (when deeper keys are read) every path under it
                    for field, d in out[col]["fields"].items():
                        if field == path or field.startswith((path + ".", path + "~", path + "[")):
                            # (also read as JSON text or as a list: the same key)
                            # there in half of the documents; a key that reports what went wrong (`status.Failure`) in few:
                            # most things succeed, and a transaction of ten receipts succeeds only if all of them do
                            wrong = re.search(r"fail|error|exception|reject|invalid|panic", re.sub(r"~\w+|\[\]", "", path).split(".")[-1], re.I)
                            d["optional"] = 0.95 if wrong else 0.5
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
                # a column declared DATE (by the source's yml, or by Snowflake for its own views) holds days:
                # `on spine.date = t.date` matches only then. A cast to DATE in a model says nothing: it truncates.
                out[col] = {"gen": "event_time", **({"type": "DATE"} if declared.get(col) == "DATE" else {})}
            elif col == updated:
                out[col] = {"gen": "updated_at", **({"type": "DATE"} if declared.get(col) == "DATE" else {})}
            else:
                out[col] = guess(col, hint)
            window = use.windows.get(col)
            # (what the table's own reader keeps is all that goes anywhere: thresholds beyond it, asked for downstream,
            # cannot be met on a first run, with these rows or with real ones)
            if window and not all(min(window) <= b <= max(window) for b in use.read_bounds.get(col, ())):
                window = None  # another model reads the table itself and asks for something outside it
            ranged = bool(window and len(window) >= 2)
            within_bounds(out[col], {min(window), max(window)} if ranged else use.bounds.get(col), exact=ranged)
            if col in use.future and out[col].get("gen") == "time_offset" and not out[col].get("format") and not out[col].get("epoch"):
                out[col].update(p=[0, 0.3, 1], seconds=[-30 * 86400, 30 * 86400, 400 * 86400])  # from last month to next year
            if col in use.nullable and out[col].get("gen") not in ("sequence", "hash_id", "loaded_at", "event_time", "lifecycle") \
                    and not out[col].get("null_rate"):
                out[col]["null_rate"] = 0.3
        for col, habit in catalog.habits(name, out).items():
            if out[col].get("gen") in ("event_time", "time_offset", "updated_at") and not out[col].get("format"):
                out[col] = {**habit, **({"type": out[col]["type"]} if out[col].get("type") else {})}
        # author_id + author_username: the name belongs to the id, so it must not vary between rows
        for col, d in out.items():
            if col.endswith("_ID") and d["gen"] in ("dist", "fk", "template"):
                for sibling, sd in out.items():
                    if sibling != col and sibling.startswith(col[:-2]) and sd["gen"] in ("template", "categorical"):
                        sd["follows"] = col
        for col, role in use.periods.items():
            # the bound of a period rows are looked up in: one every two months, so that no two periods overlap;
            # where the table holds both ends, a period lasts six weeks
            if col in out and out[col].get("gen") == "time_offset" and not out[col].get("format") and not out[col].get("epoch"):
                out[col].update(unique=True, every_days=60)
                if role == "end" and "start" in use.periods.values():
                    out[col]["plus_days"] = 45
        for col, by in use.depends.items():
            # one value per `by`: generated from it, not from the row
            if col in out and by in out and not out[col].get("follows") and out[by].get("follows") != col \
                    and out[col]["gen"] in ("dist", "template", "categorical", "time_offset", "pool"):
                out[col]["follows"] = by
        periods_have_length(out)
        tables[name] = {
            "volume": {"rows_per_day": 100_000, "weekly": [1, 1, 1, 1, 1, 1, 1]},  # placeholder: 100 rows a day at the default scale
            "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]},  # placeholder
            "columns": out,
        }
    # `dbt_utils.pivot('role', dbt_utils.get_column_values(ref('users'), 'role'))` writes one column per value it
    # finds: the column holds a handful of values, not one per row
    by_name = {n.get("name"): uid for uid, n in manifest["nodes"].items() if n.get("resource_type") == "model"}
    pivoted: list[str] = []
    for node in manifest["nodes"].values():
        raw = node.get("raw_code") or ""
        if "get_column_values" not in raw:
            continue
        for model, column in re.findall(r"get_column_values\(\s*(?:table\s*=\s*)?ref\(\s*['\"](\w+)['\"]\s*\)\s*,\s*(?:column\s*=\s*)?['\"](\w+)['\"]", raw):
            where = place(by_name.get(model, ""), column)
            holder = tables.get(where[0], {}).get("columns", {}) if where else {}
            if where and where[2]:
                holder = (holder.get(where[1]) or {}).get("fields") or {}
            key = (where[2] or where[1]) if where else None
            if key in holder and holder[key].get("gen") in ("template", "dist"):
                label = re.sub(r"~\w+|\[\]", "", key).split(".")[-1].lower()
                holder[key] = {"gen": "categorical", "values": {f"{label}_{i}": round(1 / 6, 4) for i in range(1, 7)}}
                pivoted.append(f"{where[0]}.{where[1]}" + (f" ({where[2]})" if where[2] else ""))
    if pivoted:
        notes.append(f"{len(pivoted)} column(s) are pivoted into one column per value and generated with six values: "
                     f"{', '.join(pivoted[:6])}{' ...' if len(pivoted) > 6 else ''}.")
    if assumed:
        notes.append(f"{len(assumed)} source(s) have no `unique` test; assumed keys: {', '.join(assumed[:12])}{' ...' if len(assumed) > 12 else ''}.")
    if keyless:
        notes.append(f"{len(keyless)} source(s) have no `unique` test and no id-like column, so no key: "
                     f"{', '.join(keyless[:12])}{' ...' if len(keyless) > 12 else ''}.")
    # a column named like another table's key is almost always a reference to it
    guessed: list[str] = []
    keyed: dict[str, list[str]] = {}
    for t, spec in tables.items():
        for c, d in spec["columns"].items():
            if d["gen"] in ("sequence", "hash_id"):
                keyed.setdefault(c, []).append(t)
    # (a name that is the key of one table only, and reads like an id: `ID` is the key of many tables, and a table
    # with one row per `NAME`, `MONTH` or `VERSION` is not what every other NAME, MONTH or VERSION refers to)
    key_owner = {c: owners[0] for c, owners in keyed.items()
                 if len(owners) == 1 and re.search(r"[A-Z0-9](_ID|_KEY|_UUID|_GUID|_SK|_PK|_SFID|ID)$", c)}
    for name, spec in tables.items():
        for col, d in spec["columns"].items():
            owner = key_owner.get(col)
            if owner and owner != name and d["gen"] not in ("sequence", "hash_id", "fk"):
                spec["columns"][col] = {"gen": "fk", "ref": owner, "skew": 1.5}
                guessed.append(f"{name}.{col} -> {owner}")
    if guessed:
        notes.append(f"{len(guessed)} column(s) are named like another table's key and assumed to reference it (no relationships "
                     f"test says so): {'; '.join(guessed[:6])}{' ...' if len(guessed) > 6 else ''}.")
    # text that mentions another column's value between two marks (`... (<@U123>) ...`, parsed back out with SPLIT_PART)
    for name, use in uses.items():
        for col, found in use.embeds.items():
            before, after, (other_table, other_col) = found[0]
            target = tables.get(other_table, {}).get("columns", {}).get(other_col)
            mine = tables.get(name, {}).get("columns", {}).get(col)
            if target is None or mine is None or mine.get("gen") not in _FREE:
                continue
            if target["gen"] in _KEYS:
                part = {"gen": "fk", "ref": other_table, "first": 50}
            elif target["gen"] in ("categorical", "pool"):
                part = copy.deepcopy(target)
            elif target["gen"] in _FREE:
                target = tables[other_table]["columns"][other_col] = {"gen": "pool", "pool": other_col.lower(), "size": 200}
                part = dict(target)
            else:
                continue
            tables[name]["columns"][col] = {"gen": "template", "template": f"{col.lower()} {{hex}} {before}{{0}}{after}", "parts": [part]}
            notes.append(f"{name}.{col}: models read a value of {other_table}.{other_col} out of it, between `{before}` and `{after}`; "
                         "generated with one there.")
    joined: list[str] = []
    # a column joined to a seed's column holds the seed's values: the project wrote them down
    for a, b in joins:
        for seed, other in ((a, b), (b, a)):
            values = seed_values.get((seed[0], seed[1])) if len(seed) < 3 or not seed[2] else None
            column = tables.get(other[0], {}).get("columns", {}).get(other[1])
            if not values or column is None:
                continue
            holder, key = tables[other[0]]["columns"], other[1]
            if len(other) > 2 and other[2]:
                holder, key = (column.get("fields") or {}), other[2]
            if key in holder and holder[key].get("gen") in _FREE:
                known = values[:100]
                holder[key] = {"gen": "categorical", "values": {v: round(1 / len(known), 6) for v in known}}
                if seed[0] in uses and seed[1] in uses[seed[0]].null_tests:
                    # the models ask whether the seed had a row (`team.user_id is not null as is_team`): most rows are
                    # listed in it, not all
                    numbers = [int(str(v)) for v in known if re.fullmatch(r"-?\d+", str(v))]
                    other = str(max(numbers) + 7919) if len(numbers) == len(known) else f"other_{key.split('.')[-1].lower()}"
                    holder[key] = {"gen": "categorical", "values": {**{v: round(0.85 / len(known), 6) for v in known}, other: 0.15}}
                joined.append(f"{'.'.join(x for x in other if x)}: joined to the seed {seed[0]} ({seed[1]}), generated from its values.")
    # two columns joined to two columns of one seed (`on t.event = p.event and t.action = p.action`): the pairs the
    # seed lists, not every combination of its values
    paired: dict[tuple[str, str], dict[str, str]] = {}
    for a, b in joins:
        for seed, other in ((a, b), (b, a)):
            if seed[0] in seed_rows and not (len(seed) > 2 and seed[2]) and not (len(other) > 2 and other[2]) and other[0] in tables:
                paired.setdefault((seed[0], other[0]), {})[other[1]] = seed[1]
    for (seed, table), columns in paired.items():
        held = tables[table]["columns"]
        if len(set(columns.values())) < 2 or any(held.get(c, {}).get("gen") not in ("categorical", *_FREE) for c in columns):
            continue
        rows = list(dict.fromkeys(tuple(row.get(columns[c]) or None for c in columns) for row in seed_rows[seed]))[:300]
        for i, column in enumerate(columns):
            held[column] = {"gen": "row_of", "group": seed, "values": [row[i] for row in rows]}
        joined.append(f"{table} ({', '.join(columns)}): joined together to the seed {seed}, generated as the rows it lists.")
    _share_join_values(tables, joins, joined, lookups)
    _share_union_values(tables, links)
    if joined:
        notes.append(f"{len(joined)} group(s) of columns are joined in models and generated to match, for example: {' '.join(joined[:3])}")
    def held_at(spot):  # noqa: ANN001, ANN202
        column = tables.get(spot[0], {}).get("columns", {}).get(spot[1])
        if column is None or not spot[2]:
            return column
        return (column.get("fields") or {}).get(spot[2]) if column.get("gen") == "object" else None

    for name, use in uses.items():
        # a table matched on an entity and its period together (`on q.warehouse_id = m.entity_id and q.hour = m.start_time`)
        # holds one row per period for each of a few entities, as a meter or a daily balance does: generated so, and
        # whatever is joined to the entity draws from the same few
        columns = tables.get(name, {}).get("columns") or {}
        for together_here in sorted(use.series, key=sorted):
            for c in together_here:  # cut to the hour by the models that match on it: on the hour here
                if columns.get(c, {}).get("gen") in ("event_time", "time_offset") and not columns[c].get("trunc") and use.truncs.get(c) \
                        and str(columns[c].get("type", "")).upper() != "DATE" and not columns[c].get("format") and not columns[c].get("epoch"):
                    columns[c]["trunc"] = use.truncs[c]
            times = [c for c in together_here if columns.get(c, {}).get("gen") in ("event_time", "time_offset")
                     and not columns[c].get("format") and not columns[c].get("epoch") and not columns[c].get("unique")
                     and (columns[c].get("trunc") in ("hour", "day") or str(columns[c].get("type", "")).upper() == "DATE")]
            entities = [c for c in together_here if columns.get(c, {}).get("gen") in ("pool", "dist")
                        and not columns[c].get("format") and c not in times]
            if len(times) != 1 or len(entities) != 1 or len(together_here) != 2 or tables[name].get("grain"):
                continue
            # a table with an id of its own lists things (subscriptions, orders), each once: not readings over time.
            # And the period is one the models or Snowflake cut the time to, not merely a date
            if any(d.get("gen") in _KEYS for d in columns.values()) or columns[times[0]].get("trunc") not in ("hour", "day"):
                continue
            entity, hourly = entities[0], columns[times[0]].get("trunc") == "hour"
            held = columns[entity]
            few = 4 if hourly else 20
            pool = held.get("pool") or entity.lower()
            numeric = bool(held.get("numeric")) or held.get("gen") == "dist"
            columns[entity] = {"gen": "pool", "pool": pool, "size": few, **({"numeric": True} if numeric else {})}
            tables[name]["grain"] = [times[0], entity]
            for other in tables.values():
                for d in other["columns"].values():
                    if d.get("gen") == "pool" and d.get("pool") == pool:
                        d["size"] = few
            notes.append(f"{name}: models match it on {entity} and {times[0]} together; generated as one row "
                         f"{'an hour' if hourly else 'a day'} for each of {few} {entity.lower()}s.")
    for name, columns in together:
        if name in tables:
            unique_together(tables[name], columns)
    for later, earlier in ordered:
        # a test says one time is after the other: it starts where the other's range stops
        after, before = held_at(later), held_at(earlier)
        if after and before and after.get("gen") == before.get("gen") == "time_offset" and not after.get("unique"):
            top = max(before["seconds"])
            if min(after["seconds"]) <= top:
                after.update(p=[0, 0.5, 1], seconds=[top + 86400, top + 30 * 86400, top + 365 * 86400])
    # `where server_id = '93myk...'` on a column that refers to another table's key: that table has a row with that id
    for name, table in tables.items():
        for col, gen in table["columns"].items():
            kind = gen.get("gen")
            asked = sorted(uses[name].literals.get(col, ())) if kind in ("fk", "sequence", "hash_id") and name in uses else []
            parent = (table if kind != "fk" else tables.get(str(gen.get("ref", "")).upper())) if asked else None
            key = next((g for g in parent["columns"].values() if g.get("gen") in ("sequence", "hash_id")), None) if parent else None
            if key is None:
                continue
            if not all(re.fullmatch(r"\d{1,18}", v) for v in asked):
                key["gen"] = "hash_id"  # named by a text: the ids are texts
            key["named"] = sorted(set(key.get("named") or []) | set(asked))[:50]
    # a foreign key needs its parent to have a key
    for (child, col), (parent, pcol) in fks.items():
        if child in tables and parent in tables and tables[parent]["columns"].get(pcol, {}).get("gen") not in ("sequence", "hash_id") \
                and tables[child]["columns"].get(col, {}).get("gen") == "fk":  # (a key of its own table stays one)
            notes.append(f"{child}.{col}: references {parent}.{pcol}, which is not that table's key; generated as a plain column.")
            tables[child]["columns"][col] = guess(col, None)
    # INFORMATION_SCHEMA is the warehouse describing itself: the emulator answers it, nothing to generate
    tables = {name: t for name, t in tables.items() if name.split(".")[1] != "INFORMATION_SCHEMA"}
    # a source that names what the project itself builds (a seed or a model, declared as a source so
    # other models can read it): the project provides it
    built = {(n.get("relation_name") or "").replace('"', "").upper() for n in manifest["nodes"].values()
             if n.get("resource_type") in ("model", "seed", "snapshot")}
    own = sorted(name for name in tables if name in built)
    if own:
        tables = {name: t for name, t in tables.items() if name not in built}
        notes.append(f"{len(own)} declared source(s) are relations the project builds itself ({', '.join(own[:3])}"
                     f"{' ...' if len(own) > 3 else ''}): left to the project, not generated.")
    if len(tables) > 200:
        # hundreds of sources: a smaller placeholder keeps a build to minutes and a few gigabytes
        rows = 20_000 if len(tables) <= 600 else 10_000
        for table in tables.values():
            table["volume"]["rows_per_day"] = rows
        notes.append(f"{len(tables)} source tables: placeholder volumes are {rows} rows a day ({rows // 1000} at the default scale) "
                     "to keep builds quick. `fauxflake profile` replaces them with what production has.")
    # a history longer than the usual sixty days holds the same number of rows, spread thinner: the dates the SQL
    # asks for are there, and a build costs what it did
    start, history = history_window(dates)
    if history and history > 60:
        for table in tables.values():
            table["volume"]["rows_per_day"] = max(1000, int(float(table["volume"]["rows_per_day"]) * 60 / history))
    # a shared pool as large as the table: two tables of a few thousand rows joined on a column drawn from
    # 200 values pair every row with dozens of others, and three such joins in a row fill the disk. Drawn from
    # about as many values as there are rows, each row finds a partner or two, as with real ids. (The smaller
    # table's values are the first of the larger one's, so they still meet.)
    for table in tables.values():
        rows = int(float(table["volume"]["rows_per_day"]) * 0.001 * (history or 60))  # at the default scale

        def widen(gens: dict) -> None:
            for d in gens.values():
                if d.get("gen") == "pool" and d.get("size") == 200:
                    d["size"] = max(200, rows)  # noqa: B023
                for part in d.get("parts") or []:
                    widen({"": part})
                if d.get("gen") == "object":
                    widen(d.get("fields") or {})

        widen(table["columns"])
    settings = {"database": database}
    if os.environ.get("FAUXFLAKE_TARGET"):
        settings["target"] = os.environ["FAUXFLAKE_TARGET"]
        notes.append(f"the project names its schemas differently for the target `{settings['target']}` and refers to those names "
                     f"directly, so it is built as that target (`dbt: {{target: {settings['target']}}}`). Nothing leaves this machine.")
    if (start, history) != (date(2025, 1, 1), None):
        notes.append(f"the SQL compares with dates between {min(d for _, d in dates)} and {max(d for _, d in dates)}: "
                     f"the generated history starts {start}" + (f" and spans {history} days." if history else "."))
    analysis = Analysis({"seed": 42, "start": start.isoformat(), **({"history": history} if history else {}),
                         "dbt": settings, "tables": tables}, notes, locate, place)
    analysis.bounds = {(name, col): numbers for name, use in uses.items() for col, numbers in use.bounds.items()}
    return analysis


HEADER = """# Written by `fauxflake init` from the dbt project. Keys, relationships and categories come
# from your tests; columns and types from how models use each source. Volumes, lateness and
# distributions are placeholders: run `fauxflake profile` to measure them, or edit by hand.
"""


_MISSING_ENV = re.compile(r"Env var required but not provided: '(\w+)'")


def production_target(project_dir: str | Path) -> str | None:
    """The target name a project's own `generate_schema_name` (or database, or alias) macro singles
    out: `prefix = target.name + '_' if target.name != 'prod' else ''`. Such a project names its
    relations as production does only under that target, and its models often refer to those names
    directly (`from finance.daily_arr`). Under any other target they would look for tables nobody builds."""
    names: collections.Counter = collections.Counter()
    texts = []
    for path in Path(project_dir).glob("macros/**/*.sql"):
        try:
            texts.append(path.read_text(errors="ignore"))
        except OSError:
            continue
    quoted = r"['\"](\w+)['\"]"
    for text in texts:
        for macro in re.finditer(r"(?s)\{%-?\s*macro\s+generate_(?:schema|database|alias)_name\b.*?endmacro", text):
            body = macro.group(0)
            names.update(re.findall(r"target\.name\s*[!=]=\s*" + quoted, body))
            for listed in re.findall(r"target\.name\s+(?:not\s+)?in\s+[(\[]([^)\]]*)[)\]]", body):
                names.update(re.findall(quoted, listed))
            # `target.name in production_targets`, a list another macro returns: ('prod', 'docs', 'ci')
            for variable in re.findall(r"target\.name\s+(?:not\s+)?in\s+(\w+)", body):
                source = re.search(r"set\s+" + variable + r"\s*=\s*(\w+)\(\)", body)
                where = re.search(r"set\s+" + variable + r"\s*=\s*[(\[]([^)\]]*)[)\]]", body)
                if where:
                    names.update(re.findall(quoted, where.group(1)))
                for other in texts if source else []:
                    made = re.search(r"(?s)\{%-?\s*macro\s+" + source.group(1) + r"\b.*?endmacro", other)
                    if made:
                        names.update(re.findall(quoted, made.group(0)))
    if "ci" in names:
        return "ci"  # this is a CI run: production's names, without what only production itself does
    return names.most_common(1)[0][0] if names else None


def provide_env(project) -> dict[str, str]:
    """Projects often read database and schema names from the environment. Parse until dbt
    stops asking, giving each missing variable a placeholder named after itself."""
    import os

    provided: dict[str, str] = {}

    def provide(var: str) -> None:
        value = re.sub(r"^(DBT_)?(SNOWFLAKE_)?((DATABASE|SCHEMA|DB)_)?", "", var.upper()) or "FAUXFLAKE"
        if re.search(r"SALT|SECRET|PASSWORD|TOKEN|_KEY|HASH", var.upper()):
            value = "A1B2C3D4E5F60718"  # secrets are often read as hex: to_binary(env_var('SALT'), 'HEX')
        provided[var] = os.environ[var] = value

    # the variables the project names without a default, read off its files: dbt reports only one per run
    for path in project.dir.rglob("*"):
        if path.suffix not in (".sql", ".yml", ".yaml") or any(part in ("target", "logs", ".fauxflake") for part in path.parts):
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for var in re.findall(r"env_var\(\s*['\"]([A-Za-z_]\w*)['\"]\s*\)", text):
            if var not in os.environ:
                provide(var)
    for _ in range(200):
        if project.invoke(["parse"], silent=True):  # the errors are expected: they name the variables
            break
        missing = _MISSING_ENV.search(project.last_error)
        if not missing:
            raise RuntimeError(f"dbt could not parse the project:\n{project.last_error}")
        provide(missing.group(1))
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
            if col not in columns and gen.get("gen") != "loaded_at":  # no model reads that one: it stays out if it was taken out
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
