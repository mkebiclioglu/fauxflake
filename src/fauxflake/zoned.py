"""TIMESTAMP_TZ: an instant and the offset it was written with.

DuckDB's TIMESTAMPTZ is an instant only, which is what Snowflake's TIMESTAMP_LTZ is. A TIMESTAMP_TZ is held here as
STRUCT(I TIMESTAMPTZ, O BIGINT): the instant and its offset in minutes. Measured on Snowflake:

- comparisons, ordering, MIN and MAX, COUNT(DISTINCT) and DATEDIFF in units of a day or less go by the instant;
- date parts, casts to DATE, TIME and TIMESTAMP_NTZ, and DATEDIFF in days or more go by the value's own wall clock;
- DATEADD and DATE_TRUNC work on the wall clock and keep the offset (a fixed one: adding 24 hours across a change to
  summer time keeps -0800);
- the text is the wall clock and the offset, `2025-12-31 23:30:00.000 -0530`, or Z for zero;
- CONVERT_TIMEZONE(zone, x) takes the instant and the offset that zone has at that instant.

Which expressions are TIMESTAMP_TZ is asked of sqlglot's type annotation, over the tables' types in DuckDB's catalog;
each one is then written with the macros below, so that DuckDB never meets the struct where it expects a timestamp.
"""
from __future__ import annotations

import itertools
import re

from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

_NS = "_fs_global.main."
STRUCT = "STRUCT(I TIMESTAMPTZ, O BIGINT)"
SHOWN = "STRUCT(I TIMESTAMP WITH TIME ZONE, O BIGINT)"  # (as typeof() and DESCRIBE name it)

# a text that ends in an offset, after a time of day: its wall clock and the offset (+05:30, -0330, +05, Z)
_SPLIT_SQL = r"^(.*\d:\d\d(?::\d\d(?:\.\d*)?)?)\s*(Z|[+-]\d{1,2}(?::?\d\d)?)\s*$"  # (measured: -0, +5 and +5:30 are offsets)

# Each macro reads its argument once (through a one-element list when it needs it twice): DuckDB expands a macro's
# arguments where they are written, and nested readings would otherwise grow exponentially
MACROS = [
    f"create or replace macro {_NS}_tz_is(x) as typeof(x) = '{SHOWN}'",
    # (no instant: a NULL, not a struct of NULLs, which COALESCE and IS NULL would take for a value)
    f"""create or replace macro {_NS}_tz_make(i, o) as list_transform([i::timestamptz],
          lambda _fx_mi: case when _fx_mi is not null then struct_pack(\"I\" := _fx_mi, \"O\" := o::bigint) end)[1]""",
    # (its own lambda variable: an `o` passed in may name the caller's `v`)
    f"""create or replace macro {_NS}_tz_zoned(zone, t) as list_transform([t::timestamptz],
          lambda v: {_NS}_tz_make(v, ((epoch(timezone(zone, v)) - epoch(v)) / 60)::bigint))[1]""",
    # an instant at the session's time zone (UTC unless ALTER SESSION SET TIMEZONE said otherwise)
    f"create or replace macro {_NS}_tz_local(t) as {_NS}_tz_zoned(current_setting('TimeZone'), t)",
    f"""create or replace macro {_NS}_tz_minutes(z) as case when z is null or z = '' or z = 'Z' then 0
          else (case when z[1] = '-' then -1 else 1 end) * (try_cast(regexp_extract(z, '^[+-]([0-9]{{1,2}})', 1) as integer) * 60
            + coalesce(try_cast(nullif(regexp_extract(z, '^[+-][0-9]{{1,2}}:?([0-9][0-9])$', 1), '') as integer), 0)) end""",
    f"""create or replace macro {_NS}_tz_parse(s) as list_transform([regexp_extract(s::varchar, '{_SPLIT_SQL}', ['w', 'z'])],
          lambda p: case when p.z = '' and s is not null
            then {_NS}_tz_local(timezone(current_setting('TimeZone'), cast(s::varchar as timestamp)))
            else {_NS}_tz_make(timezone('UTC', cast(coalesce(nullif(p.w, ''), s::varchar) as timestamp)
              - to_minutes({_NS}_tz_minutes(p.z)::bigint)), {_NS}_tz_minutes(p.z)) end)[1]""",
    f"""create or replace macro {_NS}_tz_try_parse(s) as list_transform([regexp_extract(s::varchar, '{_SPLIT_SQL}', ['w', 'z'])],
          lambda p: case when p.z = '' and s is not null
            then {_NS}_tz_local(timezone(current_setting('TimeZone'), try_cast(s::varchar as timestamp)))
            else {_NS}_tz_make(timezone('UTC', try_cast(coalesce(nullif(p.w, ''), s::varchar) as timestamp)
              - to_minutes({_NS}_tz_minutes(p.z)::bigint)), {_NS}_tz_minutes(p.z)) end)[1]""",
    f"create or replace macro {_NS}_tz_wall_in(s) as coalesce(nullif(regexp_extract(s::varchar, '{_SPLIT_SQL}', 1), ''), s::varchar)",
    f"create or replace macro {_NS}_tz_offset_in(s) as {_NS}_tz_minutes(regexp_extract(s::varchar, '{_SPLIT_SQL}', 2))",
    # a value of a type not known when the statement is written: dispatched on its type when it binds (through text
    # for the struct: a cast from another STRUCT would not bind)
    f"""create or replace macro {_NS}_tz_of(x) as case
          when {_NS}_tz_is(x) then try_cast(try_cast(x as varchar) as {STRUCT})
          when typeof(x) = 'VARCHAR' then {_NS}_tz_try_parse(try_cast(x as varchar))
          when typeof(x) = 'JSON' then {_NS}_tz_try_parse(try_cast(x as json) ->> '$')
          else {_NS}_tz_local(try_cast(x as timestamptz)) end""",
    f"create or replace macro {_NS}_tz_instant(x) as {_NS}_tz_of(x).i",
    f"create or replace macro {_NS}_tz_instant_of(x) as try_cast(x as timestamptz)",
    f"create or replace macro {_NS}_tz_wall_s(s) as list_transform([s], lambda v: timezone('UTC', v.i) + to_minutes(v.o::bigint))[1]",
    f"create or replace macro {_NS}_tz_wall(x) as {_NS}_tz_wall_s({_NS}_tz_of(x))",
    f"create or replace macro {_NS}_tz_rewall_o(w, o) as list_transform([o], lambda v: {_NS}_tz_make(timezone('UTC', w::timestamp - to_minutes(v::bigint)), v))[1]",
    f"""create or replace macro {_NS}_tz_sign(o) as case when o < 0 then '-' else '+' end
          || lpad((abs(o) // 60)::varchar, 2, '0')""",
    f"""create or replace macro {_NS}_tz_offset_text(o, colon) as case when o = 0 then 'Z'
          else {_NS}_tz_sign(o) || case when colon then ':' else '' end || lpad((abs(o) % 60)::varchar, 2, '0') end""",
    f"""create or replace macro {_NS}_tz_text_s(s) as list_transform([s], lambda v: case when v is not null then
          strftime(timezone('UTC', v.i) + to_minutes(v.o::bigint), '%Y-%m-%d %H:%M:%S.%g') || ' ' || {_NS}_tz_offset_text(v.o, false) end)[1]""",
    f"create or replace macro {_NS}_tz_text(x) as {_NS}_tz_text_s({_NS}_tz_of(x))",
    f"""create or replace macro {_NS}_tz_part_o(o, part) as case part
          when 'TZH:TZM' then {_NS}_tz_offset_text(o, true)
          when 'TZHTZM' then {_NS}_tz_offset_text(o, false)
          when 'TZH' then case when o = 0 then 'Z' else {_NS}_tz_sign(o) end
          else lpad((abs(o) % 60)::varchar, 2, '0') end""",
]


# In a SELECT with GROUP BY, DuckDB takes a lambda's `v.i` for a column that is not grouped ("column i must appear in the
# GROUP BY clause"): there the same macros are used without lambdas, under the prefix _tzg_ (they read an argument as
# often as they name it, which is why they are not the default)
_FLAT = {
    "_tz_make": f"create or replace macro {_NS}_tzg_make(i, o) as case when i is not null then struct_pack(\"I\" := i::timestamptz, \"O\" := o::bigint) end",
    "_tz_zoned": f"""create or replace macro {_NS}_tzg_zoned(zone, t) as {_NS}_tzg_make(t::timestamptz,
          ((epoch(timezone(zone, t::timestamptz)) - epoch(t::timestamptz)) / 60)::bigint)""",
    "_tz_wall_s": f"create or replace macro {_NS}_tzg_wall_s(s) as timezone('UTC', s.i) + to_minutes(s.o::bigint)",
    "_tz_rewall_o": f"create or replace macro {_NS}_tzg_rewall_o(w, o) as {_NS}_tzg_make(timezone('UTC', w::timestamp - to_minutes(o::bigint)), o)",
    "_tz_text_s": f"""create or replace macro {_NS}_tzg_text_s(s) as case when s is not null then
          strftime(timezone('UTC', s.i) + to_minutes(s.o::bigint), '%Y-%m-%d %H:%M:%S.%g') || ' ' || {_NS}_tzg_offset_text(s.o, false) end""",
}


def _grouped_copies() -> list[str]:
    out = []
    for macro in MACROS:
        name = re.search(r"macro " + re.escape(_NS) + r"(_tz_\w+)", macro)
        if name is None:
            continue
        out.append(_FLAT.get(name.group(1)) or macro.replace(_NS + "_tz_", _NS + "_tzg_"))
    return out


MACROS += _grouped_copies()


def _grouped(expression: exp.Expression) -> exp.Expression:
    """In each SELECT with GROUP BY or an aggregate, the _tzg_ forms of the macros (see _FLAT)."""
    for select in expression.find_all(exp.Select):
        if select.args.get("group") is None and not any(
                a.find_ancestor(exp.Select) is select and not isinstance(a.parent, exp.Window) for a in select.find_all(exp.AggFunc)):
            continue
        for call in select.find_all(exp.Anonymous):
            if call.find_ancestor(exp.Select) is select and str(call.this).startswith(_NS + "_tz_"):
                call.set("this", _NS + "_tzg_" + str(call.this)[len(_NS + "_tz_"):])
    return expression


def _fn(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=_NS + name, expressions=list(args))


def _is_tz_type(t: exp.DataType | None) -> bool:
    return t is not None and t.is_type(exp.DataType.Type.TIMESTAMPTZ)


# -- what DuckDB's catalog says, as Snowflake types ------------------------------------------------------------------
def _snowflake_type(duck: str) -> str:
    d = duck.upper()
    if d == SHOWN.upper():
        return "TIMESTAMP_TZ"
    if d.startswith("TIMESTAMP WITH TIME ZONE"):
        return "TIMESTAMP_LTZ"
    if d.startswith("TIMESTAMP"):
        return "TIMESTAMP_NTZ"
    if d == "JSON":
        return "VARIANT"
    if d.startswith("DECIMAL"):
        return d.replace("DECIMAL", "NUMBER")
    if d in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER"):
        return "NUMBER(38,0)"
    if d in ("DOUBLE", "FLOAT", "REAL"):
        return "FLOAT"
    if d in ("BOOLEAN", "DATE", "TIME"):
        return d
    if d == "BLOB":
        return "BINARY"
    if d.endswith("]"):
        return "ARRAY"
    if d.startswith(("STRUCT", "MAP")):
        return "OBJECT"
    return "VARCHAR"


def _catalog(conn, names: set[str]) -> tuple[dict, bool]:  # noqa: ANN001
    """{database: {schema: {table: {column: type}}}} for the tables named, and whether one has a TIMESTAMP_TZ."""
    if not names:
        return {}, False
    rows = conn.execute(
        "select database_name, schema_name, table_name, column_name, data_type from duckdb_columns() "
        "where upper(table_name) in (" + ", ".join("?" for _ in names) + ")", [n.upper() for n in names]).fetchall()
    schema: dict = {}
    zoned = False
    for db, sch, table, column, kind in rows:
        zoned = zoned or str(kind).upper() == SHOWN.upper()
        schema.setdefault(db.upper(), {}).setdefault(sch.upper(), {}).setdefault(table.upper(), {})[column.upper()] = _snowflake_type(str(kind))
    return schema, zoned


_ZONED_NAMES = re.compile(r"timestamp_?tz|timestamptz|convert_timezone|timestamp with time zone", re.I)


def _mentions_tz(expression: exp.Expression) -> bool:
    for node in expression.walk():
        if isinstance(node, exp.DataType) and node.is_type(exp.DataType.Type.TIMESTAMPTZ):
            return True
        if isinstance(node, exp.ConvertTimezone) and node.args.get("source_tz") is None:
            return True
        if isinstance(node, (exp.Anonymous, exp.StrToTime)) and _ZONED_NAMES.search(str(node.name or "") + str(node.args.get("target_type") or "")):
            return True
        if isinstance(node, exp.TimestampTzFromParts):
            return True
    return False


def _mentions_ltz(expression: exp.Expression) -> bool:
    """TO_CHAR of an instant, which shows the session's offset."""
    return any(isinstance(n, (exp.ToChar, exp.TimeToStr)) for n in expression.walk())


def _local() -> bool:
    from . import semantics
    return semantics.SESSION_ZONE[0].upper() not in ("UTC", "ETC/UTC", "GMT", "Z")


# -- the rewrite -----------------------------------------------------------------------------------------------------
_PASS = (exp.AggFunc, exp.Coalesce, exp.If, exp.Case, exp.Greatest, exp.Least, exp.Nullif, exp.Window, exp.Paren,
         exp.Alias, exp.Column, exp.Subquery, exp.Select, exp.Ordered, exp.Order, exp.Group, exp.Distinct, exp.Tuple,
         exp.Array, exp.Struct, exp.Values, exp.Lag, exp.Lead, exp.FirstValue, exp.LastValue, exp.NthValue,
         exp.ArrayAgg, exp.Max, exp.Min, exp.AnyValue, exp.Count, exp.PartitionedByProperty, exp.Star, exp.Is,
         exp.Not, exp.Unnest, exp.Lateral, exp.Explode, exp.ToVariant, exp.JSONFormat, exp.ArrayConstruct if hasattr(exp, "ArrayConstruct") else exp.Array)
_TEXT = (exp.Like, exp.ILike, exp.Upper, exp.Lower, exp.Length, exp.Substring, exp.Left, exp.Right, exp.StrPosition,
         exp.RegexpLike, exp.RegexpExtract, exp.Trim, exp.Split, exp.StartsWith, exp.Initcap, exp.Reverse, exp.MD5,
         exp.SHA, exp.SHA2, exp.Concat, exp.DPipe, exp.GroupConcat)
_COMPARE = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ)
_DAYS_OR_MORE = {"DAY", "DAYS", "D", "DD", "DAYOFMONTH", "WEEK", "WEEKS", "W", "WK", "WEEKOFYEAR", "WOY", "WY",
                 "MONTH", "MONTHS", "MM", "MON", "MONS", "QUARTER", "QUARTERS", "Q", "QTR", "QTRS", "YEAR", "YEARS",
                 "Y", "YY", "YYY", "YYYY", "YR", "YRS"}
_TZ_FORMAT = re.compile(r"TZH:TZM|TZHTZM|TZH|TZM", re.I)


def _unit(node: exp.Expression) -> str:
    unit = node.args.get("unit")
    return (unit.name if unit is not None else "DAY").upper()


_MAKERS = {"_tz_parse", "_tz_try_parse", "_tz_make", "_tz_rewall_o", "_tz_zoned", "_tz_of", "_tz_local"}
_INSTANTS = {"_tz_instant", "_tz_instant_of"}
_WALLS = {"_tz_wall", "_tz_wall_s"}
_TEXTS = {"_tz_text", "_tz_text_s"}


def _own(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Anonymous) and isinstance(node.this, str) and node.this.startswith(_NS):
        return node.this[len(_NS):]
    return None


def _field(x: exp.Expression, name: str) -> exp.Expression:
    return exp.Anonymous(this="struct_extract", expressions=[x, exp.Literal.string(name)])


class _Pass:
    def __init__(self, types: dict[int, exp.DataType], catalog: dict, database: str | None, schema: str | None):
        self.types, self.catalog, self.database, self.schema = types, catalog, database, schema

    def kind(self, node: exp.Expression | None) -> str:  # noqa: PLR0911
        """tz, ltz, ntz, date, text, json, number, null or unknown."""
        if node is None:
            return "unknown"
        own = _own(node)
        if own in _MAKERS:
            return "tz"
        if own in _INSTANTS:
            return "ltz"
        if own in _WALLS:
            return "ntz"
        if own in _TEXTS:
            return "text"
        if isinstance(node, exp.Paren):
            return self.kind(node.this)
        if isinstance(node, (exp.Coalesce, exp.If, exp.Case, exp.Greatest, exp.Least)) and any(self.tz(v) for v in self._values(node)):
            return "tz"  # (fix makes every value of it a TIMESTAMP_TZ: `coalesce(lead(ts) over (...), convert_timezone(...))`)
        if isinstance(node, exp.Null):
            return "null"
        if isinstance(node, exp.Literal):
            return "text" if node.is_string else "number"
        if isinstance(node, (exp.Cast, exp.TryCast)):
            return self._of_type(node.to)
        t = self.types.get(node.meta.get("fx"))
        if (t is None or t.is_type(exp.DataType.Type.UNKNOWN)) and isinstance(node, exp.Column) and not node.table:
            select = node.find_ancestor(exp.Select)
            for item in select.expressions if select is not None else []:
                if isinstance(item, exp.Alias) and item.alias.upper() == node.name.upper() and not node.find_ancestor(exp.Alias) is item:
                    return self.kind(item.this)
        return self._of_type(t)

    @staticmethod
    def _of_type(t: exp.DataType | None) -> str:  # noqa: PLR0911
        T = exp.DataType.Type
        if t is None or t.is_type(T.UNKNOWN):
            return "unknown"
        if t.is_type(T.TIMESTAMPTZ):
            return "tz"
        if t.is_type(T.TIMESTAMPLTZ):
            return "ltz"
        if t.is_type(T.TIMESTAMP, T.TIMESTAMPNTZ, T.DATETIME, T.TIMESTAMP_S, T.TIMESTAMP_MS, T.TIMESTAMP_NS):
            return "ntz"
        if t.is_type(T.DATE):
            return "date"
        if t.is_type(*exp.DataType.TEXT_TYPES):
            return "text"
        if t.is_type(T.JSON, T.VARIANT, T.OBJECT, T.ARRAY, T.STRUCT, T.MAP):
            return "json"
        if t.is_type(*exp.DataType.NUMERIC_TYPES):
            return "number"
        if t.is_type(T.NULL):
            return "null"
        return "other"

    def tz(self, node: exp.Expression | None) -> bool:
        return self.kind(node) == "tz"

    def columns_of(self, table: exp.Table) -> list[tuple[str, str]]:
        db = (table.catalog or self.database or "").upper()
        sch = (table.db or self.schema or "").upper()
        cols = self.catalog.get(db, {}).get(sch, {}).get(table.name.upper())
        if cols is None:  # (named elsewhere: by its name alone)
            cols = next((t[table.name.upper()] for s in self.catalog.values() for t in s.values() if table.name.upper() in t), {})
        return list(cols.items())

    # a value as one of the forms, by what it is known to be
    def to_tz(self, x: exp.Expression) -> exp.Expression:
        k = self.kind(x)
        if k == "null":  # (a NULL of the type: CAST(NULL AS TIMESTAMP_TZ) is described as one, dbt's contracts read it so)
            return exp.Cast(this=exp.null(), to=exp.DataType(this=exp.DataType.Type.USERDEFINED, kind=STRUCT))
        if k == "tz":
            return x
        if k == "text":
            return _fn("_tz_parse" if isinstance(x, exp.Literal) else "_tz_try_parse", x)
        if k in ("ntz", "ltz", "date"):
            return _fn("_tz_local", _fn("_tz_instant_of", x))
        if k == "number":
            return _fn("_tz_local", _fn("_tz_instant_of", exp.cast(x, "timestamp_ntz")))
        return _fn("_tz_of", x)

    def instant(self, x: exp.Expression) -> exp.Expression:
        k = self.kind(x)
        if k == "null":
            return x
        if k == "tz":
            return _field(x, "i")
        if k == "text" and isinstance(x, exp.Literal):
            return _field(_fn("_tz_parse", x), "i")
        if k in ("ntz", "ltz", "date", "text"):
            return _fn("_tz_instant_of", x)
        return _fn("_tz_instant", x)

    def wall(self, x: exp.Expression) -> exp.Expression:
        k = self.kind(x)
        if k in ("ntz", "date", "null"):
            return x
        if k == "tz":
            return _fn("_tz_wall_s", x)
        if k == "ltz":
            return exp.Anonymous(this="timezone", expressions=[exp.Literal.string("UTC"), x])
        if k == "text":
            return _fn("_tz_wall_s", self.to_tz(x))
        return _fn("_tz_wall", x)

    def utc(self, x: exp.Expression) -> exp.Expression:
        k = self.kind(x)
        if k in ("ntz", "date", "null"):
            return x
        return exp.Anonymous(this="timezone", expressions=[exp.Literal.string("UTC"), self.instant(x)])

    def text(self, x: exp.Expression) -> exp.Expression:
        return _fn("_tz_text_s", x) if self.tz(x) else _fn("_tz_text", x)

    def offset(self, x: exp.Expression) -> exp.Expression:
        return _field(x if self.tz(x) else self.to_tz(x), "o")

    def rewall(self, node: exp.Expression, original: exp.Expression) -> exp.Expression:
        return _fn("_tz_rewall_o", node, self.offset(original))

    def fix(self, node: exp.Expression) -> exp.Expression:  # noqa: C901, PLR0911, PLR0912 - one rule per kind of node
        tz = self.tz
        # making one
        if isinstance(node, (exp.Cast, exp.TryCast)) and node.to.is_type(exp.DataType.Type.TIMESTAMPTZ):
            inner = node.this
            if isinstance(inner, exp.Literal) and inner.is_string:
                return _fn("_tz_try_parse" if isinstance(node, exp.TryCast) else "_tz_parse", inner)
            return self.to_tz(inner)
        if isinstance(node, exp.Anonymous) and str(node.name).upper() in ("TO_TIMESTAMP_TZ", "TRY_TO_TIMESTAMP_TZ") \
                and len(node.expressions) == 1:
            arg = node.expressions[0]
            if isinstance(arg, exp.Literal) and arg.is_string:
                return _fn("_tz_parse" if str(node.name).upper() == "TO_TIMESTAMP_TZ" else "_tz_try_parse", arg)
            return self.to_tz(arg)
        if isinstance(node, exp.StrToTime) and node.args.get("target_type") is not None \
                and node.args["target_type"].is_type(exp.DataType.Type.TIMESTAMPTZ):
            return self.formatted(node)
        if isinstance(node, exp.UnixToTime) and node.args.get("target_type") is not None \
                and node.args["target_type"].is_type(exp.DataType.Type.TIMESTAMPTZ):
            node.set("target_type", None)
            return _fn("_tz_local", _fn("_tz_instant_of", node))
        if isinstance(node, exp.ConvertTimezone) and node.args.get("source_tz") is None and node.args.get("timestamp") is not None:
            return _fn("_tz_zoned", node.args["target_tz"], self.instant(node.args["timestamp"]))
        if isinstance(node, exp.ConvertTimezone) and node.args.get("source_tz") is not None and tz(node.args.get("timestamp")):
            from . import semantics
            node.set("timestamp", exp.Anonymous(this=semantics.ZONED_ARGUMENT, expressions=[node.args["timestamp"]]))
            return node
        if isinstance(node, exp.ColumnDef) and isinstance(node.args.get("kind"), exp.DataType) \
                and node.args["kind"].is_type(exp.DataType.Type.TIMESTAMPTZ):
            node.set("kind", exp.DataType.build(STRUCT, dialect="duckdb"))
            return node
        if isinstance(node, (exp.DateAdd, exp.DateSub, exp.TimestampAdd, exp.TimestampSub, exp.AddMonths,
                             exp.TimestampTrunc, exp.DateTrunc)) and tz(node.this):
            original = node.this
            node.set("this", self.wall(original.copy()))
            return self.rewall(node, original)
        if isinstance(node, (exp.Add, exp.Sub)) and isinstance(node.expression, exp.Interval) and tz(node.this):
            original = node.this
            node.set("this", self.wall(original.copy()))
            return self.rewall(node, original)
        # reading one
        if isinstance(node, _COMPARE) and (tz(node.this) or tz(node.expression)):
            node.set("this", self.instant(node.this))
            node.set("expression", self.instant(node.expression))
            return node
        if isinstance(node, exp.Between) and any(tz(node.args.get(k)) for k in ("this", "low", "high")):
            for k in ("this", "low", "high"):
                node.set(k, self.instant(node.args[k]))
            return node
        if isinstance(node, exp.In) and (tz(node.this) or any(tz(e) for e in node.expressions)) and not node.args.get("query"):
            node.set("this", self.instant(node.this))
            node.set("expressions", [self.instant(e) for e in node.expressions])
            return node
        if isinstance(node, (exp.DateDiff, exp.TimestampDiff)) and (tz(node.this) or tz(node.expression)):
            to = self.wall if _unit(node) in _DAYS_OR_MORE else self.utc
            node.set("this", to(node.this))
            node.set("expression", to(node.expression))
            return node
        if isinstance(node, exp.Extract) and tz(node.expression):
            part = node.this.name.upper()
            value = node.expression
            if part.startswith("EPOCH"):
                node.set("expression", self.utc(value))
            elif part in ("TIMEZONE_HOUR", "TZH"):
                return exp.cast(exp.func("trunc", exp.Div(this=self.offset(value), expression=exp.Literal.number(60))), "int")
            elif part in ("TIMEZONE_MINUTE", "TZM"):
                return exp.cast(exp.Mod(this=self.offset(value), expression=exp.Literal.number(60)), "int")
            else:
                node.set("expression", self.wall(value))
            return node
        if isinstance(node, (exp.Cast, exp.TryCast)) and tz(node.this):
            to = node.to
            if to.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return self.instant(node.this)
            if to.is_type(*exp.DataType.TEXT_TYPES):
                return self.text(node.this)
            if to.is_type("variant", "json", "object", "array"):
                return node
            node.set("this", self.wall(node.this))
            return node
        if isinstance(node, exp.TimeToStr) and isinstance(node.args.get("format"), exp.Literal) \
                and (tz(node.this) or (self.kind(node.this) == "ltz" and _local())):
            # TO_CHAR of a typed value: the format already strftime's; its offsets from the value's own
            value = node.this if tz(node.this) else _fn("_tz_local", node.this)
            if not re.search(r"%[:-]?z", node.args["format"].name):
                node.set("this", self.wall(value))
                return node
            return self.formatted_strftime(value, node.args["format"].name)
        if isinstance(node, exp.ToChar) and self.kind(node.this) == "ltz" and isinstance(node.args.get("format"), exp.Literal) \
                and _TZ_FORMAT.search(node.args["format"].name):
            return self.formatted_text(_fn("_tz_local", node.this), node.args["format"].name)
        if isinstance(node, exp.ToChar) and tz(node.this):
            fmt = node.args.get("format")
            if fmt is None:
                return self.text(node.this)
            if isinstance(fmt, exp.Literal) and fmt.is_string:
                return self.formatted_text(node.this, fmt.name)
            node.set("this", self.wall(node.this))
            return node
        if isinstance(node, exp.Anonymous) and str(node.name).upper() in ("TO_TIMESTAMP_LTZ", "TRY_TO_TIMESTAMP_LTZ") \
                and node.expressions and tz(node.expressions[0]):
            return self.instant(node.expressions[0])
        if isinstance(node, exp.GroupConcat) and isinstance(node.this, exp.Order) and tz(node.this.this):
            node.this.set("this", self.text(node.this.this))
            return node
        if isinstance(node, exp.Select) and node.args.get("group") is not None:
            self.grouped(node)
            return node
        if isinstance(node, _TEXT):
            for key, value in list(node.args.items()):
                if isinstance(value, exp.Expression) and tz(value):
                    node.set(key, self.text(value))
                elif isinstance(value, list):
                    node.set(key, [self.text(v) if isinstance(v, exp.Expression) and tz(v) else v for v in value])
            return node
        if isinstance(node, (exp.Coalesce, exp.If, exp.Case, exp.Greatest, exp.Least)) and (
                tz(node) or any(tz(v) for v in self._values(node))):
            if isinstance(node, exp.If):
                for key in ("true", "false"):
                    if node.args.get(key) is not None:
                        node.set(key, self.to_tz(node.args[key]))
            elif isinstance(node, exp.Case):
                for branch in node.args.get("ifs") or []:
                    branch.set("true", self.to_tz(branch.args["true"]))
                if node.args.get("default") is not None:
                    node.set("default", self.to_tz(node.args["default"]))
            else:
                node.set("this", self.to_tz(node.this))
                node.set("expressions", [self.to_tz(e) for e in node.expressions])
            return node
        if isinstance(node, exp.Distinct) and isinstance(node.parent, exp.Count):
            node.set("expressions", [self.instant(e) if tz(e) else e for e in node.expressions])
            return node
        if isinstance(node, exp.Func) and not isinstance(node, _PASS):
            for key, value in list(node.args.items()):
                if isinstance(value, exp.Expression) and not isinstance(value, (exp.Literal, exp.Var)) and tz(value):
                    node.set(key, self.wall(value))
                elif isinstance(value, list):
                    node.set(key, [self.wall(v) if isinstance(v, exp.Expression) and tz(v) else v for v in value])
        return node

    def grouped(self, select: exp.Select) -> None:
        """GROUP BY a TIMESTAMP_TZ goes by its instant (one group for 12:00 +0200 and 10:00 Z); the select list shows
        one of the group's values."""
        group = select.args["group"]
        keyed = []
        for g in group.expressions:
            target = g
            if isinstance(g, exp.Literal) and g.is_int and 0 < int(g.name) <= len(select.expressions):
                item = select.expressions[int(g.name) - 1]
                target = item.this if isinstance(item, exp.Alias) else item
            if self.tz(target) and not isinstance(target, exp.Literal):
                keyed.append(target.copy())
                g.replace(self.instant(target.copy()))
        if not keyed:
            return

        def one_of(node: exp.Expression) -> exp.Expression:
            if any(node == k for k in keyed) and not node.find_ancestor(exp.AggFunc, exp.Window):
                return exp.Anonymous(this="any_value", expressions=[node.copy()])
            return node

        for i, item in enumerate(list(select.expressions)):
            name = item.alias_or_name if isinstance(item, exp.Column) else None
            changed = item.transform(one_of, copy=True)
            if name and changed != item:
                changed = exp.alias_(changed, exp.to_identifier(name))
            select.expressions[i] = changed
            changed.parent = select
        for key in ("having", "qualify", "order"):
            part = select.args.get(key)
            if part is not None:
                select.set(key, part.transform(one_of, copy=True))

    @staticmethod
    def _values(node: exp.Expression) -> list[exp.Expression]:
        if isinstance(node, exp.If):
            return [v for v in (node.args.get("true"), node.args.get("false")) if v is not None]
        if isinstance(node, exp.Case):
            return [b.args["true"] for b in node.args.get("ifs") or []] + ([node.args["default"]] if node.args.get("default") else [])
        return [node.this, *node.expressions]

    def formatted(self, node: exp.StrToTime) -> exp.Expression:
        """TO_TIMESTAMP_TZ(text, format): the wall clock read by the format without its offset, and the offset."""
        fmt = node.args.get("format")
        text = node.this
        if isinstance(fmt, exp.Literal) and fmt.is_string and (_TZ_FORMAT.search(fmt.name) or re.search(r"%[:-]?z", fmt.name)):
            plain = re.sub(r"\s*(TZH:TZM|TZHTZM|TZH|%[:-]?z)", "", fmt.name, flags=re.I)
            wall = exp.StrToTime(this=_fn("_tz_wall_in", text.copy()), format=exp.Literal.string(plain),
                                 safe=node.args.get("safe"), target_type=exp.DataType.build("TIMESTAMP_NTZ", dialect="snowflake"))
            return _fn("_tz_rewall_o", wall, _fn("_tz_offset_in", text.copy()))
        node.set("target_type", exp.DataType.build("TIMESTAMP_NTZ", dialect="snowflake"))
        return _fn("_tz_local", _fn("_tz_instant_of", node))

    def formatted_text(self, value: exp.Expression, fmt: str) -> exp.Expression:
        """TO_CHAR(tz, format): the format's offset tokens from the value's own offset, the rest from its wall clock."""
        pieces: list[exp.Expression] = []
        at = 0
        for m in _TZ_FORMAT.finditer(fmt):
            if m.start() > at:
                pieces.append(exp.ToChar(this=self.wall(value.copy()), format=exp.Literal.string(fmt[at:m.start()])))
            pieces.append(_fn("_tz_part_o", self.offset(value.copy()), exp.Literal.string(m.group(0).upper())))
            at = m.end()
        if at < len(fmt):
            pieces.append(exp.ToChar(this=self.wall(value.copy()), format=exp.Literal.string(fmt[at:])))
        if not pieces:
            return exp.Literal.string("")
        out = pieces[0]
        for p in pieces[1:]:
            out = exp.DPipe(this=out, expression=p, safe=True)
        return out

    def formatted_strftime(self, value: exp.Expression, fmt: str) -> exp.Expression:
        pieces: list[exp.Expression] = []
        at = 0
        for m in re.finditer(r"%[:-]?z", fmt):
            if m.start() > at:
                pieces.append(exp.TimeToStr(this=self.wall(value.copy()), format=exp.Literal.string(fmt[at:m.start()])))
            pieces.append(_fn("_tz_part_o", self.offset(value.copy()), exp.Literal.string("TZH:TZM" if m.group(0) == "%:z" else "TZHTZM")))
            at = m.end()
        if at < len(fmt):
            pieces.append(exp.TimeToStr(this=self.wall(value.copy()), format=exp.Literal.string(fmt[at:])))
        out = pieces[0]
        for p in pieces[1:]:
            out = exp.DPipe(this=out, expression=p, safe=True)
        return out

    def writes(self, node: exp.Expression) -> None:
        """INSERT, UPDATE and MERGE into a TIMESTAMP_TZ column: the value made one."""
        if isinstance(node, exp.Insert) and isinstance(node.this, (exp.Table, exp.Schema)):
            table = node.this if isinstance(node.this, exp.Table) else node.this.this
            columns = self.columns_of(table)
            named = [c.name.upper() for c in node.this.expressions] if isinstance(node.this, exp.Schema) else [c for c, _ in columns]
            kinds = dict(columns)
            zoned = [i for i, c in enumerate(named) if kinds.get(c) == "TIMESTAMP_TZ"]
            if not zoned:
                return
            source = node.expression
            if isinstance(source, exp.Values):
                for row in source.expressions:
                    if isinstance(row, exp.Tuple):
                        row.set("expressions", [self.to_tz(e) if i in zoned else e for i, e in enumerate(row.expressions)])
                return
            selects = [source] if isinstance(source, exp.Select) else [
                s for s in source.find_all(exp.Select) if s.parent is source or isinstance(s.parent, exp.SetOperation)] \
                if isinstance(source, exp.SetOperation) else []
            for select in selects:
                if any(isinstance(e, exp.Star) for e in select.expressions):
                    continue
                for i in zoned:
                    if i < len(select.expressions):
                        item = select.expressions[i]
                        target = item.this if isinstance(item, exp.Alias) else item
                        if not self.tz(target):
                            target.replace(self.to_tz(target.copy()))
        if isinstance(node, exp.Update) and isinstance(node.this, exp.Table):
            kinds = dict(self.columns_of(node.this))
            for eq in node.expressions:
                if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column) and kinds.get(eq.this.name.upper()) == "TIMESTAMP_TZ":
                    eq.set("expression", self.to_tz(eq.expression))
        if isinstance(node, exp.Merge) and isinstance(node.this, exp.Table):
            kinds = dict(self.columns_of(node.this))
            for when in node.find_all(exp.When):
                then = when.args.get("then")
                if isinstance(then, exp.Update):
                    for eq in then.expressions:
                        if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column) and kinds.get(eq.this.name.upper()) == "TIMESTAMP_TZ":
                            eq.set("expression", self.to_tz(eq.expression))
                if isinstance(then, exp.Insert) and isinstance(then.this, exp.Tuple) and isinstance(then.expression, exp.Tuple):
                    names = [c.name.upper() for c in then.this.expressions]
                    then.expression.set("expressions", [self.to_tz(v) if kinds.get(n) == "TIMESTAMP_TZ" else v
                                                        for n, v in zip(names, then.expression.expressions)])

    def unions(self, node: exp.Expression) -> None:
        """A UNION whose column is a TIMESTAMP_TZ in one branch: the other branches' values made one too."""
        for union in node.find_all(exp.SetOperation):
            branches = [s for s in (union.left, union.right) if isinstance(s, exp.Select)]
            if len(branches) < 2 or any(any(isinstance(e, exp.Star) for e in b.expressions) for b in branches):
                continue
            for i in range(min(len(b.expressions) for b in branches)):
                values = [b.expressions[i].this if isinstance(b.expressions[i], exp.Alias) else b.expressions[i] for b in branches]
                if any(self.tz(v) for v in values):
                    for v in values:
                        if not self.tz(v) and not isinstance(v, exp.Null):
                            v.replace(self.to_tz(v.copy()))


def _annotated(expression: exp.Expression, catalog: dict, database: str | None, schema: str | None) -> dict[int, exp.DataType]:
    counter = itertools.count()
    for node in expression.walk():
        node.meta["fx"] = next(counter)
    copy = expression.copy()
    for values in copy.find_all(exp.Values):  # (Snowflake names their columns COLUMN1, COLUMN2, ...)
        first = values.expressions[0] if values.expressions else None
        width = len(first.expressions) if isinstance(first, exp.Tuple) else 1
        alias = values.args.get("alias")
        if alias is None or not alias.columns:
            values.set("alias", exp.TableAlias(this=exp.to_identifier(alias.name if alias is not None else "_fx_values"),
                                               columns=[exp.to_identifier(f"COLUMN{i + 1}") for i in range(width)]))
    kw = {"catalog": database, "db": schema} if database else {}
    try:
        copy = qualify(copy, schema=catalog or None, dialect="snowflake", validate_qualify_columns=False,
                       quote_identifiers=False, identify=False, **kw)
    except Exception:  # noqa: BLE001, S110 - typed without its columns resolved
        pass
    try:
        copy = annotate_types(copy, schema=catalog or None, dialect="snowflake")
    except Exception:  # noqa: BLE001
        return {}
    # `coalesce(lead(ts) over (...), convert_timezone(...))` is a TIMESTAMP_TZ once rewritten (see Zoned.fix); sqlglot types
    # it by its first value. Typed so, a CTE column made of it is a TIMESTAMP_TZ where the next CTE reads it
    branches = [n for n in copy.find_all(exp.Coalesce, exp.If, exp.Case, exp.Greatest, exp.Least)
                if not _is_tz_type(n.type) and any(_is_tz_type(v.type) for v in _Pass._values(n) if isinstance(v, exp.Expression))]
    if branches:
        for n in branches:
            n.replace(exp.Cast(this=n.copy(), to=exp.DataType.build("TIMESTAMPTZ")))
        try:
            copy = annotate_types(copy, schema=catalog or None, dialect="snowflake")
        except Exception:  # noqa: BLE001, S110 - the first typing stands
            pass
    return {n.meta["fx"]: n.type for n in copy.walk() if "fx" in n.meta and n.type is not None}


def rewrite(expression: exp.Expression, conn, database: str | None, schema: str | None) -> exp.Expression:  # noqa: ANN001
    """The statement with its TIMESTAMP_TZ values held as (instant, offset) and read the way Snowflake reads them."""
    names = {t.name for t in expression.find_all(exp.Table) if t.name}
    try:
        catalog, stored = _catalog(conn, names)
    except Exception:  # noqa: BLE001
        catalog, stored = {}, False
    from . import semantics
    local = semantics.SESSION_ZONE[0].upper() not in ("UTC", "ETC/UTC", "GMT", "Z")
    if not stored and not _mentions_tz(expression) and not (local and _mentions_ltz(expression)):
        return expression
    db, sch = (database.upper() if database else None), (schema.upper() if schema else None)
    if db and sch and catalog:  # a table found elsewhere (a temporary one) is read where the statement names it
        here = catalog.setdefault(db, {}).setdefault(sch, {})
        for d, schemas in list(catalog.items()):
            for s, tables in schemas.items():
                for name, cols in tables.items():
                    here.setdefault(name, cols)
    types = _annotated(expression, catalog, db, sch)
    step = _Pass(types, catalog, database.upper() if database else None, schema.upper() if schema else None)
    step.writes(expression)
    step.unions(expression)
    return _grouped(_bottom_up(expression, step.fix))


def _bottom_up(node: exp.Expression, fn) -> exp.Expression:  # noqa: ANN001
    for child in list(node.iter_expressions()):
        parent, key, index = child.parent, child.arg_key, child.index
        new = _bottom_up(child, fn)
        if new is not child:
            parent.set(key, new, index)
    return fn(node)
