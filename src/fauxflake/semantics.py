"""Where Snowflake and DuckDB disagree on the *answer*, not the syntax.

The emulator transpiles SQL faithfully enough to run, but several functions then
return different values or types. Each fix here exists because a case in
`conformance/` failed against an answer recorded from real Snowflake.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

_NS = "_fs_global.main."
_SCALE = r"coalesce(try_cast(nullif(regexp_extract(typeof(a), ',(\d+)\)', 1), '') as integer), 0)"

MACROS = [
    # NUMBER / NUMBER keeps max(s, min(s + 6, 12)) decimals and raises on zero.
    # DuckDB returns a full-precision double, and NULL on zero.
    f"""create or replace macro {_NS}_sf_div(a, b) as case
          when b = 0 and a is not null then error('Division by zero')
          when typeof(a) in ('DOUBLE', 'FLOAT') or typeof(b) in ('DOUBLE', 'FLOAT') then a / b
          else round(a::double / b, greatest({_SCALE}, least({_SCALE} + 6, 12))) end""",
    # DATE_TRUNC keeps the input type. DuckDB turns a DATE into a TIMESTAMP.
    f"""create or replace macro {_NS}_sf_date_trunc
          (p varchar, x date) as date_trunc(p, x)::date,
          (p varchar, x timestamp) as date_trunc(p, x),
          (p varchar, x timestamptz) as date_trunc(p, x),
          (p varchar, x varchar) as date_trunc(p, x::timestamp)""",
    # TYPEOF looks inside a VARIANT. DuckDB only says JSON.
    f"""create or replace macro {_NS}_sf_typeof(v) as case when typeof(v) <> 'JSON' then typeof(v) else
          case json_type(v::varchar)
            when 'UBIGINT' then 'INTEGER' when 'BIGINT' then 'INTEGER' when 'DOUBLE' then 'DECIMAL'
            when 'NULL' then 'NULL_VALUE' else json_type(v::varchar) end end""",
    # A windowed AVG keeps three extra decimals, not six.
    f"""create or replace macro {_NS}_sf_div3(a, b) as case
          when typeof(a) in ('DOUBLE', 'FLOAT') then a / b
          else round(a::double / b, {_SCALE} + 3) end""",
    # VARIANCE of fixed-point input is fixed-point; STDDEV of integers is the root of that.
    f"""create or replace macro {_NS}_sf_var(v, a) as case
          when typeof(a) in ('DOUBLE', 'FLOAT') then v else round(v, least(2 * {_SCALE} + 6, 12)) end""",
    f"""create or replace macro {_NS}_sf_std(v, a) as case
          when typeof(a) in ('DOUBLE', 'FLOAT') or {_SCALE} > 0 then sqrt(v) else sqrt(round(v, 6)) end""",
    # Casting keeps the left operand's type: DATE + INTERVAL '1 month' is still a DATE.
    f"""create or replace macro {_NS}_sf_like
          (x date, r timestamp) as r::date,
          (x, r) as r""",
    # Floats print the way C's %g does: 100, 1e+20, 0.1.
    f"""create or replace macro {_NS}_sf_text(x) as case
          when typeof(x) in ('DOUBLE', 'FLOAT') then format('{{:.15g}}', x::double) else x::varchar end""",
    f"""create or replace macro {_NS}_sf_width_bucket(x, lo, hi, n) as case
          when x < lo then 0 when x >= hi then n + 1 else floor((x - lo) * n / (hi - lo))::bigint + 1 end""",
    f"create or replace macro {_NS}_sf_current_account_name() as 'FAUXFLAKE'",
    f"create or replace macro {_NS}_sf_try_base64(x) as try(from_base64(x)::varchar)",
    f"create or replace macro {_NS}_sf_try_timestamp(x) as try_cast(x::varchar as timestamp)",
    f"create or replace macro {_NS}_sf_array_distinct(l) as list_filter(l, lambda x, i: list_position(l, x) = i)",
    f"create or replace macro {_NS}_sf_jarowinkler(a, b) as round(jaro_winkler_similarity(upper(a), upper(b)) * 100)::bigint",
    f"create or replace macro {_NS}_sf_object_insert(o, k, v) as json_merge_patch(o::json, json_object(k::varchar, v))",
    f"create or replace macro {_NS}_sf_object_delete(o, k) as json_merge_patch(o::json, json_object(k::varchar, null))",
    f"create or replace macro {_NS}_sf_object_pick(o, k) as json_object(k::varchar, json_extract(o::json, '$.' || k))",
    *[f"create or replace macro {_NS}_sf_is_{name}(v) as json_type(v::varchar) in ({kinds})" for name, kinds in {
        "object": "'OBJECT'", "array": "'ARRAY'", "integer": "'UBIGINT', 'BIGINT'", "varchar": "'VARCHAR'",
        "char": "'VARCHAR'", "boolean": "'BOOLEAN'", "decimal": "'UBIGINT', 'BIGINT', 'DOUBLE'",
        "double": "'UBIGINT', 'BIGINT', 'DOUBLE'", "real": "'UBIGINT', 'BIGINT', 'DOUBLE'"}.items()],
    f"create or replace macro {_NS}_sf_as_integer(v) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT') then v::bigint end",
    f"create or replace macro {_NS}_sf_as_varchar(v) as case when json_type(v::varchar) = 'VARCHAR' then v::json ->> '$' end",
    f"""create or replace macro {_NS}_sf_split_to_table(s, d) as table
          select 1 as seq, unnest(range(1, len(string_split(s, d)) + 1)) as index, unnest(string_split(s, d)) as value""",
]

_RENAMED = {  # Snowflake function -> macro, same arguments
    "width_bucket": "_sf_width_bucket", "array_distinct": "_sf_array_distinct",
    "jarowinkler_similarity": "_sf_jarowinkler", "object_insert": "_sf_object_insert",
    "object_delete": "_sf_object_delete", "object_pick": "_sf_object_pick",
    "current_account_name": "_sf_current_account_name", "try_base64_decode_string": "_sf_try_base64",
    "as_integer": "_sf_as_integer", "as_varchar": "_sf_as_varchar", "as_char": "_sf_as_varchar",
    **{f"is_{k}": f"_sf_is_{k}" for k in ("object", "array", "integer", "varchar", "char", "boolean", "decimal", "double", "real")},
}
_DATE_TOKENS = re.compile(r"YYYY|YY|MM|DD|HH|MI|SS|MON|DY|AM|PM", re.IGNORECASE)
_HASHES = (exp.MD5, exp.SHA, exp.SHA2, exp.MD5Digest)
UDFS: dict[str, str] = {}  # user-defined functions from the spec: name -> macro name
_DAY_UNITS = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "YEARS", "QUARTERS", "MONTHS", "WEEKS", "DAYS"}
_ARITHMETIC = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)
_NUMERIC_TEXT = re.compile(r"^\s*-?\d+(\.\d+)?\s*$")

_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)


def _func_args(node: exp.Func) -> list[exp.Expression]:
    out: list[exp.Expression] = []
    for key in node.arg_types:
        value = node.args.get(key)
        if isinstance(value, exp.Expression):
            out.append(value)
        elif isinstance(value, list):
            out.extend(value)
    return out


_NUMBER_FORMAT = re.compile(r"^(FM)?(\$)?([09,]+)(?:\.([09]+))?$", re.IGNORECASE)


def _number_format(value: exp.Expression, model: str) -> exp.Expression | None:
    """TO_CHAR(number, '9,999.00') as a DuckDB format() call, for the common format models."""
    if re.fullmatch(r"X+", model.strip()):  # hexadecimal
        return sqlglot.parse_one(f"format('{{:X}}', ({value.sql(dialect='duckdb')})::bigint)", read="duckdb")
    m = _NUMBER_FORMAT.match(model.strip())
    if not m:
        return None
    fill, dollar, whole, frac = m.groups()
    decimals = len(frac or "")
    digits = whole.replace(",", "")
    spec = ("," if "," in whole else "") + f".{decimals}f"
    if "0" in digits and "," not in whole:  # zero padded to the width of the model
        spec = f"0{len(digits) + (decimals + 1 if decimals else 0)}.{decimals}f"
    text = f"format('{{:{spec}}}', ({value.sql(dialect='duckdb')})::double)"
    if dollar:
        text = f"concat('$', {text})"
    if not fill:  # Snowflake right-aligns in the model's width, plus one position for the sign
        text = f"lpad({text}, {len(model.strip()) + 1}, ' ')"
    return sqlglot.parse_one(text, read="duckdb")



def _fn(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=_NS + name, expressions=list(args))


def _over(window: exp.Window | None, agg: exp.Expression) -> exp.Expression:
    if window is None:
        return agg
    w = window.copy()
    w.set("this", agg)
    return w


def _avg(avg: exp.Avg, window: exp.Window | None = None) -> exp.Expression:
    """AVG has division's scale rule, so compute it as SUM / COUNT."""
    total = _over(window, exp.Sum(this=avg.this.copy()))
    count = _over(window, exp.Count(this=avg.this.copy()))
    return exp.Case(
        ifs=[exp.If(this=exp.EQ(this=count, expression=exp.Literal.number(0)), true=exp.Null())],
        default=_fn("_sf_div", total, count.copy()),
    )


def _operand(node: exp.Expression) -> exp.Expression:
    """Snowflake converts these implicitly inside arithmetic."""
    if isinstance(node, exp.Literal) and node.is_string and _NUMERIC_TEXT.match(node.name):
        return exp.Literal.number(node.name.strip())  # '1' + 1
    if isinstance(node, exp.JSONExtract):
        return exp.cast(node, "double")  # v:n + 1
    return node


def _fix(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Literal) and not node.is_string and "." in node.name and "e" not in node.name.lower():
        # 1.50 is NUMBER(2,1) in Snowflake: trailing zeros are not part of a literal's scale
        trimmed = node.name.rstrip("0").rstrip(".")
        return exp.Literal.number(trimmed or "0") if trimmed != node.name else node
    if isinstance(node, _ARITHMETIC):
        node.set("this", _operand(node.this))
        node.set("expression", _operand(node.expression))
    if isinstance(node, exp.DPipe):
        for side in ("this", "expression"):
            part = node.args[side]
            if isinstance(part, exp.JSONExtract):
                node.set(side, exp.cast(part, "varchar"))  # v:s || 'y'
            elif not (isinstance(part, exp.Literal) and part.is_string) and not isinstance(part, exp.DPipe):
                node.set(side, _fn("_sf_text", part))
    if isinstance(node, _COMPARISONS) or isinstance(node, exp.In):
        other = node.expression if isinstance(node, _COMPARISONS) else (node.expressions[0] if node.expressions else None)
        if isinstance(node.this, exp.JSONExtract) and isinstance(other, exp.Literal) and other.is_string:
            node.set("this", exp.cast(node.this, "varchar"))  # v:s = 'x' compares the value, not JSON text
    if isinstance(node, exp.TryCast) and node.to.is_type("timestamp", "timestampntz") and node.args.get("requires_string"):
        return _fn("_sf_try_timestamp", node.this)
    if isinstance(node, exp.UnixToTime) and isinstance(node.this, exp.Literal) and node.this.is_string \
            and _NUMERIC_TEXT.match(node.this.name):
        node.set("this", exp.Literal.number(node.this.name.strip()))  # TO_TIMESTAMP('1735689600') is an epoch
    if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_DATE" and len(node.expressions) == 1 \
            and isinstance(node.expressions[0], exp.Literal) and re.fullmatch(r"\d{8,}", node.expressions[0].name):
        return exp.cast(exp.UnixToTime(this=exp.Literal.number(node.expressions[0].name)), "date")
    if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
        renamed = next((_RENAMED[n.lower()] for n in type(node).sql_names() if n.lower() in _RENAMED), None)
        if renamed:
            return _fn(renamed, *_func_args(node))
    if isinstance(node, (exp.TimeToStr, exp.StrToTime, exp.StrToDate)) and isinstance(node.args.get("format"), exp.Literal):
        fmt = node.args["format"]
        fmt.set("this", fmt.name.replace("Mon", "%b").replace("Dy", "%a"))
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and node.to.is_type("varchar", "text") \
            and not node.to.expressions and not isinstance(node.this, (exp.JSONExtract, exp.Bracket)) \
            and not (isinstance(node.this, exp.Literal) and node.this.is_string):
        return _fn("_sf_text", node.this)
    if isinstance(node, exp.Add) and isinstance(node.expression, exp.Interval) \
            and (node.expression.unit and node.expression.unit.name.upper() in _DAY_UNITS):
        return _fn("_sf_like", node.this.copy(), node)
    if isinstance(node, exp.DateAdd) and node.unit and node.unit.name.upper() == "QUARTER":
        node.set("unit", exp.var("MONTH"))
        node.set("expression", exp.Mul(this=exp.paren(node.expression), expression=exp.Literal.number(3)))
    if isinstance(node, exp.MonthsBetween):
        return exp.func("round", node, exp.Literal.number(6))
    if isinstance(node, (exp.Variance, exp.VariancePop)) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_var", node, exp.Min(this=node.this.copy()))
    if isinstance(node, (exp.Stddev, exp.StddevSamp)) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_std", exp.Variance(this=node.this.copy()), exp.Min(this=node.this.copy()))
    if isinstance(node, exp.StddevPop) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_std", exp.VariancePop(this=node.this.copy()), exp.Min(this=node.this.copy()))
    if isinstance(node, exp.GroupConcat):
        if node.args.get("separator") is None:
            node.set("separator", exp.Literal.string(""))  # the default delimiter is empty, not a comma
        if not isinstance(node.parent, (exp.Window, exp.WithinGroup)):
            return exp.Coalesce(this=node, expressions=[exp.Literal.string("")])  # and no rows gives ''
    if isinstance(node, (exp.ToChar, exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate)) and isinstance(node.args.get("format"), exp.Literal):
        node.args["format"].set("this", node.args["format"].name.replace("Mon", "MON").replace("Dy", "DY"))
    if isinstance(node, exp.ToNumber) and isinstance(node.args.get("format"), exp.Literal) and "," in node.args["format"].name:
        scale = len(node.args["format"].name.partition(".")[2])
        return exp.cast(exp.func("replace", node.this, exp.Literal.string(","), exp.Literal.string("")), f"decimal(38, {scale})")
    if isinstance(node, exp.Anonymous) and node.name.lower() in _RENAMED:
        return _fn(_RENAMED[node.name.lower()], *node.expressions)
    if isinstance(node, exp.TableFromRows) and isinstance(node.this, exp.SplitToTable):
        call = exp.Table(this=_fn("_sf_split_to_table", node.this.this, node.this.expression))
        if node.args.get("alias"):
            call.set("alias", node.args["alias"])
        return call
    if isinstance(node, exp.Join) and node.args.get("match_condition") is not None:
        cond = node.args.pop("match_condition")  # ASOF JOIN ... MATCH_CONDITION (c) is ASOF JOIN ... ON c
        node.set("on", exp.and_(cond, node.args["on"]) if node.args.get("on") else cond)
    if isinstance(node, exp.Div):
        return _fn("_sf_div", node.this, node.expression)
    if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
        return _fn("_sf_date_trunc", exp.Literal.string(node.unit.name.lower()), node.this)
    if isinstance(node, _HASHES) and not (isinstance(node.this, exp.Literal) and node.this.is_string):
        node.set("this", _fn("_sf_text", node.this))  # MD5(42) hashes the text '42'
    if isinstance(node, exp.Anonymous) and node.name.lower() in UDFS:
        call = _fn(UDFS[node.name.lower()], *node.expressions)
        if isinstance(node.parent, exp.Dot):  # db.schema.my_udf(x): drop the qualification
            top = node.parent
            while isinstance(top.parent, exp.Dot):
                top = top.parent
            top.replace(call)
        return call
    if isinstance(node, exp.Typeof):
        return _fn("_sf_typeof", node.this)
    if isinstance(node, exp.ToChar) and isinstance(node.args.get("format"), exp.Literal):
        formatted = _number_format(node.this, node.args["format"].name)
        if formatted is not None:
            return formatted
        if _DATE_TOKENS.search(node.args["format"].name):
            # TO_CHAR(column, 'YYYYMMDD'): without a known column type the format was dropped
            from sqlglot.dialects.snowflake import Snowflake
            from sqlglot.time import format_time

            model = node.args["format"].name.replace("Mon", "MON").replace("Dy", "DY")
            fmt = format_time(model, Snowflake.TIME_MAPPING, Snowflake.TIME_TRIE)
            return exp.TimeToStr(this=exp.cast(node.this, "timestamp"), format=exp.Literal.string(fmt))
    if isinstance(node, exp.Avg) and not (isinstance(node.parent, exp.Window) and node.parent.this is node):
        return _avg(node)
    if isinstance(node, exp.Window):
        inner = node.this
        if isinstance(inner, exp.Avg):
            return _fn("_sf_div3", _over(node, exp.Sum(this=inner.this.copy())), _over(node, exp.Count(this=inner.this.copy())))
        if isinstance(inner, exp.Anonymous) and inner.expressions:
            name, arg = inner.name.lower(), inner.expressions[0]
            if name == "ratio_to_report":
                return _fn("_sf_div", arg.copy(), _over(node, exp.Sum(this=arg.copy())))
            if name == "conditional_true_event":
                flag = exp.Case(ifs=[exp.If(this=arg.copy(), true=exp.Literal.number(1))], default=exp.Literal.number(0))
                return _over(node, exp.Sum(this=flag))
    return node


def rewrite(node: exp.Expression) -> exp.Expression:
    """Apply the fixes bottom-up, so nested cases (a ratio of averages) all get rewritten."""
    for child in list(node.iter_expressions()):
        # remember the slot first: a fix may wrap the child, which re-parents it
        parent, key, index = child.parent, child.arg_key, child.index
        new = rewrite(child)
        if new is not child:
            parent.set(key, new, index)
    return _fix(node)


_AS_BIGINT = {"TINYINT", "SMALLINT", "UTINYINT", "USMALLINT", "UINTEGER"}
_SUPPORTED = {"BIGINT", "BLOB", "BOOLEAN", "DATE", "DOUBLE", "HUGEINT", "INTEGER", "JSON", "TIME", "TIMESTAMP",
              "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "UBIGINT", "VARCHAR", '"NULL"'}


def coerce_result(cursor) -> None:  # noqa: ANN001
    """Re-select result columns whose DuckDB type the Snowflake wire protocol cannot carry:
    ARRAY and OBJECT values travel as JSON, small integers as BIGINT, everything else as text."""
    if cursor._arrow_table is None or not cursor._last_sql or not cursor._duck_conn.description:
        return
    casts, changed = [], False
    for i, (name, kind, *_rest) in enumerate(cursor._duck_conn.description):
        kind, alias = str(kind).upper(), '"' + name.replace('"', '""') + '"'
        if kind in _SUPPORTED or kind.startswith("DECIMAL"):
            casts.append(f"_c{i} as {alias}")
            continue
        changed = True
        if kind.endswith("]") or kind.startswith(("STRUCT", "MAP", "UNION")):
            casts.append(f"to_json(_c{i})::JSON as {alias}")
        elif kind in _AS_BIGINT:
            casts.append(f"_c{i}::BIGINT as {alias}")
        elif kind == "FLOAT":
            casts.append(f"_c{i}::DOUBLE as {alias}")
        else:
            casts.append(f"_c{i}::VARCHAR as {alias}")
    if not changed:
        return
    names = ", ".join(f"_c{i}" for i in range(len(casts)))
    sql = f"SELECT {', '.join(casts)} FROM ({cursor._last_sql.rstrip().rstrip(';')}) AS _r({names})"
    cursor._duck_conn.execute(sql, cursor._last_params)
    cursor._arrow_table = cursor._duck_conn.to_arrow_table()
    cursor._last_sql = sql


_FLATTEN_CALL = re.compile(r"\bflatten\s*\(", re.IGNORECASE)


def outer_flattens(sql: str) -> list[bool]:
    """For each FLATTEN(...) in order: was it called with OUTER => TRUE?"""
    flags = []
    for m in _FLATTEN_CALL.finditer(sql):
        depth, i = 1, m.end()
        while i < len(sql) and depth:
            depth += {"(": 1, ")": -1}.get(sql[i], 0)
            i += 1
        flags.append(bool(re.search(r"outer\s*=>\s*true", sql[m.end():i], re.IGNORECASE)))
    return flags


def keep_outer_rows(transformed: exp.Expression, flags: list[bool]) -> exp.Expression:
    """FLATTEN(..., OUTER => TRUE) keeps rows that have nothing to expand: a LEFT lateral join."""
    if not any(flags):
        return transformed
    calls = [n for n in transformed.find_all(exp.Anonymous) if n.name.upper() == "_FS_FLATTEN"]
    for call, outer in zip(calls, flags):
        join = call.find_ancestor(exp.Join)
        if outer and join is not None:
            join.set("side", "LEFT")
            join.set("on", exp.true())
    return transformed


def patch_upstream() -> None:
    """Switch off emulator transforms that change answers."""
    import fakesnow.transforms as upstream

    # Casting every SUM/AVG argument to DOUBLE loses exact decimals and the result type.
    upstream.numeric_agg_implicit_cast = lambda e: e

    trim = upstream.trim_cast_varchar

    def trim_cast_varchar(e):  # noqa: ANN001
        # LTRIM/RTRIM/TRIM with explicit characters were collapsed to a plain TRIM.
        if isinstance(e, exp.Trim) and (e.args.get("position") or e.args.get("expression")):
            return e
        return trim(e)

    upstream.trim_cast_varchar = trim_cast_varchar

    # NULL timestamps were sent to the client as 1970-01-01: the struct that carries a
    # timestamp on the wire was built without its null mask.
    import fakesnow.arrow as wire
    import pyarrow as pa
    import pyarrow.compute as pc

    to_struct = wire.timestamp_to_sf_struct

    def timestamp_to_sf_struct(ts):  # noqa: ANN001
        if isinstance(ts, pa.ChunkedArray):
            ts = ts.combine_chunks()
        out = to_struct(ts)
        if ts.null_count == 0:
            return out
        children = [pc.fill_null(out.field(i), 0) for i in range(out.type.num_fields)]
        return pa.StructArray.from_arrays(children, fields=list(out.type), mask=ts.is_null())

    wire.timestamp_to_sf_struct = timestamp_to_sf_struct
