"""Where Snowflake and DuckDB disagree on the *answer*, not the syntax.

The emulator transpiles SQL faithfully enough to run, but several functions then
return different values or types. Each fix here exists because a case in
`conformance/` failed against an answer recorded from real Snowflake.
"""

from __future__ import annotations

from sqlglot import exp

_NS = "_fs_global.main."
_SCALE = r"coalesce(try_cast(nullif(regexp_extract(typeof(a), ',(\d+)\)', 1), '') as integer), 0)"

MACROS = [
    # NUMBER / NUMBER keeps max(s, min(s + 6, 12)) decimals and raises on zero.
    # DuckDB returns a full-precision double, and NULL on zero.
    f"""create or replace macro {_NS}_sf_div(a, b) as case
          when b = 0 then error('Division by zero')
          when typeof(a) in ('DOUBLE', 'FLOAT') or typeof(b) in ('DOUBLE', 'FLOAT') then a / b
          else round(a::double / b, greatest({_SCALE}, least({_SCALE} + 6, 12))) end""",
    # DATE_TRUNC keeps the input type. DuckDB turns a DATE into a TIMESTAMP.
    f"""create or replace macro {_NS}_sf_date_trunc
          (p varchar, x date) as date_trunc(p, x)::date,
          (p varchar, x timestamp) as date_trunc(p, x),
          (p varchar, x timestamptz) as date_trunc(p, x),
          (p varchar, x varchar) as date_trunc(p, x::timestamp)""",
]


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


def _fix(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Div):
        return _fn("_sf_div", node.this, node.expression)
    if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
        return _fn("_sf_date_trunc", exp.Literal.string(node.unit.name.lower()), node.this)
    if isinstance(node, exp.Avg) and not (isinstance(node.parent, exp.Window) and node.parent.this is node):
        return _avg(node)
    if isinstance(node, exp.Window):
        inner = node.this
        if isinstance(inner, exp.Avg):
            return _avg(inner, node)
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
        new = rewrite(child)
        if new is not child:
            child.replace(new)
    return _fix(node)


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
