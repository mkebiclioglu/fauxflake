"""Measure the shape of production tables without moving any rows.

Each table costs a handful of aggregate queries over a trailing window. What
comes back, and what is written to the spec, is limited to:

* daily row counts (fitted to level, weekly seasonality, growth, noise),
* quantiles of load lateness and of numeric columns,
* null rates,
* frequencies of low-cardinality values, and only values shared by at least
  `k_anonymity` rows. High-cardinality strings are never read at all.
"""

from __future__ import annotations

import math
import statistics
from datetime import timedelta
from pathlib import Path

import yaml

DECLARED = {"fk", "lifecycle", "template", "sequence", "hash_id", "event_time", "updated_at", "loaded_at", "constant"}
LATENESS_P = [0, 0.5, 0.9, 0.99, 0.999, 1]
NUMERIC_P = [0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1]
OTHER = "__other__"


def _rows(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    try:
        return cur.execute(sql).fetchall()
    finally:
        cur.close()


def fit_volume(daily: list[tuple]) -> dict:
    """Level, weekly seasonality, growth and noise from (day, rows) pairs."""
    days = [d for d, _ in daily]
    counts = [max(float(n), 1.0) for _, n in daily]
    mean = statistics.fmean(counts)
    weekly = []
    for dow in range(7):
        same = [n for d, n in zip(days, counts) if d.weekday() == dow]
        weekly.append((statistics.fmean(same) / mean) if same else 1.0)
    deseason = [math.log(n / weekly[d.weekday()]) for d, n in zip(days, counts)]
    xs = [(d - days[0]).days for d in days]
    slope = 0.0
    if len(days) >= 56:
        slope, _ = statistics.linear_regression(xs, deseason)
    intercept = statistics.fmean(deseason) - slope * statistics.fmean(xs)
    fitted = [math.exp(intercept + slope * x) * weekly[d.weekday()] for x, d in zip(xs, days)]
    resid = [n / f - 1 for n, f in zip(counts, fitted)]
    return {
        "rows_per_day": round(math.exp(intercept + slope * xs[-1])),
        "growth_per_year": round(math.exp(slope * 365) - 1, 3),
        "weekly": [round(w, 3) for w in weekly],
        "noise": round(statistics.pstdev(resid), 3) if len(resid) > 1 else 0.0,
        "as_of": days[-1].isoformat(),
    }


def _type(data_type: str, precision, scale) -> tuple[str, str]:
    """(kind, DuckDB type) for a Snowflake column."""
    dt = data_type.upper()
    if dt in ("NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
        return ("numeric", f"DECIMAL({precision},{scale})") if scale else ("numeric", "BIGINT")
    if dt in ("FLOAT", "DOUBLE", "REAL", "FLOAT4", "FLOAT8", "DOUBLE PRECISION"):
        return "numeric", "DOUBLE"
    if dt == "BOOLEAN":
        return "category", "BOOLEAN"
    if dt in ("TEXT", "VARCHAR", "STRING", "CHAR", "CHARACTER"):
        return "category", "VARCHAR"
    return "other", dt


def profile_table(conn, name: str, t: dict, *, days: int, sample_pct: float | None,
                  k_anonymity: int, max_categories: int, log=print) -> None:
    db, schema, table = name.upper().split(".")
    cols = {c.upper(): dict(d or {}) for c, d in (t.get("columns") or {}).items()}

    def role(gen: str, hint: str) -> str | None:
        found = next((c for c, d in cols.items() if d.get("gen") == gen), None)
        return found or (t[hint].upper() if t.get(hint) else None)

    ev, loaded = role("event_time", "event_time"), role("loaded_at", "loaded_at")
    changed = next((c for c, d in cols.items() if d.get("gen") == "updated_at"), None)
    key = next((c for c, d in cols.items() if d.get("gen") in ("sequence", "hash_id")), None) or \
        (t["key"].upper() if t.get("key") else None)
    if not (ev and loaded and key):
        raise ValueError(f"{name}: declare key, event_time and loaded_at before profiling")

    sample = f" sample system ({sample_pct})" if sample_pct else ""
    factor = 100.0 / sample_pct if sample_pct else 1.0
    src = f"{db}.{schema}.{table}{sample}"

    (last,) = _rows(conn, f"select max({ev}) from {db}.{schema}.{table}")[0]  # metadata-only on Snowflake
    end = last.date()  # the last day is partial, so the window stops before it
    begin = end - timedelta(days=days)
    window = f"{ev} >= '{begin}' and {ev} < '{end}'"

    daily = _rows(conn, f"select {ev}::date, count(distinct {key}) from {src} where {window} group by 1 order by 1")
    t["volume"] = fit_volume([(d, n * factor) for d, n in daily])

    stamp = changed or ev
    lag = f"datediff('millisecond', {stamp}, {loaded}) / 1000"
    qs = ", ".join(f"approx_percentile({lag}, {p})" for p in LATENESS_P[1:-1])
    row = _rows(conn, f"select min({lag}), {qs}, max({lag}) from {src} where {window}")[0]
    t["lateness"] = {"p": LATENESS_P, "seconds": _monotonic([round(float(x), 3) for x in row])}

    info = _rows(conn, f"""
        select column_name, data_type, numeric_precision, numeric_scale
        from {db}.information_schema.columns
        where table_schema = '{schema}' and table_name = '{table}' order by ordinal_position""")
    out: dict[str, dict] = {}
    for col, data_type, precision, scale in info:
        col = col.upper()
        current = cols.get(col, {})
        if current.get("gen") in DECLARED or current.get("pin"):
            out[col] = current
            continue
        if col in (key, ev, loaded):
            gen = {key: "sequence" if _type(data_type, precision, scale)[0] == "numeric" else "hash_id",
                   ev: "event_time", loaded: "loaded_at"}[col]
            out[col] = {"gen": gen}
            continue
        kind, duck_type = _type(data_type, precision, scale)
        total, nonnull, distinct = _rows(
            conn, f"select count(*), count({col}), approx_count_distinct({col}) from {src} where {window}")[0]
        d: dict = {}
        if kind == "numeric":
            qs = ", ".join(f"approx_percentile({col}, {p})" for p in NUMERIC_P[1:-1])
            row = _rows(conn, f"select min({col}), {qs}, max({col}) from {src} where {window}")[0]
            d = {"gen": "dist", "type": duck_type, "p": NUMERIC_P, "v": _monotonic([float(x) for x in row])}
        elif kind == "category" and distinct <= max_categories:
            groups = _rows(conn, f"""
                select {col}, count(*) from {src} where {window} and {col} is not null
                group by 1 having count(*) >= {int(k_anonymity)} order by 2 desc""")
            values = {v: round(n / nonnull, 4) for v, n in groups}
            rest = round(1 - sum(values.values()), 4)
            if rest > 0.0005:
                values[OTHER] = rest  # rare values are folded together, never listed
            d = {"gen": "categorical", "values": values}
            if duck_type != "VARCHAR":
                d["type"] = duck_type
        elif kind == "category":
            d = {"gen": "template", "template": f"{col.lower()}_{{hex}}"}  # high cardinality: invent, don't read
        else:
            log(f"  {name}.{col}: {data_type} is not profiled yet, generating NULL (declare it by hand)")
            d = {"gen": "constant", "value": None, "type": duck_type}
        if total and nonnull < total:
            d["null_rate"] = round(1 - nonnull / total, 4)
        out[col] = d
    for gone in set(cols) - set(out):
        log(f"  {name}.{gone}: no longer in Snowflake, removed")
    t["columns"] = out
    for hint in ("key", "event_time", "loaded_at"):
        t.pop(hint, None)


def _monotonic(values: list[float]) -> list[float]:
    out = []
    for v in values:
        out.append(max(v, out[-1]) if out else v)
    return out


def profile_spec(path: str | Path, conn, *, days: int = 90, sample_pct: float | None = None,
                 k_anonymity: int = 50, max_categories: int = 50, log=print) -> list[str]:
    raw = yaml.safe_load(Path(path).read_text())
    done = []
    for name, t in raw["tables"].items():
        log(f"profiling {name}")
        profile_table(conn, name, t, days=days, sample_pct=sample_pct,
                      k_anonymity=k_anonymity, max_categories=max_categories, log=log)
        done.append(name)
    Path(path).write_text(
        "# Measured by `fauxflake profile`. Aggregates only; review the diff like code.\n"
        + yaml.safe_dump(raw, sort_keys=False, default_flow_style=None, width=110)
    )
    return done
