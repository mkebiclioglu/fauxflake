"""Measure, rather than assume, that the emulator agrees with Snowflake.

The sources are synthetic, so they are safe to load into a real account. The
same rows go to both engines, the same dbt project builds on both, simulated
days pass on both, and every model is compared row for row.
"""

from __future__ import annotations

import contextlib

import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .dbt import DbtProject
from .generate import Warehouse

TYPES = {"BIGINT": "NUMBER(38,0)", "INTEGER": "NUMBER(38,0)", "HUGEINT": "NUMBER(38,0)", "UBIGINT": "NUMBER(38,0)",
         "SMALLINT": "NUMBER(38,0)", "TINYINT": "NUMBER(38,0)", "DOUBLE": "FLOAT", "FLOAT": "FLOAT", "VARCHAR": "VARCHAR",
         "TIMESTAMP": "TIMESTAMP_NTZ", "TIMESTAMP_NS": "TIMESTAMP_NTZ", "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
         "DATE": "DATE", "TIME": "TIME", "BOOLEAN": "BOOLEAN", "JSON": "VARIANT", "BLOB": "BINARY"}


def _snowflake_type(kind: str) -> str:
    if kind.endswith("[]") or kind.startswith(("STRUCT", "MAP")):
        return "VARIANT"
    return TYPES.get(kind, kind.replace("DECIMAL", "NUMBER"))


def seed(conn, wh: Warehouse, after: datetime | None = None, log=print, workers: int = 8) -> None:
    """Copy the emulator's source tables (or the rows loaded since `after`) into Snowflake. Each table is written
    to a Parquet file here, one after the other (DuckDB is one connection), and loaded into Snowflake by a few
    threads at once (PUT and COPY wait on the network, not on this machine)."""
    from concurrent.futures import ThreadPoolExecutor

    with tempfile.TemporaryDirectory() as tmp:
        work = []
        for t in wh.spec.ordered():
            db, schema, table = t.name.split(".")
            loaded = t.role("loaded_at")
            since = None if t.raw.get("mode") == "upsert" or not loaded else after  # (rows that change in place: replaced)
            where = f"where {loaded} > '{since}'" if since else ""
            path = Path(tmp) / f"{db}.{schema}.{table}.parquet"
            n = wh.cur.execute(f"copy (select * from {t.name} {where}) to '{path}' (format parquet)").fetchone()[0]
            described = [(c, _snowflake_type(ty)) for c, ty, *_ in wh.cur.execute(f"describe {t.name}").fetchall()]
            work.append((t.name, db, schema, table, path, n, described, since is None))
        cur = conn.cursor()
        for db, schema in sorted({(w[1], w[2]) for w in work if w[7]}):
            try:
                cur.execute(f"create database if not exists {db}")
            except Exception:  # noqa: BLE001, S110 - a role that may not create databases: the schema says so if it is missing
                pass
            cur.execute(f"create schema if not exists {db}.{schema}")

        def load(item) -> str:  # noqa: ANN001
            name, db, schema, table, path, n, described, fresh = item
            c = conn.cursor()
            if fresh:
                c.execute(f"create or replace table {name} ({', '.join(f'{chr(34)}{col}{chr(34)} {ty}' for col, ty in described)})")
            if n:
                c.execute(f"put file://{path} @{db}.{schema}.%{table} auto_compress=false overwrite=true")
                # (a document travels as text and is parsed on arrival: loaded by name it would stay a string)
                picked = ", ".join(f'parse_json($1:"{col}"::varchar)' if ty == "VARIANT" else f'$1:"{col}"::{ty}' for col, ty in described)
                c.execute(f"copy into {name} from (select {picked} from @{db}.{schema}.%{table}) "
                          "file_format=(type=parquet use_logical_type=true) purge=true")
            c.close()
            return f"  seeded {name}: {n:,} rows"

        with ThreadPoolExecutor(workers) as pool:
            for line in pool.map(load, work):
                log(line)


def _fetch(conn, sql: str):
    """Query Snowflake into an Arrow table (None when empty), without needing pandas."""
    import pyarrow as pa

    cur = conn.cursor()
    rows = cur.execute(sql).fetchall()
    if not rows:
        return None
    names = [d[0] for d in cur.description]
    return pa.table({n: [r[i] for r in rows] for i, n in enumerate(names)})


@dataclass
class Parity:
    model: str
    emulator_rows: int
    snowflake_rows: int
    only_emulator: int
    only_snowflake: int
    note: str = ""
    undetermined: str = ""  # why the rows are not determined by the data (ties in an ordered window), when they are not
    order_only: bool = False  # the rows are the same once the elements of each array are sorted

    @property
    def ok(self) -> bool:
        return self.only_emulator == 0 and self.only_snowflake == 0 and (
            not self.note or self.note.startswith(("differ in", "both engines fail to read")))


def _comparable(column: str, kind: str, source: str) -> str:
    """One column in a form both engines agree on: Snowflake's values arrive as Python objects
    (pretty-printed JSON text, floats that differ in the last bit)."""
    quoted = f'"{column}"'
    if kind == "JSON":
        return f"{quoted}::varchar as {quoted}"  # canonical text: see _canonical_columns
    if kind in ("DOUBLE", "FLOAT"):
        return f"format('{{:.10g}}', {quoted}::double) as {quoted}"
    return f"{quoted}::{kind} as {quoted}" if source == "theirs" else quoted


def _canonical(text: str | None, unordered: bool = False) -> str | None:
    """JSON as both engines would agree on it: Snowflake stores object keys sorted and prints 1.0.
    `unordered`: arrays with their elements sorted too."""
    import json

    def tidy(v):
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, dict):
            return {k: tidy(x) for k, x in v.items()}
        if isinstance(v, list):
            items = [tidy(x) for x in v]
            return sorted(items, key=lambda x: json.dumps(x, sort_keys=True)) if unordered else items
        return v

    if text is None:
        return None
    try:
        return json.dumps(tidy(json.loads(text)), sort_keys=True, separators=(",", ":"))
    except ValueError:
        return text


def _canonical_columns(table, columns: list[str], unordered: bool = False):
    """The Arrow table with its JSON columns rewritten as canonical text."""
    import pyarrow as pa

    lookup = {name.upper(): name for name in table.column_names}
    for col in columns:
        name = lookup[col]
        values = [_canonical(None if v is None else str(v), unordered) for v in table.column(name).to_pylist()]
        table = table.set_column(table.column_names.index(name), name, pa.array(values, pa.string()))
    return table


def compare(conn, emu, local: DbtProject, schema: str | None = None, remote_schema: str | None = None,
            only: set[str] | None = None, clock: tuple | None = None) -> list[Parity]:
    """Each model on the emulator against the same model on Snowflake, row for row. Without
    `schema`, each model is read from the schema dbt built it in (the same on both sides)."""
    duck, out = emu.cursor(), []
    for node in sorted(local.models(), key=lambda n: n["name"]):
        if only is not None and node["unique_id"] not in only:
            continue
        name = node.get("alias") or node["name"]
        mine = f'{node["database"]}.{schema or node["schema"]}.{name}'
        try:
            theirs = _fetch(conn, f'select * from {node["database"]}.{remote_schema or node["schema"]}.{name}')
        except Exception as e:  # noqa: BLE001 - a view that fails when read (its data): does the emulator's fail too?
            try:
                duck.execute(f"select * from {mine}").fetchall()
            except Exception:  # noqa: BLE001
                out.append(Parity(node["name"], 0, 0, 0, 0, f"both engines fail to read it: {str(e)[:160]}"))
            else:
                out.append(Parity(node["name"], 0, 0, 0, 0, f"Snowflake fails to read it: {type(e).__name__}: {str(e)[:160]}"))
            continue
        try:
            types = {c.upper(): ty for c, ty, *_ in duck.execute(f"describe {mine}").fetchall()}
            note, order_only = "", False
            if theirs is None:  # zero rows
                theirs_n, a, b = 0, duck.execute(f"select count(*) from {mine}").fetchone()[0], 0
            else:
                duck.register("_theirs", theirs)
                # compare in the emulator's types, so NUMBER(38,0) vs BIGINT is not a difference
                missing = set(types) ^ {c.upper() for c in theirs.column_names}
                if missing:
                    out.append(Parity(node["name"], 0, theirs.num_rows, 0, 0, f"columns differ: {sorted(missing)}"))
                    continue
                theirs_n = theirs.num_rows
                source = mine
                documents = [c for c, ty in types.items() if ty == "JSON"]
                if documents:  # compare JSON by content: key order and 1 vs 1.0 are not differences
                    duck.register("_theirs", _canonical_columns(theirs, documents))
                    cols = ", ".join(f'"{c}"::varchar as "{c}"' if c in documents else f'"{c}"' for c in types)
                    duck.register("_mine", _canonical_columns(duck.execute(f"select {cols} from {mine}").to_arrow_table(), documents))
                    source = "_mine"
                both = (f"with t as (select {', '.join(_comparable(c, ty, 'theirs') for c, ty in types.items())} from _theirs), "
                        f"m as (select {', '.join(_comparable(c, ty, 'mine') for c, ty in types.items())} from {source}) ")
                a, b = duck.execute(both + """
                    select (select count(*) from (select * from m except all select * from t)),
                           (select count(*) from (select * from t except all select * from m))""").fetchone()
                note = ""
                if (a or b) and documents:  # the same rows once array elements are sorted: only the order differs
                    plain = duck.execute(f"select {cols} from {mine}").to_arrow_table()
                    duck.register("_theirs", _canonical_columns(theirs, documents, unordered=True))
                    duck.register("_mine", _canonical_columns(plain, documents, unordered=True))
                    a2, b2 = duck.execute(both + """
                        select (select count(*) from (select * from m except all select * from t)),
                               (select count(*) from (select * from t except all select * from m))""").fetchone()
                    duck.register("_theirs", _canonical_columns(theirs, documents))
                    duck.register("_mine", _canonical_columns(plain, documents))
                    if not a2 and not b2:
                        order_only = True
                if a or b:
                    # which columns hold other values, each compared on its own (rows need not line up): one sample each
                    differing, clocks = [], []
                    for c in types:
                        q = f'"{c}"'
                        got = duck.execute(both + f"""
                            select (select any_value({q}::varchar) from (select {q} from m except all select {q} from t)),
                                   (select any_value({q}::varchar) from (select {q} from t except all select {q} from m)),
                                   (select count(*) from (select {q} from m except all select {q} from t))""").fetchone()
                        if got[2] and _is_clock(duck, both, c, types[c], clock):
                            clocks.append(c)
                        elif got[2]:
                            differing.append(f"{c}: emulator {str(got[0])[:60]!r} snowflake {str(got[1])[:60]!r}")
                    note = "differ in " + "; ".join(differing[:6]) if differing else ""
                    if clocks:  # the rows compared without them
                        rest = {c: ty for c, ty in types.items() if c not in clocks}
                        if rest:
                            both = (f"with t as (select {', '.join(_comparable(c, ty, 'theirs') for c, ty in rest.items())} from _theirs), "
                                    f"m as (select {', '.join(_comparable(c, ty, 'mine') for c, ty in rest.items())} from {source}) ")
                            a, b = duck.execute(both + """
                                select (select count(*) from (select * from m except all select * from t)),
                                       (select count(*) from (select * from t except all select * from m))""").fetchone()
                        else:
                            a = b = 0
            mine_n = duck.execute(f"select count(*) from {mine}").fetchone()[0]
            out.append(Parity(node["name"], mine_n, theirs_n, a, b, note if theirs is not None and (a or b) else "", order_only=order_only))
        except Exception as e:  # noqa: BLE001
            out.append(Parity(node["name"], 0, 0, 0, 0, f"{type(e).__name__}: {str(e)[:160]}"))
    return out


def outcomes(project: DbtProject) -> dict[str, tuple[str, str, object]]:
    """What the last dbt invocation did with each node: (status, first line of the message, failures)."""
    import json

    path = project.target_path / "run_results.json"
    if not path.exists():
        return {}
    out = {}
    for r in json.loads(path.read_text()).get("results") or []:
        message = " ".join(str(r.get("message") or "").split())
        out[r["unique_id"]] = (str(r.get("status")), message[:300], r.get("failures"))
    return out


@dataclass
class Difference:
    node: str
    kind: str  # emulator_only_error | snowflake_only_error | test_result | rows | undetermined
    detail: str


def differences(local: dict, remote: dict, rows: list[Parity]) -> list[Difference]:
    """Where the two engines did not do the same: a node that failed on one side only, a test whose result or
    failure count differs, a model whose rows differ."""
    out = []
    for uid in sorted(set(local) | set(remote)):
        mine, theirs = local.get(uid), remote.get(uid)
        if mine is None or theirs is None:
            continue
        broke = lambda o: o[0] in ("error", "runtime error")  # noqa: E731
        if broke(mine) and not broke(theirs):
            out.append(Difference(uid, "emulator_only_error", mine[1]))
        elif broke(theirs) and not broke(mine):
            out.append(Difference(uid, "snowflake_only_error", theirs[1]))
        elif uid.startswith("test.") and not broke(mine) and (mine[0], mine[2]) != (theirs[0], theirs[2]):
            out.append(Difference(uid, "test_result", f"emulator {mine[0]} ({mine[2]}), snowflake {theirs[0]} ({theirs[2]})"))
    for r in rows:
        if not r.ok and r.undetermined:
            out.append(Difference(r.model, "undetermined", r.undetermined))
        elif not r.ok:
            out.append(Difference(r.model, "rows", f"emulator={r.emulator_rows} snowflake={r.snowflake_rows} "
                                                    f"only_emulator={r.only_emulator} only_snowflake={r.only_snowflake} {r.note}"))
    return out


def _ties(conn, sql: str) -> str | None:  # noqa: ANN001
    """A window in the model whose result hangs on the order of rows that tie on its PARTITION BY and ORDER BY
    (ROW_NUMBER, LAG, FIRST_VALUE, a ROWS frame...): on such data any engine may give other rows, each run.
    Counted on Snowflake. None when every such window has distinct keys (or none can be asked about)."""
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except Exception:  # noqa: BLE001
        return None
    sensitive = (exp.RowNumber, exp.FirstValue, exp.LastValue, exp.NthValue, exp.Lag, exp.Lead)
    for win in tree.find_all(exp.Window):
        order, spec = win.args.get("order"), win.args.get("spec")
        if order is None or not (isinstance(win.this, sensitive) or (spec is not None and str(spec.args.get("kind", "")).lower() == "rows")):
            continue
        select = win.find_ancestor(exp.Select)
        if select is None or select.args.get("group") or select.args.get("from_") is None:
            continue
        keys = [p.copy() for p in win.args.get("partition_by") or []] + [o.this.copy() for o in order.expressions]
        probe = exp.Select(expressions=[exp.alias_(k, f"_t{i}") for i, k in enumerate(keys)])
        for arg in ("from_", "joins", "where"):
            if select.args.get(arg) is not None:
                value = select.args[arg]
                probe.set(arg, [j.copy() for j in value] if isinstance(value, list) else value.copy())
        withs, scope = [], select
        while scope is not None:
            if scope.args.get("with_") is not None:
                withs.append(scope.args["with_"])
            scope = scope.parent
        ctes = [c.sql(dialect="snowflake") for w in reversed(withs) for c in w.expressions]
        names = ", ".join(f"_t{i}" for i in range(len(keys)))
        query = (("with " + ", ".join(ctes) + " ") if ctes else "") + \
            f"select count(*) from (select 1 from ({probe.sql(dialect='snowflake')}) group by {names} having count(*) > 1)"
        try:
            (tied,) = conn.cursor().execute(query).fetchone()
        except Exception:  # noqa: BLE001, S112 - a window that only binds in its place: not asked
            continue
        if tied:
            return f"{tied} tied group(s) in {' '.join(win.sql(dialect='snowflake').split())[:160]}"
    return None


def _unordered_aggregate(sql: str) -> str | None:
    """ARRAY_AGG / ARRAY_UNIQUE_AGG / LISTAGG without WITHIN GROUP (ORDER BY ...): the order of what it gathers is not defined."""
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except Exception:  # noqa: BLE001
        return None
    for agg in tree.find_all(exp.ArrayAgg, exp.ArrayUniqueAgg, exp.GroupConcat):
        if not isinstance(agg.parent, exp.WithinGroup) and not agg.find(exp.Order):
            return {exp.ArrayAgg: "ARRAY_AGG", exp.ArrayUniqueAgg: "ARRAY_UNIQUE_AGG"}.get(type(agg), "LISTAGG") + " without an order"
    return None


def undetermined(conn, remote: DbtProject, rows: list[Parity]) -> None:  # noqa: ANN001
    """Mark each differing model whose rows the data does not determine, or that is built on one that is."""
    nodes = {n["name"]: n for n in remote.models()}
    by_id = {n["unique_id"]: n for n in nodes.values()}
    found: dict[str, str] = {}
    for r in rows:
        if not r.ok and r.model in nodes:
            why = _ties(conn, nodes[r.model].get("compiled_code") or "")
            if why:
                found[nodes[r.model]["unique_id"]] = why
    unordered = {uid: why for uid, n in by_id.items() if (why := _unordered_aggregate(n.get("compiled_code") or ""))}

    def unordered_upstream(uid: str, seen: set[str]) -> str | None:
        if uid in unordered:
            return uid
        for parent in by_id.get(uid, {}).get("depends_on", {}).get("nodes", []):
            if parent not in seen:
                seen.add(parent)
                if (hit := unordered_upstream(parent, seen)) is not None:
                    return hit
        return None

    def upstream(uid: str, seen: set[str]) -> str | None:
        for parent in by_id.get(uid, {}).get("depends_on", {}).get("nodes", []):
            if parent in seen:
                continue
            seen.add(parent)
            if parent in found:
                return parent
            hit = upstream(parent, seen)
            if hit:
                return hit
        return None

    for r in rows:
        if r.ok or r.model not in nodes:
            continue
        uid = nodes[r.model]["unique_id"]
        if r.order_only and (source := unordered_upstream(uid, set())) is not None:
            r.undetermined = (f"the same rows with array elements in another order; {by_id[source]['name']} uses "
                              f"{unordered[source]}, which gives no order")
        elif uid in found:
            r.undetermined = "the order of rows is not determined: " + found[uid]
        elif (parent := upstream(uid, set())) is not None:
            r.undetermined = f"built on {by_id[parent]['name']}, whose order of rows is not determined"


@contextlib.contextmanager
def remote_session():
    """dbt on Snowflake as the emulator runs: every model on the connection's warehouse (a model's
    `snowflake_warehouse` names one of its own account), and the clock in UTC (the emulator's zone)."""
    import snowflake.connector
    from dbt.adapters.snowflake.impl import SnowflakeAdapter

    hook, connect = SnowflakeAdapter.pre_model_hook, snowflake.connector.connect

    def in_utc(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs["session_parameters"] = {**(kwargs.get("session_parameters") or {}), "TIMEZONE": "UTC"}
        return connect(*args, **kwargs)

    SnowflakeAdapter.pre_model_hook = lambda self, config: None
    snowflake.connector.connect = in_utc
    try:
        yield
    finally:
        SnowflakeAdapter.pre_model_hook, snowflake.connector.connect = hook, connect


def _instant(v):  # noqa: ANN001, ANN202
    """A timestamp or date as a naive UTC datetime."""
    from datetime import date, timezone

    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    return None


def _is_clock(duck, both: str, column: str, kind: str, clock) -> bool:  # noqa: ANN001
    """The column holds when the build ran: the emulator's frozen clock on its side, a time within the
    Snowflake build on the other (CURRENT_TIMESTAMP() and the like differ by the hour of the build, not by engine)."""
    if clock is None or not kind.startswith(("TIMESTAMP", "DATE")):
        return False
    frozen, start, end = clock
    q = f'"{column}"'
    lo_m, hi_m, lo_t, hi_t = (_instant(v) for v in duck.execute(
        both + f"select (select min({q}) from m), (select max({q}) from m), (select min({q}) from t), (select max({q}) from t)").fetchone())
    if None in (lo_m, hi_m, lo_t, hi_t):
        return False
    slack = timedelta(seconds=2)
    if kind == "DATE":
        days = {frozen.date(), start.date(), end.date()}
        return lo_m.date() == hi_m.date() == frozen.date() and lo_t.date() in days and hi_t.date() in days
    return frozen - slack <= lo_m <= hi_m <= frozen + slack and start - slack <= lo_t <= hi_t <= end + slack


def run(conn, wh: Warehouse, local: DbtProject, remote: DbtProject, start: datetime, days: int,
        remote_schema: str = "FAUXFLAKE_PARITY", log=print, select: list[str] | None = None) -> tuple[list[Parity], list[Difference]]:
    """The same sources on both engines, the same project built on both (every node, also after one fails), then
    what each did compared: which nodes failed on one side only, which tests answered differently, which models hold
    other rows. Day by day for incremental models."""
    import os

    found: list[Difference] = []
    rows: list[Parity] = []
    for i in range(days + 1):
        now = start + timedelta(days=i)
        prev = wh.now
        wh.advance_to(now)
        log(f"{now:%Y-%m-%d}  seeding Snowflake")
        seed(conn, wh, after=prev, log=log)
        full = ["--full-refresh"] if i == 0 else []
        os.environ["FAUXFLAKE_TESTS_DO_NOT_BLOCK"] = "1"  # (a failing test keeps nothing else from being built and compared)
        try:
            # the same schema name on both sides: some models embed their own relation names
            chosen = ["--select", *select] if select else []
            local.invoke(["build", *full, *chosen], schema=remote_schema, quiet=True)
            mine = outcomes(local)
            began = conn.cursor().execute("select sysdate()").fetchone()[0]
            with remote_session():
                remote.invoke(["build", *full, *chosen], schema=remote_schema, quiet=True)
            ended = conn.cursor().execute("select sysdate()").fetchone()[0]
            theirs = outcomes(remote)
        finally:
            os.environ.pop("FAUXFLAKE_TESTS_DO_NOT_BLOCK", None)
        log(f"{now:%Y-%m-%d}  built on both engines")
        built = {uid for uid in mine if mine[uid][0] == "success" and theirs.get(uid, ("",))[0] == "success"}
        from . import emulator

        clock = (emulator.FROZEN_NOW, began, ended) if emulator.FROZEN_NOW else None
        rows = compare(conn, wh.emu, local, only=built, clock=clock)
        undetermined(conn, remote, rows)  # (each model where dbt put it: the same schema on both sides)
        found = differences(mine, theirs, rows)
    return rows, found
