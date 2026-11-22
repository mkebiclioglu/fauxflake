"""Measure, rather than assume, that the emulator agrees with Snowflake.

The sources are synthetic, so they are safe to load into a real account. The
same rows go to both engines, the same dbt project builds on both, simulated
days pass on both, and every model is compared row for row.
"""

from __future__ import annotations

import contextlib
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import account
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


def _ident(name: str) -> str:
    """A part of a Snowflake name, quoted as it is stored (upper case)."""
    return '"' + str(name).upper().replace('"', '""') + '"'


def seed(conn, wh: Warehouse, after: datetime | None = None, log=print, workers: int = 8, renamed: dict | None = None) -> None:
    """Copy the emulator's source tables (or the rows loaded since `after`) into Snowflake. Each table is written
    to a Parquet file here, one after the other (DuckDB is one connection), and loaded into Snowflake by a few
    threads at once (PUT and COPY wait on the network, not on this machine)."""
    from concurrent.futures import ThreadPoolExecutor

    with tempfile.TemporaryDirectory() as tmp:
        work = []
        for t in wh.spec.ordered():
            db, schema, table = t.name.split(".")
            # (a database that cannot be written there, SNOWFLAKE's own say, goes in under another name)
            db = (renamed or {}).get(db.upper(), db)
            loaded = t.role("loaded_at")
            since = None if t.raw.get("mode") == "upsert" or not loaded else after  # (rows that change in place: replaced)
            where = f"where {loaded} > '{since}'" if since else ""
            path = Path(tmp) / f"{db}.{schema}.{table}.parquet"
            target = ".".join(_ident(part) for part in (db, schema, table))  # (a part named ORDER or GROUP: quoted)
            n = wh.cur.execute(f"copy (select * from {t.name} {where}) to '{path}' (format parquet)").fetchone()[0]
            described = [(c, _snowflake_type(ty)) for c, ty, *_ in wh.cur.execute(f"describe {t.name}").fetchall()]
            work.append((target, db, schema, table, path, n, described, since is None))
        cur = conn.cursor()
        for db, schema in sorted({(w[1], w[2]) for w in work if w[7]}):
            try:
                cur.execute(f"create database if not exists {_ident(db)}")
            except Exception:  # noqa: BLE001, S110 - a role that may not create databases: the schema says so if it is missing
                pass
            cur.execute(f"create schema if not exists {_ident(db)}.{_ident(schema)}")

        def load(item) -> str:  # noqa: ANN001
            name, db, schema, table, path, n, described, fresh = item
            c = conn.cursor()
            stage = f"@{_ident(db)}.{_ident(schema)}.%{_ident(table)}"
            if fresh:
                c.execute(f"create or replace table {name} ({', '.join(f'{chr(34)}{col}{chr(34)} {ty}' for col, ty in described)})")
            if n:
                c.execute(f"put 'file://{path}' {stage} auto_compress=false overwrite=true")
                # (a document travels as text and is parsed on arrival: loaded by name it would stay a string)
                picked = ", ".join(f'parse_json($1:"{col}"::varchar)' if ty == "VARIANT" else f'$1:"{col}"::{ty}' for col, ty in described)
                c.execute(f"copy into {name} from (select {picked} from {stage}) "
                          "file_format=(type=parquet use_logical_type=true) purge=true")
            c.close()
            return f"  seeded {name}: {n:,} rows"

        with ThreadPoolExecutor(workers) as pool:
            for line in pool.map(load, work):
                log(line)


def _fetch_into(conn, sql: str, duck, types: dict[str, str]) -> tuple[list[str], int]:  # noqa: ANN001
    """Query Snowflake into the DuckDB table `_theirs`, a batch at a time (a spine of millions of rows does not
    fit in memory as Python values; DuckDB spills to disk). The columns take the emulator's types where it has
    the column (documents as text). Returns Snowflake's column names and the number of rows."""
    import pyarrow as pa

    cur = conn.cursor()
    cur.execute(sql)
    names = [d[0] for d in cur.description]
    query = sql
    if any(d[1] in (6, 7) for d in cur.description):
        # TIMESTAMP_LTZ and TIMESTAMP_TZ come as instants; the emulator keeps a timestamp's wall clock,
        # which is what Snowflake gives when it is cast to TIMESTAMP_NTZ (a TZ in its own offset, an LTZ in the session's)
        exact = lambda n: '"' + n.replace('"', '""') + '"'  # noqa: E731 - as Snowflake named it
        query = "select " + ", ".join(exact(n) + ("::timestamp_ntz " + exact(n) if d[1] in (6, 7) else "")
                                      for n, d in zip(names, cur.description)) + f" from ({sql})"
        cur.execute(query)
    kinds = {n: ("VARCHAR" if types.get(n.upper()) == "JSON" else types.get(n.upper(), "VARCHAR")) for n in names}

    def chunks():  # noqa: ANN202 - rows as Python values, a chunk at a time
        while rows := cur.fetchmany(100_000):
            yield pa.table({n: [None if r[i] is None else r[i] for r in rows] for i, n in enumerate(names)})

    def load(batches) -> int:  # noqa: ANN001
        duck.execute("drop table if exists _theirs")
        duck.execute("create temp table _theirs (" + ", ".join(f'"{n}" {kinds[n]}' for n in names) + ")")
        count = 0
        for batch in batches:
            duck.register("_batch", batch)
            duck.execute("insert into _theirs by name select * from _batch")
            duck.unregister("_batch")
            count += batch.num_rows
        return count

    described = list(cur.description)
    try:
        return names, load(cur.fetch_arrow_batches())
    except pa.ArrowInvalid:
        # the chunks disagree on a column's Arrow type: a timestamp of the year 1 or 8888 (a data vault's ghost records)
        # does not fit nanoseconds, and its chunk comes in another unit. Read again with the times as text (DuckDB reads
        # them back into the column's type): still Arrow, where millions of rows as Python values would not fit in memory
        exact = lambda n: '"' + n.replace('"', '""') + '"'  # noqa: E731
        cur.execute("select " + ", ".join(
            f"to_varchar({exact(d[0])}, 'YYYY-MM-DD HH24:MI:SS.FF9') {exact(d[0])}" if d[1] in (3, 4, 6, 7, 8) and d[1] != 3
            else f"to_varchar({exact(d[0])}, 'YYYY-MM-DD') {exact(d[0])}" if d[1] == 3 else exact(d[0]) for d in described)
                    + f" from ({query})")
        return names, load(cur.fetch_arrow_batches())
    except Exception:  # noqa: BLE001 - a result that does not come as Arrow: rows, a chunk at a time
        cur.execute(query)
        return names, load(chunks())


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


def _comparable(column: str, kind: str, source: str, unordered: bool = False) -> str:
    """One column in a form both engines agree on: Snowflake's values arrive as Python objects
    (pretty-printed JSON text, floats that differ in the last bit)."""
    quoted = f'"{column}"'
    if kind == "JSON":  # canonical text (see _canonical), on both sides
        return f"{'_fx_canon_u' if unordered else '_fx_canon'}({quoted}::varchar) as {quoted}"
    if kind in ("DOUBLE", "FLOAT"):
        return f"format('{{:.10g}}', {quoted}::double) as {quoted}"
    if unordered and kind == "VARCHAR":  # text holding a list (`to_json(array_agg(...))`): its elements sorted too
        return f"case when {quoted} like '[%' then _fx_canon_u({quoted}) else {quoted} end as {quoted}"
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
    import re

    from .semantics import UNDEFINED

    # (an array's `undefined` element: Snowflake writes it bare, the emulator holds it as a marked string)
    text = re.sub(r"(?<=[\[,])(\s*)undefined(?=\s*[,\]])", lambda m: m.group(1) + f'"{UNDEFINED}"', text)
    try:
        return json.dumps(tidy(json.loads(text)), sort_keys=True, separators=(",", ":"))
    except ValueError:
        return text


_CANON: dict[str, object] = {}  # the callables behind _fx_canon and _fx_canon_u, alive as long as DuckDB may call them


def _canonical_functions(duck) -> None:  # noqa: ANN001
    """`_fx_canon(text)` and `_fx_canon_u(text)` (arrays sorted too) in DuckDB, a batch of values at a time. Registered on
    the emulator's own connection (`duck`), which outlives the cursors that read: DuckDB keeps a function for the life
    of the database, and one registered from a cursor calls freed memory once that cursor is gone (a TypeError from
    some other function, or a crash, on the second day's comparison)."""
    import pyarrow as pa

    def made(unordered: bool):  # noqa: ANN202 - (DuckDB wants a function of exactly one argument)
        return lambda values: pa.array([_canonical(v, unordered) for v in values.to_pylist()], pa.string())

    for name, unordered in (("_fx_canon", False), ("_fx_canon_u", True)):
        if duck.execute("select count(*) from duckdb_functions() where function_name = ?", [name]).fetchall()[0][0]:
            continue
        _CANON[name] = made(unordered)
        duck.create_function(name, _CANON[name], ["VARCHAR"], "VARCHAR", type="arrow")


def _through_policies(emu, duck, database: str, schema: str, name: str, mine: str) -> str:  # noqa: ANN001
    """The model as the session's role reads it: through the masking and row access policies set on it (the emulator
    applies them to what a client reads; this reads DuckDB's tables)."""
    from . import objects

    if not hasattr(emu, "_fs"):
        return mine
    state = objects.state_of(emu._fs)
    fq = (database.upper(), schema.upper(), name.upper())
    masks, rows = state.masks.get(fq, {}), state.row_policies.get(fq, [])
    if not masks and not rows:
        return mine
    quote = lambda c: '"' + c.replace('"', '""') + '"'  # noqa: E731
    call = lambda fn, cols: f"_fs_global.main._udf_{fn}({', '.join(quote(c) for c in cols)})"  # noqa: E731
    replaced = ", ".join(f"{call(fn, [col, *extra])} as {quote(col)}" for col, (fn, extra) in masks.items())
    where = " and ".join(call(fn, cols) for fn, cols in rows)
    duck.execute(f"create or replace temp view _fx_read_as_role as select *{' replace (' + replaced + ')' if replaced else ''} "
                 f"from {mine}{' where ' + where if where else ''}")
    return "_fx_read_as_role"


def compare(conn, emu, local: DbtProject, schema: str | None = None, remote_schema: str | None = None,
            only: set[str] | None = None, clock: tuple | None = None) -> list[Parity]:
    """Each model on the emulator against the same model on Snowflake, row for row. Without
    `schema`, each model is read from the schema dbt built it in (the same on both sides)."""
    duck, out = emu.cursor(), []
    _canonical_functions(emu.duck)

    def pair(kinds: dict[str, str], unordered: bool = False) -> str:  # (the two sides, as CTEs t and m)
        return (f"with t as (select {', '.join(_comparable(c, ty, 'theirs', unordered) for c, ty in kinds.items())} from _theirs), "
                f"m as (select {', '.join(_comparable(c, ty, 'mine', unordered) for c, ty in kinds.items())} from {mine}) ")

    counted = "select (select count(*) from (select * from m except all select * from t)), " \
              "(select count(*) from (select * from t except all select * from m))"
    for node in sorted(local.models(), key=lambda n: n["name"]):
        if only is not None and node["unique_id"] not in only:
            continue
        name = node.get("alias") or node["name"]
        mine = f'{node["database"]}.{schema or node["schema"]}.{name}'
        mine = _through_policies(emu, duck, node["database"], schema or node["schema"], name, mine)
        try:
            types = {c.upper(): ty for c, ty, *_ in duck.execute(f"describe {mine}").fetchall()}
        except Exception:  # noqa: BLE001 - the emulator cannot read it (a view over something that fails)
            types = {}
        try:
            names, theirs_n = _fetch_into(conn, f'select * from {node["database"]}.{remote_schema or node["schema"]}.{name}', duck, types)
            # (a view is evaluated when it is read: a CURRENT_TIMESTAMP() in it says when this read ran, after the build)
            read_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=30)
            seen = (clock[0], clock[1], max(clock[2], read_at), *clock[3:]) if clock else None
        except Exception as e:  # noqa: BLE001 - a view that fails when read (its data): does the emulator's fail too?
            if _unavailable(e):
                raise SnowflakeUnavailable(str(e)[:300], out) from e
            try:
                read = duck.execute(f"select * from {mine}")  # (every column: a count would not evaluate them)
                while read.fetchmany(50_000):
                    pass
            except Exception:  # noqa: BLE001
                out.append(Parity(node["name"], 0, 0, 0, 0, f"both engines fail to read it: {str(e)[:160]}"))
            else:
                out.append(Parity(node["name"], 0, 0, 0, 0, f"Snowflake fails to read it: {type(e).__name__}: {str(e)[:160]}"))
            continue
        try:
            if not types:
                types = {c.upper(): ty for c, ty, *_ in duck.execute(f"describe {mine}").fetchall()}
            note, order_only = "", False
            if not theirs_n:  # zero rows
                a, b = duck.execute(f"select count(*) from {mine}").fetchone()[0], 0
            else:
                # compare in the emulator's types, so NUMBER(38,0) vs BIGINT is not a difference
                missing = set(types) ^ {c.upper() for c in names}
                if missing:
                    out.append(Parity(node["name"], 0, theirs_n, 0, 0, f"columns differ: {sorted(missing)}"))
                    continue
                # (JSON is compared by content: key order and 1 vs 1.0 are not differences)
                documents = [c for c, ty in types.items() if ty == "JSON"]
                both = pair(types)
                a, b = duck.execute(both + counted).fetchone()
                note = ""
                if (a or b) and (documents or 'VARCHAR' in types.values()):  # the same rows once array elements are sorted
                    a2, b2 = duck.execute(pair(types, unordered=True) + counted).fetchone()
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
                        if got[2] and _is_clock(duck, both, c, types[c], seen):
                            clocks.append(c)
                        elif got[2]:
                            differing.append(f"{c}: emulator {str(got[0])[:60]!r} snowflake {str(got[1])[:60]!r}")
                    note = "differ in " + "; ".join(differing[:6]) if differing else ""
                    if clocks:  # the rows compared without them
                        rest = {c: ty for c, ty in types.items() if c not in clocks}
                        a, b = duck.execute(pair(rest) + counted).fetchone() if rest else (0, 0)
            mine_n = duck.execute(f"select count(*) from {mine}").fetchone()[0]
            out.append(Parity(node["name"], mine_n, theirs_n, a, b, note if theirs_n and (a or b) else "", order_only=order_only))
        except Exception as e:  # noqa: BLE001
            out.append(Parity(node["name"], 0, 0, 0, 0, f"{type(e).__name__}: {str(e)[:160]}"))
    return out


@contextlib.contextmanager
def _environment(values: dict | None):
    """Environment variables set for the Snowflake side only (a source the project points elsewhere by one)."""
    import os

    saved = {k: os.environ.get(k) for k in values or {}}
    os.environ.update(values or {})
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _databases_built_in(conn, remote: DbtProject, schema: str, extra: list[str]) -> None:  # noqa: ANN001
    """On Snowflake the databases a project builds in must exist before it runs (`PREP`, named by an env var): the
    ones the parsed project names, created where they are missing."""
    import json
    import re

    remote.invoke(["parse", *extra], schema=schema, quiet=True)
    path = remote.target_path / "manifest.json"
    if not path.exists():
        return
    nodes = json.loads(path.read_text())["nodes"].values()
    names = {str(n.get("database") or "").strip('"').upper() for n in nodes if n.get("resource_type") in ("model", "seed", "snapshot")}
    # an incremental model that says `full_refresh: false` keeps what an earlier run left, --full-refresh or not:
    # it starts from nothing here, as on the emulator. So does a snapshot (--full-refresh does not rebuild one: an
    # earlier run's rows, and their times, would stay)
    for n in nodes:
        config = n.get("config") or {}
        kept = n.get("resource_type") == "snapshot" or (
            n.get("resource_type") == "model" and config.get("materialized") == "incremental" and config.get("full_refresh") is False)
        if kept and n.get("relation_name"):
            try:
                conn.cursor().execute(f"drop table if exists {n['relation_name']}")
            except Exception:  # noqa: BLE001, S110 - not there, or not a table
                pass
    for name in sorted(n for n in names if re.fullmatch(r"[A-Z_][\w$]*", n) and n != "SNOWFLAKE"):
        try:
            conn.cursor().execute(f"create database if not exists {name}")
        except Exception:  # noqa: BLE001, S110 - a role that may not create databases: the build says so if one is missing
            pass


def _existing_on_snowflake(conn, wh: Warehouse, renamed: dict | None, log=print, local: DbtProject | None = None) -> None:  # noqa: ANN001
    """The spec's `existing:` relations, which the emulator starts with (empty), made on Snowflake too where they are
    missing: both builds start from the same warehouse (a model that reads one by name finds it on both). Each is of the
    kind the model that builds it makes (a table for a table): dbt then replaces it in one statement, where a view in a
    table's place is dropped first, and a model reading it by name at that moment finds nothing."""
    from .generate import existing_view

    tables = set()
    try:
        nodes = json.loads((local.target_path / "manifest.json").read_text())["nodes"] if local is not None else {}
        tables = {str(n.get("relation_name") or "").replace('"', "").upper() for n in nodes.values()
                  if (n.get("config") or {}).get("materialized") in ("table", "incremental", "snapshot", "seed")
                  or n.get("resource_type") in ("snapshot", "seed")}
    except (OSError, ValueError, KeyError):
        pass
    cur, made = conn.cursor(), 0
    for name, rel in wh.spec.existing.items():
        db, schema, table = name.split(".")
        db = (renamed or {}).get(db, db)
        try:
            cur.execute(f"create schema if not exists {_ident(db)}.{_ident(schema)}")
            cur.execute(existing_view(f"{db}.{schema}.{table}", rel.get("columns") or {},
                                      table=rel.get("as") == "table" or name.upper() in tables))
            made += 1
        except Exception as e:  # noqa: BLE001 - one Snowflake will not make is left to the build, as before
            if getattr(e, "errno", None) in (1998, 2002):
                continue  # (there already, as a table where the stand-in is a view or the other way round)
            log(f"  could not make {name} on Snowflake ({str(e).splitlines()[0][:120]})")
    if made:
        log(f"  {made} relation(s) the project reads by name made on Snowflake where missing, as the emulator has them")


def _invocation(project: DbtProject) -> str | None:
    """The invocation_id of the last dbt command run in the project."""
    import json

    path = project.target_path / "run_results.json"
    return json.loads(path.read_text()).get("metadata", {}).get("invocation_id") if path.exists() else None


def outcomes(project: DbtProject) -> dict[str, tuple[str, str, object]]:
    """What the last dbt invocation did with each node: (status, first line of the message, failures)."""
    import json

    path = project.target_path / "run_results.json"
    if not path.exists():
        return {}
    out = {}
    for r in json.loads(path.read_text()).get("results") or []:
        message = " ".join(str(r.get("message") or "").split())
        # (the end too, where Snowflake names the parameter whose value it refused)
        short = message if len(message) <= 300 else message[:240] + " ... " + message[-60:]
        out[r["unique_id"]] = (str(r.get("status")), short, r.get("failures"))
    return out


@dataclass
class Difference:
    node: str
    kind: str  # emulator_only_error | snowflake_only_error | test_result | rows | undetermined | snowflake_unavailable
    detail: str


class SnowflakeUnavailable(Exception):  # noqa: N818
    """Snowflake stopped answering during the comparison (a resource monitor's quota is spent, say): the models
    not yet read are not compared, rather than each counted as a difference. `rows`: those compared before."""

    def __init__(self, message: str, rows: list | None = None) -> None:
        super().__init__(message)
        self.rows = rows or []


def _unavailable(e: Exception) -> bool:
    """090073: the warehouse cannot be resumed (its resource monitor's quota is spent)."""
    return getattr(e, "errno", None) == 90073 or "resource monitor" in str(e)


def differences(local: dict, remote: dict, rows: list[Parity], depends: dict | None = None) -> list[Difference]:
    """Where the two engines did not do the same: a node that failed on one side only, a test whose result or
    failure count differs, a model whose rows differ. `depends`: each node's parents (the manifest's), so that a node
    skipped on one side only for a parent whose failure the engines do not decide is not counted as a difference."""
    out = []
    broke = lambda o: o[0] in ("error", "runtime error")  # noqa: E731
    tagged = lambda o: broke(o) and "for parameter 'QUERY_TAG'" in o[1]  # noqa: E731
    undecided = {uid for uid in set(local) & set(remote)
                 if (tagged(local[uid]) and not broke(remote[uid])) or (tagged(remote[uid]) and not broke(local[uid]))}

    def after_undecided(uid: str, seen: set[str]) -> str | None:
        for parent in (depends or {}).get(uid, ()):
            if parent in undecided:
                return parent
            if parent not in seen:
                seen.add(parent)
                if (hit := after_undecided(parent, seen)) is not None:
                    return hit
        return None

    for uid in sorted(set(local) | set(remote)):
        mine, theirs = local.get(uid), remote.get(uid)
        if mine is None or theirs is None:
            continue
        skipped_once = (mine[0] == "skipped") != (theirs[0] == "skipped")
        if uid not in undecided and skipped_once and (parent := after_undecided(uid, set())) is not None:
            out.append(Difference(uid, "undetermined", f"skipped on one side only, after {parent.split('.')[-1]} failed there on a "
                                                       "query tag that grew past 2000 characters (the order dbt's threads run in)"))
            continue
        if uid in undecided:
            # a query tag that takes in the session's last one grows with each node a dbt thread runs: which node passes
            # Snowflake's 2000 characters first goes by the order the threads take them in, not by the engine
            out.append(Difference(uid, "undetermined", "fails on one side only on a query tag that grew past 2000 characters: "
                                                       "which node that is goes by the order dbt's threads run them in"))
        elif broke(mine) and not broke(theirs):
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
        # (a key may be an alias of the same select, `partition by question_id` with `... as question_id`, and so may a
        # name in WHERE: asked again with the expression it names. A name that is also a column of the FROM is the
        # column, as Snowflake reads it)
        named = {e.alias.upper(): e.this for e in select.expressions if isinstance(e, exp.Alias) and not e.this.find(exp.Window)}
        withs, scope = [], select
        while scope is not None:
            if scope.args.get("with_") is not None:
                withs.append(scope.args["with_"])
            scope = scope.parent
        ctes = [c.sql(dialect="snowflake") for w in reversed(withs) for c in w.expressions]
        prefix = ("with " + ", ".join(ctes) + " ") if ctes else ""

        def probe_of(expressions: list, where: exp.Expression | None) -> exp.Select:
            probe = exp.Select(expressions=expressions)
            for arg in ("from_", "joins"):
                if select.args.get(arg) is not None:
                    value = select.args[arg]
                    probe.set(arg, [j.copy() for j in value] if isinstance(value, list) else value.copy())
            if where is not None:
                probe.set("where", where)
            return probe

        where = select.args.get("where")
        used = {c.name.upper() for k in [*keys, *([where] if where else [])] for c in k.find_all(exp.Column) if not c.table} & set(named)
        columns = set()
        for name in used:
            try:
                conn.cursor().execute(prefix + probe_of([exp.column(name)], None).sql(dialect="snowflake") + " limit 0")
                columns.add(name)
            except Exception:  # noqa: BLE001, S112 - an alias only
                continue

        def spelled(key: exp.Expression) -> exp.Expression:
            for _ in range(3):  # (an alias built from another)
                key = key.transform(lambda n: named[n.name.upper()].copy() if isinstance(n, exp.Column) and not n.table
                                    and n.name.upper() in named and n.name.upper() not in columns else n)
            return key

        names = ", ".join(f"_t{i}" for i in range(len(keys)))
        tied = None
        for attempt, filtered in ((keys, where), ([spelled(k.copy()) for k in keys], spelled(where.copy()) if where else None)):
            probe = probe_of([exp.alias_(k, f"_t{i}") for i, k in enumerate(attempt)], filtered.copy() if filtered else None)
            query = prefix + f"select count(*) from (select 1 from ({probe.sql(dialect='snowflake')}) group by {names} having count(*) > 1)"
            try:
                (tied,) = conn.cursor().execute(query).fetchone()
                break
            except Exception as e:  # noqa: BLE001 - a window that only binds in its place: not asked
                if _unavailable(e):
                    raise SnowflakeUnavailable(str(e)[:300]) from e
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


def _unordered_lists(sql: str) -> set[str]:
    """The output columns a model writes with LISTAGG without an order (also as a window): their values come in any order."""
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except Exception:  # noqa: BLE001
        return set()
    aliases = list(tree.find_all(exp.Alias))
    found = {a.alias.upper() for a in aliases
             if any(not isinstance(g.parent, exp.WithinGroup) and not g.find(exp.Order) for g in a.this.find_all(exp.GroupConcat))}
    while True:  # (and what a later select makes of such a column: `max(open_invoices) as list_of_open_invoices`)
        more = {a.alias.upper() for a in aliases if a.alias.upper() not in found
                and any(c.name.upper() in found for c in a.this.find_all(exp.Column))}
        if not more:
            return found
        found |= more


def _any_values(sql: str) -> set[str]:
    """The output columns a model gives ANY_VALUE(...) of: any value of each group, not one the data decides."""
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except Exception:  # noqa: BLE001
        return set()
    return {a.alias.upper() for a in tree.find_all(exp.Alias) if isinstance(a.this, exp.AnyValue)}


def undetermined(conn, remote: DbtProject, rows: list[Parity]) -> None:  # noqa: ANN001
    """Mark each differing model whose rows the data does not determine, or that is built on one that is."""
    import json

    nodes = {n["name"]: n for n in remote.models()}
    # (the graph has the ephemeral models too: they are not compared, but what is built on them goes through them)
    by_id = {uid: n for uid, n in json.loads((remote.target_path / "manifest.json").read_text())["nodes"].items()
             if n.get("resource_type") in ("model", "snapshot", "seed")}
    found: dict[str, str] = {}
    ephemeral = {uid for uid, n in by_id.items() if n.get("config", {}).get("materialized") == "ephemeral"}
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
            if parent in ephemeral and parent not in found and (why := _ties(conn, by_id[parent].get("compiled_code") or "")):
                found[parent] = why  # (an ephemeral model is never compared: its ties are looked for when something is built on it)
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
        elif (named := re.findall(r"(?:^differ in |; )([^:;]+): emulator", r.note)) and len(named) < 6 \
                and {c.upper() for c in named} <= _any_values(nodes[r.model].get("compiled_code") or ""):
            r.undetermined = f"differs only in ANY_VALUE() of a group with several values ({', '.join(named)}): any of them is right"
        elif named and len(named) < 6 and {c.upper() for c in named} <= _unordered_lists(nodes[r.model].get("compiled_code") or ""):
            r.undetermined = f"differs only in LISTAGG without an order ({', '.join(named)}): its values come in any order"
        elif (parent := upstream(uid, set())) is not None:
            r.undetermined = f"built on {by_id[parent]['name']}, whose order of rows is not determined"


@contextlib.contextmanager
def remote_session(renamed: dict | None = None):
    """dbt on Snowflake as the emulator runs: every model on the connection's warehouse (a model's
    `snowflake_warehouse` names one of its own account), and the clock in UTC (the emulator's zone). `renamed`:
    sources in these databases are read from the ones their data was loaded into (SNOWFLAKE's own cannot be written)."""
    import snowflake.connector
    from dbt.adapters.snowflake.impl import SnowflakeAdapter
    from dbt.context.providers import RuntimeSourceResolver

    hook, connect, resolve = SnowflakeAdapter.pre_model_hook, snowflake.connector.connect, RuntimeSourceResolver.resolve

    def moved(self, source_name: str, table_name: str):  # noqa: ANN001, ANN202
        relation = resolve(self, source_name, table_name)
        target = (renamed or {}).get(str(relation.database or "").strip('"').upper())
        return relation.replace_path(database=target) if target else relation

    RuntimeSourceResolver.resolve = moved
    from snowflake.connector.cursor import SnowflakeCursor

    execute = SnowflakeCursor.execute

    def named_moved(self, command, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ANN202
        # (a model that names `snowflake.account_usage.x` itself, not through source(): read from the copy too)
        return execute(self, renamed_sql(command, renamed) if renamed and isinstance(command, str) else command, *args, **kwargs)

    SnowflakeCursor.execute = named_moved

    def in_utc(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs["session_parameters"] = {**(kwargs.get("session_parameters") or {}), "TIMEZONE": "UTC"}
        return connect(*args, **kwargs)

    SnowflakeAdapter.pre_model_hook = lambda self, config: None
    snowflake.connector.connect = in_utc
    try:
        yield
    finally:
        SnowflakeAdapter.pre_model_hook, snowflake.connector.connect, RuntimeSourceResolver.resolve = hook, connect, resolve
        SnowflakeCursor.execute = execute


def renamed_sql(sql: str, renamed: dict) -> str:
    """`sql` with three-part names in the databases of `renamed` pointed at their copies (`snowflake.account_usage.x`
    -> `FAUXFLAKE_SNOWFLAKE.account_usage.x`); text inside string literals and comments is left as it is."""
    names = "|".join(re.escape(k) for k in renamed)
    part = r'(?:"[^"]+"|[A-Za-z_][\w$]*)'
    pattern = re.compile(r'(?<![\w$."])(?:"(' + names + r')"|(' + names + r'))(?=\s*\.\s*' + part + r'\s*\.\s*' + part + ')', re.I)
    pieces = re.split(r"('(?:[^'\\]|\\.|'')*'|--[^\n]*|/\*.*?\*/)", sql, flags=re.S)
    for i in range(0, len(pieces), 2):
        pieces[i] = pattern.sub(lambda m: renamed[(m.group(1) or m.group(2)).upper()]
                                if (m.group(1) or m.group(2)).upper() in renamed and (m.group(2) or m.group(1) == m.group(1).upper())
                                else m.group(0), pieces[i])
    return "".join(pieces)


def _instant(v):  # noqa: ANN001, ANN202
    """A timestamp or date as a naive UTC datetime."""
    from datetime import date, timezone

    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    return None


def _is_clock(duck, both: str, column: str, kind: str, clock) -> bool:  # noqa: ANN001
    """The column holds when the build ran, not something the engine worked out: the emulator's frozen clock or a time
    within its own build on its side, a time within the Snowflake build on the other (CURRENT_TIMESTAMP() and the like
    differ by the hour of the build, not by engine). Also what dbt itself writes into rows: `run_started_at` (as a
    timestamp or as text, in UTC or in this machine's time) and `invocation_id`, one per build.
    `clock`: (frozen, snowflake build start, its end[, {"local": (start, end), "ids": (emulator's, Snowflake's)}])."""
    if clock is None:
        return False
    frozen, start, end = clock[:3]
    extra = clock[3] if len(clock) > 3 else {}
    q = f'"{column}"'
    if not kind.startswith(("TIMESTAMP", "DATE")):
        if not kind.startswith("VARCHAR"):
            return False
        lo_m, hi_m, lo_t, hi_t = duck.execute(
            both + f"select (select min({q}) from m), (select max({q}) from m), (select min({q}) from t), (select max({q}) from t)").fetchone()
        ids = extra.get("ids")
        if ids and lo_m == hi_m == ids[0] and lo_t == hi_t == ids[1]:
            return True  # the build's invocation_id
        q = f"try_cast({q} as timestamp)"  # `run_started_at` written as text
        kind = "TIMESTAMP"
    lo_m, hi_m, lo_t, hi_t = (_instant(v) for v in duck.execute(
        both + f"select (select min({q}) from m), (select max({q}) from m), (select min({q}) from t), (select max({q}) from t)").fetchone())
    if None in (lo_m, hi_m, lo_t, hi_t):
        return False
    slack = timedelta(seconds=2)
    if kind == "DATE":
        days = {frozen.date(), start.date(), end.date(), *(d.date() for d in extra.get("local", ()))}
        return lo_m.date() == hi_m.date() and lo_m.date() in days and lo_t.date() in days and hi_t.date() in days
    shift = datetime.now().astimezone().utcoffset() or timedelta(0)  # (a time of this machine's day, as dbt writes `now()`)

    def within(lo: datetime, hi: datetime, a: datetime, b: datetime, d: timedelta) -> bool:
        try:
            return a - slack <= lo + d and hi + d <= b + slack
        except OverflowError:  # (a data vault's ghost record of the year 1, less an offset: no time of a build)
            return False

    # (a time shown in a zone, CONVERT_TIMEZONE('America/Los_Angeles', CURRENT_TIMESTAMP()): the same offset of a
    # quarter hour on both engines; this machine's own offset too)
    offsets = {timedelta(0), -shift, *(timedelta(minutes=15 * k) for k in range(-14 * 4, 14 * 4 + 1))}
    for d in offsets:
        mine = within(lo_m, hi_m, frozen, frozen, d) or ("local" in extra and within(lo_m, hi_m, *extra["local"], d))
        if mine and within(lo_t, hi_t, start, end, d):
            return True
    # (from the first Snowflake build of the run: a model that reads another by name, not through ref(), can run before
    # that one is built again, and read what an earlier day's build wrote)
    # (each day's build: a model that reads another by name, not through ref(), can run before that one is built again
    # and read what an earlier day wrote; an incremental model keeps every day's rows)
    days = [(f, b - slack, e + slack) for f, b, e in extra.get("builds") or [(frozen, start, end)] if f is not None]
    days[-1] = (days[-1][0], days[-1][1], max(days[-1][2], end + slack))  # (a view read after the build: its read)
    return _clock_relative(duck, both, q, days)


def _clock_relative(duck, both: str, q: str, days: list[tuple]) -> bool:  # noqa: ANN001
    """Times written from the clock (`dateadd('day', -30, current_timestamp)`): a statement reads the clock once, so the
    emulator's values are its clock that day plus some offsets, and Snowflake's are its clock during that day's build
    plus the same offsets. The distinct values one side has and the other lacks pair up in order, each pair as many
    times on both sides, and each pair's difference puts the emulator's clock of some day inside that day's Snowflake
    build. `days`: (emulator clock, build start, end)."""
    # (only the values one side has and the other lacks: a row the two agree on, an hour truncated, say, is no evidence)
    def counted(a: str, b: str) -> list[tuple]:
        return [(_instant(v), n) for v, n in duck.execute(
            both + f"select v, count(*) from (select {q} v from {a} except all select {q} from {b}) group by v "
                   "order by v nulls last limit 10001").fetchall()]

    mine, theirs = counted("m", "t"), counted("t", "m")
    if not mine or not days or len(mine) != len(theirs) or len(mine) > 10000:
        return False
    if any((a is None) != (b is None) or n != k for (a, n), (b, k) in zip(mine, theirs)):
        return False
    return all(any(lo <= frozen + (b - a) <= hi for frozen, lo, hi in days) for (a, _), (b, _) in zip(mine, theirs) if a is not None)

def run(conn, wh: Warehouse, local: DbtProject, remote: DbtProject, start: datetime, days: int,
        remote_schema: str = "FAUXFLAKE_PARITY", log=print, select: list[str] | None = None, exclude: list[str] | None = None,
        renamed: dict | None = None, remote_vars: str | None = None,
        remote_env: dict | None = None) -> tuple[list[Parity], list[Difference]]:
    """The same sources on both engines, the same project built on both (every node, also after one fails), then
    what each did compared: which nodes failed on one side only, which tests answered differently, which models hold
    other rows. Day by day for incremental models."""
    import os

    from . import objects

    found: list[Difference] = []
    rows: list[Parity] = []
    builds: list[tuple] = []  # each day's (emulator clock, Snowflake build start, its end)
    # both engines run as the same role: the one Snowflake's session has (masking policies show it what they show it)
    (role,) = conn.cursor().execute("select current_role()").fetchone()
    for i in range(days + 1):
        now = start + timedelta(days=i)
        prev = wh.now
        wh.advance_to(now)
        if role:
            objects.set_role(wh.emu.duck, role)
        log(f"{now:%Y-%m-%d}  seeding Snowflake")
        seed(conn, wh, after=prev, log=log, renamed=renamed)
        # the account's warehouses, monitors and parameters as Snowflake shows them now: both engines read the same
        account.mirror(wh.emu, conn, account.shown_by(local.dir))
        full = ["--full-refresh"] if i == 0 else []
        extra = ["--vars", remote_vars] if remote_vars else []
        if i == 0:
            with _environment(remote_env):
                _databases_built_in(conn, remote, remote_schema, extra)
            _existing_on_snowflake(conn, wh, renamed, log, local)
        os.environ["FAUXFLAKE_TESTS_DO_NOT_BLOCK"] = "1"  # (a failing test keeps nothing else from being built and compared)
        try:
            # the same schema name on both sides: some models embed their own relation names
            chosen = (["--select", *select] if select else []) + (["--exclude", *exclude] if exclude else [])
            here = datetime.now(timezone.utc).replace(tzinfo=None)
            from . import emulator

            if emulator.FROZEN_NOW is not None and i > 0:
                # the clock moves between the days, as Snowflake's does: a load stamped with SYSDATE() (a data vault's
                # LDTS) is newer than the last one on both engines, not the same moment again on the emulator's
                emulator.FROZEN_NOW = conn.cursor().execute("select sysdate()").fetchone()[0]
            local.invoke(["build", *full, *chosen], schema=remote_schema, quiet=True)
            built_here = (here, datetime.now(timezone.utc).replace(tzinfo=None))
            mine = outcomes(local)
            began = conn.cursor().execute("select sysdate()").fetchone()[0]
            with remote_session(renamed), _environment(remote_env):
                remote.invoke(["build", *full, *chosen, *extra], schema=remote_schema, quiet=True)
            ended = conn.cursor().execute("select sysdate()").fetchone()[0]
            theirs = outcomes(remote)
        finally:
            os.environ.pop("FAUXFLAKE_TESTS_DO_NOT_BLOCK", None)
        log(f"{now:%Y-%m-%d}  built on both engines")
        built = {uid for uid in mine if mine[uid][0] == "success" and theirs.get(uid, ("",))[0] == "success"}
        from . import emulator

        ids = (_invocation(local), _invocation(remote))
        builds.append((emulator.FROZEN_NOW, began, ended))
        clock = (emulator.FROZEN_NOW, began, ended, {"local": built_here, "ids": ids, "builds": list(builds)}) if emulator.FROZEN_NOW else None
        stopped, rows = None, []
        try:
            rows = compare(conn, wh.emu, local, only=built, clock=clock)
            undetermined(conn, remote, rows)  # (each model where dbt put it: the same schema on both sides)
        except SnowflakeUnavailable as e:
            rows = rows or e.rows  # (all of them, when it stopped while ties were looked for)
            stopped = Difference("(comparison)", "snowflake_unavailable",
                                 f"Snowflake stopped answering after {len(rows)} model(s) were compared: {e}; the models "
                                 "not read are not compared, and the differences found are not all checked for ties")
            log(f"{now:%Y-%m-%d}  {stopped.detail}")
        try:
            graph = json.loads((local.target_path / "manifest.json").read_text())["nodes"]
            depends = {uid: n.get("depends_on", {}).get("nodes", []) for uid, n in graph.items()}
        except (OSError, ValueError, KeyError):
            depends = {}
        found = differences(mine, theirs, rows, depends) + ([stopped] if stopped else [])
        if stopped:
            break
    return rows, found
