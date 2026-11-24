"""Time travel: `t AT(OFFSET => -60)`, `AT(TIMESTAMP => ...)`, `BEFORE(STATEMENT => '<query id>')`.

A table's earlier states are kept only for the tables a statement has read that way (or the spec's `time_travel:`
lists): before a statement writes to one, its rows as they stand are copied aside, stamped with the time and, once
the statement has run, its query id. A read AT a time takes the state that held then; BEFORE a statement, the state
that statement replaced. A time before the first kept state reads that first state; one the table did not exist at
is Snowflake's error only when nothing at all was kept, as there is then no telling."""
from __future__ import annotations

import itertools
import time
from datetime import timezone

from sqlglot import exp

SCHEMA = "_fs_global._fx_history"
KEEP: set[tuple[str, str, str]] = set()  # tables whose earlier states are kept
VERSIONS: dict[tuple[str, str, str], list[dict]] = {}  # table -> [{name, until, statement}], oldest first
_COUNTER = itertools.count()
_STARTED = time.time()


def _fq(cursor, table: exp.Table) -> tuple[str, str, str]:  # noqa: ANN001
    conn = cursor._conn
    return ((table.catalog or conn.database or "").upper(), (table.db or conn.schema or "").upper(), table.name.upper())


def _target(tree: exp.Expression) -> exp.Table | None:
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        t = tree.this
        if isinstance(t, exp.Schema):
            t = t.this
        return t if isinstance(t, exp.Table) else None
    if isinstance(tree, exp.Create) and tree.kind == "TABLE" and tree.args.get("replace"):
        t = tree.this
        return t.this if isinstance(t, exp.Schema) else t if isinstance(t, exp.Table) else None
    if isinstance(tree, exp.TruncateTable) and tree.expressions:
        return tree.expressions[0] if isinstance(tree.expressions[0], exp.Table) else None
    if isinstance(tree, exp.Drop) and tree.kind == "TABLE" and isinstance(tree.this, exp.Table):
        return tree.this
    return None


def before(cursor, tree: exp.Expression) -> None:  # noqa: ANN001
    """Note the tables read back in time; copy aside the one this statement is about to change, if it is kept."""
    for node in tree.find_all(exp.Table):
        if isinstance(node.args.get("when"), exp.HistoricalData):
            KEEP.add(_fq(cursor, node))
    target = _target(tree)
    if target is None:
        return
    fq = _fq(cursor, target)
    if fq not in KEEP:
        return
    duck = cursor._duck_conn
    raw = f"{fq[0]}.{fq[1]}.{fq[2]}#{next(_COUNTER)}"
    name = '"' + raw.replace('"', '""') + '"'
    source = ".".join('"' + p.replace('"', '""') + '"' for p in fq)
    try:
        duck.execute(f"create schema if not exists {SCHEMA}")
        duck.execute(f"create table {SCHEMA}.{name} as select * from {source}")
    except Exception:  # noqa: BLE001 - not there yet: nothing to keep
        return
    version = {"name": raw, "until": time.time(), "statement": None}
    VERSIONS.setdefault(fq, []).append(version)
    cursor._fx_versions = [*getattr(cursor, "_fx_versions", []), version]


def after(cursor) -> None:  # noqa: ANN001
    """The query id of the statement that replaced each state just kept."""
    for version in getattr(cursor, "_fx_versions", []):
        version["statement"] = cursor.sfqid
    cursor._fx_versions = []


def _real_time(value: exp.Expression, cursor) -> float | None:  # noqa: ANN001
    """A TIMESTAMP => value as this machine's clock: the emulator's own clock may be frozen at another time."""
    from . import emulator

    try:
        row = cursor._duck_conn.execute("select epoch(" + value.sql(dialect="duckdb") + "::timestamptz)").fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row is None or row[0] is None:
        return None
    at = float(row[0])
    if emulator.FROZEN_NOW is not None:
        frozen = emulator.FROZEN_NOW.replace(tzinfo=timezone.utc).timestamp()
        return _STARTED + (at - frozen)
    return at


def _state(fq: tuple[str, str, str], when: exp.HistoricalData, cursor) -> str | None:  # noqa: ANN001
    """The table holding fq as it stood then, or None for its current rows."""
    versions = VERSIONS.get(fq) or []
    kind = str(when.args.get("kind") or "").upper()
    before = str(when.this).upper() == "BEFORE"
    value = when.expression
    if kind == "STATEMENT":
        qid = value.name if isinstance(value, exp.Literal) else None
        for i, v in enumerate(versions):
            if v["statement"] == qid:
                if before:
                    return v["name"]
                return versions[i + 1]["name"] if i + 1 < len(versions) else None
        return None
    if kind == "OFFSET":
        try:
            seconds = float(cursor._duck_conn.execute("select (" + value.sql(dialect="duckdb") + ")::double").fetchone()[0])
        except Exception:  # noqa: BLE001
            return None
        at = time.time() + seconds
    elif kind == "TIMESTAMP":
        at = _real_time(value, cursor)
        if at is None:
            return None
    else:
        return None
    for v in versions:  # the first state still holding at that time (BEFORE a time: one an instant earlier)
        if v["until"] > at or (before and v["until"] >= at):
            return v["name"]
    return None


def rewrite(tree: exp.Expression, cursor) -> exp.Expression:  # noqa: ANN001
    """Each table read AT or BEFORE some point: the state it had then (its current rows when none was kept)."""
    for node in list(tree.find_all(exp.Table)):
        when = node.args.get("when")
        if not isinstance(when, exp.HistoricalData):
            continue
        node.set("when", None)
        original = node.name
        name = _state(_fq(cursor, node), when, cursor)
        if name is None:
            continue
        catalog, db = SCHEMA.split(".")
        node.set("this", exp.to_identifier(name, quoted=True))
        node.set("db", exp.to_identifier(db))
        node.set("catalog", exp.to_identifier(catalog))
        if not node.alias:  # (columns named t.x still read it)
            node.set("alias", exp.TableAlias(this=exp.to_identifier(original)))
    return tree


def keep(tables: list[str]) -> None:
    """The spec's `time_travel:`: tables kept from the start (DB.SCHEMA.TABLE)."""
    for t in tables:
        parts = [p.strip('"').upper() for p in t.split(".")]
        if len(parts) == 3:
            KEEP.add(tuple(parts))  # type: ignore[arg-type]

