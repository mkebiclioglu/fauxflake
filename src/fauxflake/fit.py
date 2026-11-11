"""Teach a spec what reading the SQL could not tell, by trying the project out.

`init` reads the project's SQL to find each source's columns and types. That misses whatever
is only implied: a source read with `select *` and used three models later, a text column that
is summed, a column that must be unique because a test says so. `fit` finds those the way a
person would: run it, read the error, fix the spec, run it again.

Two loops, cheap one first:

* `learn_from_models` runs every compiled model straight on the emulator against a few
  generated rows. "Table X does not have a column named Y" adds Y to the source behind X;
  "Could not convert string 'amount_3fa2' to DOUBLE" retypes the column that produced the value.
* `learn_from_build` runs the real `dbt build` and reads `run_results.json`: models that only
  compile once their parents exist, and the tests (`unique`, `not_null`, `accepted_values`).

Nothing here hides a failure: what cannot be fixed by editing the spec is reported as it is.
"""

from __future__ import annotations

import graphlib
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import sqlglot
import yaml
from sqlglot import exp

from . import emulator
from .init import guess, within_bounds
from .spec import quoted

Log = Callable[[str], None]

_MISSING = [
    re.compile(r'(?:Table|Values list) "(?P<table>[^"]+)" does not have a column named "(?P<col>[^"]+)"'),
    re.compile(r'Referenced column "(?P<col>[^"]+)" not found in FROM clause'),
    re.compile(r'Referenced column "?(?P<col>[^"\s]+)"? not found in FROM clause'),
    re.compile(r'Referenced column "(?P<col>[^"]+)" was not found because the FROM clause is missing'),
    re.compile(r'Column "(?P<col>[^"]+)" referenced that exists in the SELECT clause'),
    re.compile(r'Alias with name "+(?P<col>[^"]+)"+ exists, but aliases cannot be used'),
    re.compile(r'Column "(?P<col>[^"]+)" in EXCLUDE list not found'),
    re.compile(r'Referenced table "(?P<table>[^"]+)" not found!?\s+Candidate tables: (?P<cands>[^\n]+)\n\n?LINE \d+:[^\n]*?(?P=table)\.(?P<col>\w+)', re.S),
]
_CONVERT = [
    (re.compile(r"Could not convert string '(?P<value>[^']*)' to (?P<type>[A-Z0-9]+)"), None),
    (re.compile(r'(?P<type>date|timestamp|time) field (?:format|value out of range): "(?P<value>[^"]*)"'), None),
    (re.compile(r"""Malformed JSON.*?Input: "?(?P<value>[^"\n]*)""", re.S), "VARIANT"),
    (re.compile(r'Expected (?P<type>ARRAY|OBJECT), but got VARCHAR: "(?P<value>[^"]*)"'), None),
    (re.compile(r'''Could not parse string "(?P<value>[^"]*)" according to format'''), "TIMESTAMP"),
    (re.compile(r"""Failed to cast value: Could not convert string '(?P<value>[^']*)' to (?P<type>[A-Z0-9]+)"""), None),
]
_NO_TABLE = re.compile(r'Table with name "?(?:[\w$]+\.)*(?P<name>[\w$]+)"? does not exist')
_NO_FUNCTION = re.compile(r"No function matches the given name and argument types '(?P<name>\w+)\((?P<types>[^']*)\)'")
_TEMPORAL = {"epoch", "epoch_ms", "date_part", "datepart", "extract", "year", "month", "day", "hour", "minute", "second",
             "dayofweek", "dayofyear", "week", "quarter", "last_day", "dayname", "monthname"}
_NUMERIC = {"abs", "round", "floor", "ceil", "sqrt", "ln", "log", "exp", "power", "sign"}
_NOT_STRUCT = re.compile(r"""Cannot extract field '(?P<field>[^']+)' from expression "(?P<col>[^"]+)" because it is not a struct""")
_HINTS = {"INT8": "BIGINT", "INT16": "BIGINT", "INT32": "BIGINT", "INT64": "BIGINT", "INT128": "BIGINT", "UINT8": "BIGINT",
          "UINT16": "BIGINT", "UINT32": "BIGINT", "UINT64": "BIGINT", "HUGEINT": "BIGINT", "BIGINT": "BIGINT", "INTEGER": "BIGINT",
          "DOUBLE": "DOUBLE", "FLOAT": "DOUBLE", "DECIMAL": "DOUBLE", "BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
          "TIMESTAMP": "TIMESTAMP", "TIME": "TIMESTAMP", "VARIANT": "VARIANT", "ARRAY": "ARRAY", "OBJECT": "VARIANT"}
_FREE = ("template", "pool")  # generators that only make up text: safe to retype
_INSPECTS = re.compile(r"run_query\(|statement\(|load_result\(|get_columns_in_relation\(|get_relation\(|get_relations_by|"
                       r"get_filtered_columns_in_relation|list_relations")
_HARMLESS = {"macro.dbt.is_incremental", "macro.dbt.should_full_refresh"}  # ask only whether the model's own table exists


def duck_type(gen: dict) -> str:
    """The DuckDB type of the column a generator entry produces (close enough to add it empty)."""
    if gen.get("type"):
        return str(gen["type"])
    kind = gen["gen"]
    if kind in ("time_offset", "loaded_at", "event_time", "updated_at"):
        return "TIMESTAMP"
    if kind in ("object", "array"):
        return "JSON"
    if kind in ("sequence", "dist", "fk"):
        return "BIGINT"
    return "VARCHAR"


def _fq(relation_name: str) -> str:
    return ".".join(part.strip('"').upper() for part in relation_name.split("."))


class Fitter:
    def __init__(self, emu, spec_path: Path, log: Log = print, bounds: dict | None = None) -> None:  # noqa: ANN001
        self.emu, self.path, self.log = emu, Path(spec_path), log
        self.bounds = bounds or {}
        text = self.path.read_text()
        self.header = "".join(line + "\n" for line in text.splitlines() if line.startswith("#"))
        self.spec = yaml.safe_load(text)
        self.tables: dict[str, dict] = self.spec.setdefault("tables", {})
        self.existing: dict[str, dict] = self.spec.get("existing") or {}
        self.nodes: dict[str, dict] = {}
        self.project_relations: set[str] = set()
        self.relation_owner: dict[str, str] = {}
        self.uncompiled: dict[str, str] = {}  # model -> why it did not compile
        self.pending: dict[str, dict[str, str]] = {}  # stand-ins for those models: relation -> columns read from it
        self.stand_ins: set[str] = set()
        self.deferred: set[str] = set()  # models whose SQL only exists during a build
        self.retyped: set[str] = set()  # sources whose generated values changed since the last pass
        self.model_timeout = float(os.environ.get("FAUXFLAKE_FIT_TIMEOUT", "90"))  # seconds one model may take while fitting
        self.learned: list[str] = []
        self.dirty = False
        self.trees: dict[str, exp.Expression | None] = {}
        self.sql_of: dict[str, str] = {}
        self.by_rel: dict[str, str] = {}
        self.parents: dict[str, set[str]] = {}

    # --- the spec

    def save(self) -> None:
        if self.existing:
            self.spec["existing"] = self.existing
        self.path.write_text(self.header + yaml.safe_dump(self.spec, sort_keys=False, default_flow_style=None, width=110))

    def generate(self, scale: float | None = None, history: int = 3):  # noqa: ANN201
        from .generate import Warehouse
        from .spec import Spec

        self.save()
        spec = Spec.load(self.path)
        if scale is None:
            # a few hundred rows per table: enough for filters to let some through, little enough to be quick
            smallest = min((float((t.raw.get("volume") or {}).get("rows_per_day", 1000)) for t in spec.tables.values()), default=1000.0)
            scale = min(1.0, max(1e-5, 100.0 / max(smallest, 1.0)))
        wh = Warehouse(self.emu, spec, scale)
        wh.advance_to(datetime.combine(spec.start, datetime.min.time()) + timedelta(days=history))
        self.dirty = False
        return wh

    def add_column(self, table: str, col: str) -> bool:
        columns = self.tables[table].setdefault("columns", {})
        if col in columns:
            return False
        columns[col] = guess(col, None)
        self.emu.duck.execute(f'alter table {quoted(table)} add column if not exists "{col}" {duck_type(columns[col])}')
        self.learned.append(f"{table}.{col}: read by a model, added.")
        return True

    def missing_relation(self, sql_key: str, message: str) -> bool:
        """A model reads a relation that nothing has created. If the project builds it later (a
        hard-coded name instead of ref(), as in a model that looks at its own previous output), it
        starts out as an empty view, like a first run after the table was created by hand. Anything
        else is a source nobody declared: it becomes a generated table."""
        m = _NO_TABLE.search(message)
        t = self.tree(sql_key) if m else None
        node = self.nodes.get(sql_key)
        if t is None or node is None:
            return False
        ctes = {c.alias.upper() for c in t.find_all(exp.CTE)}
        name = m.group("name").upper()
        for tb in t.find_all(exp.Table):
            if tb.name.upper() != name or (not tb.db and name in ctes):
                continue
            fq = ".".join([(tb.catalog or node["database"]).upper(), (tb.db or node["schema"]).upper(), name])
            if fq in self.tables or fq in self.existing:
                continue
            owner = self.relation_owner.get(fq)
            if owner and owner in self._ancestors(sql_key):
                if owner in self.uncompiled and fq not in self.pending:
                    # the parent could not even compile (its Jinja found nothing to work with). Stand in for
                    # it, to learn which columns this model expects from it: see push_pending()
                    self.emu.ensure_database(*fq.split(".")[:2])
                    self.pending[fq] = {c: duck_type(guess(c, None)) for c in self.columns_read(t, tb)}
                    self.stand_ins.add(fq)
                    self.create_pending(fq)
                    return True
                return False  # a ref() to a model that did not build: that model's failure, not a missing table
            db, schema, _ = fq.split(".")
            self.emu.ensure_database(db, schema)
            read = self.columns_read(t, tb)
            if fq in self.project_relations:
                self.existing[fq] = {"columns": {c: duck_type(guess(c, None)) for c in read}}
                self.create_existing(fq)
                self.learned.append(f"{fq}: read by {node['name']} before the project builds it; starts as an empty relation.")
            else:
                self.tables[fq] = {"volume": {"rows_per_day": 1_000_000, "weekly": [1] * 7},
                                   "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]},
                                   "columns": {"_LOADED_AT": {"gen": "loaded_at"}, **{c: guess(c, None) for c in read if c != "_LOADED_AT"}}}
                cols = ", ".join(f'"{c}" {duck_type(g)}' for c, g in self.tables[fq]["columns"].items())
                self.emu.duck.execute(f"create table if not exists {quoted(fq)} ({cols})")
                self.learned.append(f"{fq}: read by {node['name']} but not declared as a source; now generated.")
                self.retyped.add(fq)
                self.dirty = True
            return True
        return False

    @staticmethod
    def columns_read(tree: exp.Expression, table: exp.Table) -> list[str]:
        """Columns a query reads from one table: those qualified with its name, and the bare
        ones in a SELECT that reads nothing else."""
        select = table.find_ancestor(exp.Select)
        if select is None:
            return []
        alias = table.alias_or_name.upper()
        alone = not select.args.get("joins") and select.args.get("from_") is not None and select.args["from_"].this is table
        out: list[str] = []
        for col in select.find_all(exp.Column):
            if isinstance(col.this, exp.Star) or col.find_ancestor(exp.Select) is not select:
                continue
            if (col.table.upper() == alias if col.table else alone) and col.name.upper() not in out:
                out.append(col.name.upper())
        return out

    def bootstrap(self, fq: str, built: str) -> None:
        """A model that reads its own relation by name was just built against the empty stand-in.
        Its real columns are now known: from here on the relation starts each run as an empty
        table of that shape, which is what a project like this finds in production."""
        duck = self.emu.duck
        kind = duck.execute("select table_type from information_schema.tables where table_catalog = ? and table_schema = ? "
                            "and table_name = ?", fq.split(".")).fetchone()
        duck.execute(f"drop {'view' if kind and kind[0] == 'VIEW' else 'table'} if exists {quoted(fq)}")
        duck.execute(f'alter table {built} rename to "{fq.split(".")[2]}"')
        columns = {r[0]: r[1] for r in duck.execute(f"describe {quoted(fq)}").fetchall()}
        if self.existing[fq] != {"columns": columns, "as": "table"}:
            self.existing[fq] = {"columns": columns, "as": "table"}
            self.learned.append(f"{fq}: built once; later runs start from an empty table with its {len(columns)} columns.")

    def _ancestors(self, uid: str) -> set[str]:
        seen: set[str] = set()
        stack = list(self.nodes.get(uid, {}).get("depends_on", {}).get("nodes", []))
        while stack:
            node = stack.pop()
            if node not in seen:
                seen.add(node)
                stack += self.nodes.get(node, {}).get("depends_on", {}).get("nodes", [])
        return seen

    def create_pending(self, fq: str) -> None:
        from .generate import existing_view

        self.emu.duck.execute(existing_view(fq, self.pending[fq], replace=True))

    def push_pending(self) -> None:
        """Stand-ins for models that did not compile have collected the columns read from them."""
        for fq, columns in self.pending.items():
            self.give_union_columns(fq, self.uncompiled.get(self.relation_owner.get(fq, ""), ""), list(columns))
        self.pending = {}

    def give_union_columns(self, fq: str, message: str, columns: list[str]) -> bool:
        """A model built with dbt_utils.union_relations over the tables it finds keeps only the columns
        a macro lists, so no SQL names them for the sources. What the models downstream read from it
        is what those tables must have: add the columns to the tables the failed compile looked at."""
        m = re.search(r"no columns found to union for relations ([^\n]+)", message)
        if not m or not columns:
            return False
        names = {n.strip().strip('"').upper() for n in m.group(1).split(",")}
        by_schema: dict[str, list[str]] = {}
        for table in self.tables:
            if table.split(".")[-1] in names:
                by_schema.setdefault(table.rsplit(".", 1)[0], []).append(table)
        if not by_schema:
            return False
        most = max(len(v) for v in by_schema.values())
        added = 0
        for tables in by_schema.values():
            if len(tables) < most:
                continue
            for table in tables:
                for col in columns:
                    if col not in self.tables[table].setdefault("columns", {}):
                        self.tables[table]["columns"][col] = guess(col, None)
                        self.retyped.add(table)
                        added += 1
        if added:
            self.dirty = True
            self.learned.append(f"{fq}: unions {len(names)} tables by a column list; gave them the {len(columns)} columns read from it.")
        return bool(added)

    def columns_children_read(self, uid: str) -> list[str]:
        """Columns the models that depend on `uid` read from it, also through a `select *` CTE."""
        fq = _fq(self.nodes[uid]["relation_name"])
        name = fq.split(".")[-1]
        out: list[str] = []
        for child, node in self.nodes.items():
            if uid not in node.get("depends_on", {}).get("nodes", []) or child not in self.sql_of:
                continue
            tree = self.tree(child)
            if tree is None:
                continue
            for tb in tree.find_all(exp.Table):
                if tb.name.upper() != name:
                    continue
                found = self.columns_read(tree, tb)
                select = tb.find_ancestor(exp.Select)
                cte = select.parent if select is not None and isinstance(select.parent, exp.CTE) else None
                if cte is not None and select.find(exp.Star) and not select.args.get("joins"):
                    for other in tree.find_all(exp.Table):  # `with t as (select * from it) select a, b from t`
                        if other.name.upper() == cte.alias.upper() and not other.db:
                            found += self.columns_read(tree, other)
                out += [c for c in found if c not in out]
        return out

    def create_existing(self, fq: str) -> None:
        from .generate import existing_view

        kind = self.emu.duck.execute("select table_type from information_schema.tables where table_catalog = ? and "
                                     "table_schema = ? and table_name = ?", fq.split(".")).fetchone()
        if kind is None or kind[0] == "VIEW":  # once the project has built the real thing, leave it alone
            self.emu.duck.execute(existing_view(fq, self.existing[fq].get("columns") or {}, replace=True))

    def retype(self, value: str, hint: str, fmt: str | None = None) -> bool:
        """The generated text `value` was converted to `hint`: retype the column(s) that made it.
        Generated text starts with its column's name, which is how the column is found."""
        tokens = value.split()
        changed = False
        for position, token in enumerate(tokens):
            name = token.rsplit("_", 1)[0].upper() if "_" in token else ""
            for table, t in self.tables.items():
                gen = (t.get("columns") or {}).get(name)
                for holder in (t.get("columns") or {}).values():  # the same, for a field inside an object column
                    fields = holder.get("fields") if holder.get("gen") == "object" else None
                    for path, field in list((fields or {}).items()):
                        if path.split(".")[-1].upper() == name and field.get("gen") in _FREE and not field.get("format"):
                            fresh = {"gen": "time_offset", "format": fmt, "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]} if fmt \
                                else guess(name, hint)
                            if fresh["gen"] not in _FREE:
                                fields[path] = fresh
                                self.retyped.add(table)
                                self.learned.append(f"{table}: field {path} used as {fresh.get('format') or hint}, retyped.")
                                changed = True
                if not gen or gen.get("gen") not in _FREE or gen.get("type") or gen.get("format"):
                    continue
                if fmt:  # parsed with an explicit format: text in that format
                    fresh = {"gen": "time_offset", "format": fmt, "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
                elif hint == "TIMESTAMP" and len(tokens) > 1 and position > 0:
                    # `date_col || ' ' || time_col` read as a timestamp: this one is the time of day
                    fresh = {"gen": "time_offset", "format": "%H:%M:%S", "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
                else:
                    fresh = guess(name, hint)
                if fresh["gen"] in _FREE:
                    continue
                if "follows" in gen:
                    fresh["follows"] = gen["follows"]
                within_bounds(fresh, self.bounds.get((table, name)))
                t["columns"][name] = fresh
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: used as {fresh.get('format') or hint}, retyped.")
                changed = True
        self.dirty |= changed
        return changed

    def make_variant(self, col: str, field: str) -> bool:
        changed = False
        for table, t in self.tables.items():
            gen = (t.get("columns") or {}).get(col.upper())
            if gen and gen.get("gen") in _FREE and not gen.get("type"):
                t["columns"][col.upper()] = {"gen": "object", "fields": {field.lower(): guess(field, None)}}
                self.retyped.add(table)
                self.learned.append(f"{table}.{col.upper()}: read as an object ({field}), retyped.")
                changed = True
            elif gen and gen.get("gen") == "object" and field.lower() not in gen.get("fields", {}) and field.upper() not in gen.get("fields", {}):
                gen.setdefault("fields", {})[field.lower()] = guess(field, None)
                self.retyped.add(table)
                changed = True
        self.dirty |= changed
        return changed

    # --- which source owns a column

    def tree(self, uid: str):  # noqa: ANN201
        if uid not in self.trees:
            try:
                self.trees[uid] = sqlglot.parse_one(self.sql_of[uid], read="snowflake")
            except Exception:  # noqa: BLE001
                self.trees[uid] = None
        return self.trees[uid]

    def real_tables(self, node: exp.Expression, ctes: dict[str, exp.Expression], depth: int = 0) -> set[str]:
        out: set[str] = set()
        for t in node.find_all(exp.Table):
            name = t.name.upper()
            if not t.db and name in ctes:
                if depth < 6:
                    out |= self.real_tables(ctes[name], ctes, depth + 1)
            elif t.catalog and t.db:
                out.add(f"{t.catalog.upper()}.{t.db.upper()}.{name}")
        return out

    def passthrough(self, fq: str):  # noqa: ANN201
        """The single relation a model is a `select *` of, if that is what it is."""
        uid = self.by_rel.get(fq)
        t = self.tree(uid) if uid else None
        if t is None or not t.find(exp.Star):
            return None
        tables = self.real_tables(t, {c.alias.upper(): c.this for c in t.find_all(exp.CTE)})
        return next(iter(tables)) if len(tables) == 1 else None

    def blameable(self, fq: str, depth: int = 0) -> bool:
        if fq in self.tables or fq in self.existing or fq in self.pending:
            return True
        parent = self.passthrough(fq) if depth <= 8 else None
        return bool(parent) and self.blameable(parent, depth + 1)

    def blame(self, fq: str, col: str, depth: int = 0) -> bool:
        if fq in self.tables:
            return self.add_column(fq, col)
        if fq in self.pending:
            if col in self.pending[fq]:
                return False
            self.pending[fq][col] = duck_type(guess(col, None))
            self.create_pending(fq)
            return True
        if fq in self.existing:
            columns = self.existing[fq].setdefault("columns", {})
            if col in columns:
                return False
            columns[col] = duck_type(guess(col, None))
            self.create_existing(fq)
            return True
        parent = self.passthrough(fq) if depth <= 8 else None
        if parent and self.blame(parent, col, depth + 1):
            try:  # the model was already built without the column: give it one so the pass can go on
                self.emu.duck.execute(f'alter table {quoted(fq)} add column if not exists "{col}" varchar')
            except Exception:  # noqa: BLE001, S110
                pass
            return True
        return False

    def missing_column(self, sql_key: str, message: str) -> bool:
        m = next((m for m in (p.search(message) for p in _MISSING) if m), None)
        t = self.tree(sql_key) if m else None
        if t is None:
            return False
        ctes = {c.alias.upper(): c.this for c in t.find_all(exp.CTE)}
        col = m.group("col").upper().split(".")[-1].strip('"')
        if not re.fullmatch(r"[\w$ ]+", col):
            return False
        candidates: set[str] = set()
        alias = (m.groupdict().get("table") or "").upper()
        if alias:
            for tb in t.find_all(exp.Table):
                if tb.alias_or_name.upper() != alias:
                    continue
                if not tb.db and tb.name.upper() in ctes:
                    candidates |= self.real_tables(ctes[tb.name.upper()], ctes)
                elif tb.catalog and tb.db:
                    candidates.add(f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}")
            if not candidates and alias in ctes:
                candidates = self.real_tables(ctes[alias], ctes)
        if not candidates:
            candidates = self.real_tables(t, ctes)
        candidates = {c for c in candidates if self.blameable(c)}
        if len(candidates) > 1:
            # several sources could own it: the one already known to have a column of that name
            # cannot be the one missing it
            lacking = {c for c in candidates if c not in self.tables or col not in self.tables[c].get("columns", {})}
            candidates = lacking or candidates
        if len(candidates) > 1:
            # still ambiguous: prefer the tables this very SELECT reads directly
            direct = {f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}" for tb in t.find_all(exp.Table) if tb.catalog and tb.db}
            if len(candidates & direct) == 1:
                candidates &= direct
        return len(candidates) == 1 and self.blame(next(iter(candidates)), col)

    def learn(self, sql_key: str, message: str) -> bool:
        """Apply whatever one error message teaches. True when the spec changed."""
        if self.missing_column(sql_key, message) or self.missing_relation(sql_key, message):
            return True
        for pattern, fixed in _CONVERT:
            m = pattern.search(message)
            if m:
                hint = fixed or _HINTS.get(m.group("type").upper())
                fmt = re.search(r'according to format specifier "([^"]+)"', message)
                return bool(hint) and self.retype(m.group("value"), hint, fmt.group(1) if fmt else None)
        m = _NOT_STRUCT.search(message)
        if m:
            return self.make_variant(m.group("col"), m.group("field"))
        m = _NO_FUNCTION.search(message)
        if m and "VARCHAR" in m.group("types"):
            return self.wrong_type(sql_key, m.group("name").lower())
        return False

    def wrong_type(self, sql_key: str, function: str) -> bool:
        """Snowflake does not read text as a date in DATE_PART or EXTRACT, or as a number in a few
        other places: a text column handed to one of them is really a timestamp or a number."""
        hint = "TIMESTAMP" if function in _TEMPORAL else "DOUBLE" if function in _NUMERIC else None
        tree = self.tree(sql_key)
        if hint is None or tree is None:
            return False
        classes = (exp.Extract, exp.TimeToUnix, exp.UnixToTime) if hint == "TIMESTAMP" else (exp.Func,)
        names = set()
        for node in tree.find_all(*classes):
            if hint == "DOUBLE" and node.sql_name().lower() != function and getattr(node, "name", "").lower() != function:
                continue
            for arg in ([node.expression] if isinstance(node, exp.Extract) else list(node.iter_expressions())):
                if isinstance(arg, exp.Column):
                    names.add(arg.name.upper())
        changed = False
        for name in names:
            for table, t in self.tables.items():
                gen = (t.get("columns") or {}).get(name)
                if gen and gen.get("gen") in _FREE and not gen.get("type") and not gen.get("format"):
                    t["columns"][name] = guess(name, hint)
                    self.retyped.add(table)
                    self.learned.append(f"{table}.{name}: passed to {function}(), which takes no text; retyped as {hint}.")
                    changed = True
        self.dirty |= changed
        return changed

    # --- loop 1: compiled models straight on the emulator

    def load(self, manifest: dict) -> list[str]:
        nodes = self.nodes = manifest["nodes"]
        self.project_relations = {_fq(n["relation_name"]) for n in nodes.values() if n.get("relation_name")}
        self.relation_owner = {_fq(n["relation_name"]): uid for uid, n in nodes.items() if n.get("relation_name")}
        self.uncompiled = {uid: n.get("compile_error") or "" for uid, n in nodes.items()
                           if n["resource_type"] == "model" and not n.get("compiled_code")}
        self.sql_of = {uid: n["compiled_code"].strip().rstrip(";") for uid, n in nodes.items()
                       if n["resource_type"] in ("model", "snapshot") and n.get("compiled_code") and n.get("language", "sql") == "sql"
                       and n.get("relation_name") and n["config"].get("materialized") != "ephemeral"}
        self.trees = {}
        rel = {uid: _fq(nodes[uid]["relation_name"]) for uid in self.sql_of}
        self.by_rel = {fq: uid for uid, fq in rel.items() if fq.count(".") == 2}
        ephemeral = {uid for uid, n in nodes.items() if n["resource_type"] == "model" and n["config"].get("materialized") == "ephemeral"}

        def deps(uid: str, seen: frozenset = frozenset()) -> set[str]:
            out: set[str] = set()
            for d in nodes[uid]["depends_on"]["nodes"]:
                if d in ephemeral and d not in seen:
                    out |= deps(d, seen | {d})
                elif d in self.sql_of:
                    out.add(d)
            return out

        graph = {uid: deps(uid) - {uid} for uid in self.sql_of if rel[uid].count(".") == 2}
        self.parents = graph
        # Models whose Jinja looks at the warehouse while compiling (dbt_utils.star, union_relations,
        # get_column_values, run_query) compile to a placeholder when nothing is there yet. Their real
        # SQL only exists during a build, so they and everything downstream are left to the build.
        macros = manifest.get("macros", {})
        verdict: dict[str, bool] = {}

        def inspects(macro: str, depth: int = 0) -> bool:
            if macro in verdict or macro in _HARMLESS or depth > 12:
                return verdict.get(macro, False)
            verdict[macro] = False
            body = macros.get(macro, {})
            verdict[macro] = bool(_INSPECTS.search(body.get("macro_sql") or "")) or any(
                inspects(m, depth + 1) for m in (body.get("depends_on") or {}).get("macros", []))
            return verdict[macro]

        self.deferred = {uid for uid in graph if _INSPECTS.search(nodes[uid].get("raw_code") or "")
                         or any(inspects(m) for m in nodes[uid]["depends_on"].get("macros", []))}
        try:
            order = list(graphlib.TopologicalSorter(graph).static_order())
        except graphlib.CycleError:
            order = list(graph)
        for uid in order:  # parents come first, so one sweep carries it downstream
            if any(p in self.deferred for p in graph.get(uid, ())):
                self.deferred.add(uid)
        return [uid for uid in order if uid not in self.deferred]

    def hidden_dependencies(self) -> None:
        """A model that names another model's table without ref() depends on it in a way dbt cannot
        see: on a first run the table may not be there yet, and with several threads that is a matter
        of luck. Production has the table from an earlier run; give this warehouse one too."""
        for uid in self.sql_of:
            tree, node = self.tree(uid), self.nodes[uid]
            if tree is None:
                continue
            ctes = {c.alias.upper() for c in tree.find_all(exp.CTE)}
            ancestors = None
            for tb in tree.find_all(exp.Table):
                name = tb.name.upper()
                if not isinstance(tb.this, exp.Identifier) or (not tb.db and name in ctes):
                    continue
                fq = ".".join([(tb.catalog or node["database"]).upper(), (tb.db or node["schema"]).upper(), name])
                owner = self.relation_owner.get(fq)
                if owner is None or fq in self.existing or self.nodes[owner]["resource_type"] not in ("model", "snapshot"):
                    continue
                ancestors = self._ancestors(uid) if ancestors is None else ancestors
                if owner in ancestors:
                    continue
                self.emu.ensure_database(*fq.split(".")[:2])
                self.existing[fq] = {"columns": {c: duck_type(guess(c, None)) for c in self.columns_read(tree, tb)}}
                self.create_existing(fq)
                self.learned.append(f"{fq}: read by {node['name']} by name, not through ref(); starts as an empty relation.")

    def run_model(self, conn, uid: str, fq: str) -> str | None:  # noqa: ANN001
        """Build one model straight on the emulator. Whenever the error names something the spec can
        supply (a column, a relation), supply it and bind again. Returns the error that remains, if any."""
        db, schema, _ = fq.split(".")
        target = f'{db}.{schema}."__FX_BOOT"' if fq in self.existing else fq
        statement = f"create or replace table {target} as (\n{self.sql_of[uid]}\n)"
        fast: list[str] | None = None  # the statement as DuckDB SQL, to retry without transpiling again
        fallbacks = 0
        began = time.time()
        failure = None
        for _ in range(400):
            cur = conn.cursor()
            watchdog = threading.Timer(self.model_timeout, cur._duck_conn.interrupt)
            watchdog.daemon = True
            watchdog.start()
            try:
                if time.time() - began > self.model_timeout:
                    raise TimeoutError
                if fast is None:
                    cur.execute(statement)  # the path dbt takes
                else:
                    for part in fast:
                        cur._duck_conn.execute(part)
                if target != fq:
                    self.bootstrap(fq, target)
                break
            except Exception as e:  # noqa: BLE001
                message = str(e)
                if self.missing_column(uid, message) or self.missing_relation(uid, message):
                    if fast is None:  # the column or relation now exists (empty): bind again
                        try:
                            parsed = sqlglot.parse_one(emulator.rewrite(statement), read="snowflake")
                            fast = [cur._transform(part, None).sql(dialect="duckdb") for part in cur._transform_explode(parsed)]
                        except Exception:  # noqa: BLE001
                            fast = None
                    continue
                if fast is not None and fallbacks < 3:
                    fast, fallbacks = None, fallbacks + 1  # something else: the full path can rewrite the statement
                    continue
                if "INTERRUPT" in message.upper() or isinstance(e, TimeoutError):
                    message = f"stopped after {self.model_timeout:.0f}s: the query runs away on the generated data"
                else:
                    self.learn(uid, message)
                failure = message
                break
            finally:
                watchdog.cancel()
                cur.close()
        if time.time() - began > 20:
            self.log(f"    slow: {self.nodes[uid]['name']} took {time.time() - began:.0f}s")
        return failure

    def learn_from_models(self, manifest: dict, database: str, passes: int = 4) -> dict[str, str]:
        """Returns {model: error} for the models that still fail after the spec stopped changing."""
        order = [uid for uid in self.load(manifest)]
        if self.deferred:
            self.log(f"  {len(self.deferred)} model(s) inspect the warehouse while compiling, or read one that does: left to the build")
        self.hidden_dependencies()
        nodes = manifest["nodes"]
        sources = {uid: _fq(f'{n["database"]}.{n["schema"]}.{n.get("identifier") or n["name"]}')
                   for uid, n in manifest.get("sources", {}).items()}
        failures: dict[str, str] = {}
        todo = set(order)
        for n_pass in range(1, passes + 1):
            self.generate()
            before = len(self.learned)
            self.retyped = set()
            failures = {uid: m for uid, m in failures.items() if uid not in todo}
            conn = self.emu._fs.connect(database=database, schema="CI")
            started, done = time.time(), 0
            for uid in order:
                if uid not in todo:
                    continue
                done += 1
                if done % 250 == 0:
                    self.log(f"    {done}/{len(todo)} models, {time.time() - started:.0f}s")
                fq = _fq(nodes[uid]["relation_name"])
                db, schema, _ = fq.split(".")
                self.emu.ensure_database(db, schema)
                broken = next((p for p in self.parents.get(uid, ()) if p in failures), None)
                if broken:  # its input is wrong, so its errors would teach nothing true
                    failures[uid] = f"upstream failed: {broken}"
                    continue
                if fq in self.stand_ins:  # it compiles now: the stand-in has done its job
                    self.emu.duck.execute(f"drop view if exists {quoted(fq)}")
                    self.stand_ins.discard(fq)
                    self.pending.pop(fq, None)
                error = self.run_model(conn, uid, fq)
                if error:
                    failures[uid] = error
            conn.close()
            self.push_pending()
            roots = sum(1 for m in failures.values() if not m.startswith("upstream failed"))
            self.log(f"  pass {n_pass}: {len(order) - len(failures)}/{len(order)} models run ({roots} fail on their own), "
                     f"{len(self.learned) - before} thing(s) learned")
            if len(self.learned) == before:
                break
            # next pass: what failed, what reads a source whose values changed, and everything downstream of those
            again = set(failures)
            if self.retyped:
                for uid in order:
                    if any(sources.get(dep) in self.retyped for dep in self._ancestors(uid) if dep in sources):
                        again.add(uid)
            for uid in order:  # parents come first in `order`, so one sweep carries it downstream
                if uid not in again and any(p in again for p in self.parents.get(uid, ())):
                    again.add(uid)
            todo = again
        self.save()
        if os.environ.get("FAUXFLAKE_FIT_DEBUG"):  # why each model did not run, for working on the emulator
            Path(os.environ["FAUXFLAKE_FIT_DEBUG"]).write_text(json.dumps(failures, indent=1))
        return failures

    def drop_models(self, manifest: dict) -> None:
        """Remove what loop 1 built, so dbt starts from the state a first run has."""
        for uid in self.sql_of:
            fq = _fq(manifest["nodes"][uid]["relation_name"])
            if fq.count(".") == 2:
                try:
                    self.emu.duck.execute(f"drop table if exists {quoted(fq)}")
                except Exception:  # noqa: BLE001, S110
                    pass

    # --- loop 2: the real dbt build

    def learn_from_build(self, project, analysis, rounds: int = 8) -> dict:  # noqa: ANN001
        """Run `dbt build`, read the results, fix what the spec can fix. Returns the last summary."""
        summary: dict = {}
        selected: list[str] = []
        outcome: dict[str, tuple[str, str, str]] = {}  # node -> (kind, status, message), across rounds
        for n_round in range(1, rounds + 1):
            self.generate()
            before = len(self.learned)
            project.invoke(["build", *selected], silent=True)
            results_path = project.target_path / "run_results.json"
            if not results_path.exists():
                self.log(f"  dbt build did not run: {project.last_error[:400]}")
                return {"error": project.last_error}
            results = json.loads(results_path.read_text())["results"]
            manifest = json.loads((project.target_path / "manifest.json").read_text())
            nodes = self.nodes = manifest["nodes"]
            self.project_relations = {_fq(n["relation_name"]) for n in nodes.values() if n.get("relation_name")}
            self.relation_owner = {_fq(n["relation_name"]): uid for uid, n in nodes.items() if n.get("relation_name")}
            failed = [r for r in results if r["status"] in ("error", "fail")]
            expected = self.spec.setdefault("dbt", {}).setdefault("expected_failures", [])
            accepted = 0
            again: set[str] = set()  # models to build in the next round, with everything downstream
            for r in failed:
                node = nodes.get(r["unique_id"])
                if node is None:
                    continue
                message = r.get("message") or ""
                if node["resource_type"] == "test":
                    # a failing test either teaches the spec something, or states a fact about production
                    # data that invented data does not share: that one goes on the list of expected failures
                    if r["status"] == "fail" and not self.learn_from_test(node, analysis) and node["name"] not in expected:
                        expected.append(node["name"])
                        accepted += 1
                        outcome.pop(r["unique_id"], None)
                        tested = nodes.get(node.get("attached_node") or "") or next(
                            (nodes[d] for d in node["depends_on"]["nodes"] if d in nodes), None)
                        if tested:
                            again.add(tested["name"])  # what dbt skipped because this test failed
                    continue
                again.add(node["name"])
                if "no columns found to union" in message and node.get("relation_name"):
                    self.give_union_columns(_fq(node["relation_name"]), message, self.columns_children_read(node["unique_id"]))
                    continue
                compiled = self._compiled(project, node)
                if compiled and node.get("relation_name") and _fq(node["relation_name"]).count(".") == 2:
                    uid, fq = node["unique_id"], _fq(node["relation_name"])
                    self.sql_of[uid] = compiled
                    self.trees.pop(uid, None)
                    self.by_rel[fq] = uid
                    if not self.learn(uid, message):
                        continue
                    # the error taught something: keep going on this model, as loop 1 does, so that one
                    # round finds every column it is missing instead of one per round
                    conn = self.emu._fs.connect(database=node["database"], schema=node["schema"])
                    try:
                        self.run_model(conn, uid, fq)
                    finally:
                        conn.close()
            for r in results:
                node = nodes.get(r["unique_id"], {})
                if node.get("resource_type") == "test" and node.get("name") in expected:
                    continue
                outcome[r["unique_id"]] = (node.get("resource_type", "other"), r["status"], (r.get("message") or "").strip())
            counts: dict[str, int] = {}
            for kind, status, _ in outcome.values():
                counts[f"{kind}:{status}"] = counts.get(f"{kind}:{status}", 0) + 1
            summary = {"counts": counts, "failed": [(uid, status, message) for uid, (_, status, message) in outcome.items()
                                                    if status in ("error", "fail")]}
            self.log(f"  build {n_round}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                     + f"; {len(self.learned) - before} thing(s) learned")
            if not failed or (len(self.learned) == before and not accepted):
                break
            if len(self.learned) > before or not again or len(again) > 300:
                selected = []  # the data changed: build everything again
            else:
                selected = ["--select", *[f"{name}+" for name in sorted(again)]]  # only what the excluded tests held back
        if not self.spec["dbt"].get("expected_failures"):
            self.spec["dbt"].pop("expected_failures", None)
        self.save()
        return summary

    def _compiled(self, project, node: dict) -> str | None:  # noqa: ANN001
        if node.get("compiled_code"):
            return node["compiled_code"].strip().rstrip(";")
        path = project.target_path / "compiled" / node["package_name"] / node["original_file_path"]
        return path.read_text().strip().rstrip(";") if path.exists() else None

    def learn_from_test(self, node: dict, analysis) -> bool:  # noqa: ANN001
        """A failing generic test states a fact about the data the spec did not know."""
        meta = node.get("test_metadata")
        if not meta or not node.get("attached_node"):
            return False
        kwargs = meta["kwargs"].get("arguments") or meta["kwargs"]
        column = node.get("column_name") or kwargs.get("column_name") or meta["kwargs"].get("column_name")
        here = analysis.locate(node["attached_node"], column) if column and re.fullmatch(r"\w+", str(column)) else None
        if not here or here[0] not in self.tables or here[1] not in self.tables[here[0]].get("columns", {}):
            return False
        table, col = here
        columns = self.tables[table]["columns"]
        gen = columns[col]
        name = meta["name"]
        if name == "unique":
            if gen["gen"] in ("sequence", "hash_id"):
                return False
            numeric = gen["gen"] == "dist" or str(gen.get("type", "")).upper().startswith(("BIGINT", "INT", "DECIMAL", "DOUBLE"))
            if not any(d["gen"] in ("sequence", "hash_id") for d in columns.values()):
                columns[col] = {"gen": "sequence" if numeric else "hash_id"}  # the table had no key: this is it
            elif numeric:
                if gen.get("unique"):
                    return False
                columns[col] = {"gen": "dist", "type": gen.get("type", "BIGINT"), "p": [0, 1], "v": [1, 1000], "unique": True}
            else:
                fresh = {"gen": "template", "template": f"{col.lower()}_{{hex}}"}  # one value per row
                if gen == fresh:
                    return False
                columns[col] = fresh
            self.learned.append(f"{table}.{col}: a `unique` test failed, now generated without repeats.")
        elif name == "not_null":
            if not gen.get("null_rate"):
                return False
            gen.pop("null_rate")
            self.learned.append(f"{table}.{col}: a `not_null` test failed, no longer generated with NULLs.")
        elif name == "accepted_values":
            values = kwargs.get("values") or []
            if not values or gen["gen"] in ("sequence", "hash_id", "fk"):
                return False
            columns[col] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
            self.learned.append(f"{table}.{col}: an `accepted_values` test failed, now drawn from its values.")
        else:
            return False
        self.dirty = True
        return True
