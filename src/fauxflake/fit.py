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

import copy
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
    (re.compile(r'Could not convert string "(?P<value>[^"]*)" to (?P<type>[A-Z0-9]+)'), None),
    (re.compile(r'(?P<type>date|timestamp|time) field (?:format|value out of range): "(?P<value>[^"]*)"'), None),
    (re.compile(r"""Malformed JSON.*?Input: "?(?P<value>[^"\n]*)""", re.S), "VARIANT"),
    (re.compile(r'Expected (?P<type>ARRAY|OBJECT), but got \w+: "?(?P<value>[^"\n]*)"?'), None),
    (re.compile(r'''Could not parse string "(?P<value>[^"]*)" according to format'''), "TIMESTAMP"),
    (re.compile(r"""Failed to cast value: Could not convert string '(?P<value>[^']*)' to (?P<type>[A-Z0-9]+)"""), None),
]
_NO_TABLE = re.compile(r'Table with name "?(?:[\w$]+\.)*(?P<name>[\w$]+)"? does not exist')
_FROM_COLUMN = re.compile(r"when casting from source column \"?([\w$][\w$ ()/.#%&-]*?)\"?\s*(?:$|\n)")
_BAD_CAST = re.compile(r"Unimplemented type for cast \((?P<from>[\w ]+?) -> (?P<to>[\w(), ]+?)\)")
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
# Jinja whose output depends on the columns or the list of relations it finds in the warehouse. (Asking
# whether the model's own table exists, as is_incremental() does, gives the first-run answer and is fine.)
_INSPECTS = re.compile(r"get_columns_in_relation|get_filtered_columns_in_relation|get_relations_by|list_relations|"
                       r"information_schema|get_column_values")
_HARMLESS = {"macro.dbt.is_incremental", "macro.dbt.should_full_refresh"}


def _same_format(written: str, failed: str) -> bool:
    """Is the format a node carries (strftime, or still Snowflake's) the one DuckDB reported?"""
    if written == failed:
        return True
    try:
        from sqlglot.dialects.snowflake import Snowflake

        return sqlglot.time.format_time(written, Snowflake.TIME_MAPPING, Snowflake.TIME_TRIE) == failed
    except Exception:  # noqa: BLE001
        return False


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
        self.source_names: dict[str, str] = {}  # source node -> generated table
        self.uncompiled: dict[str, str] = {}  # model -> why it did not compile
        self.pending: dict[str, dict[str, str]] = {}  # stand-ins for those models: relation -> columns read from it
        self.stand_ins: set[str] = set()
        self.unions: dict[str, list[str]] = {}  # model built by union_relations -> the tables it unions
        self.deferred: set[str] = set()  # models whose SQL only exists during a build
        self.retyped: set[str] = set()  # sources whose generated values changed since the last pass
        self.model_timeout = float(os.environ.get("FAUXFLAKE_FIT_TIMEOUT", "90"))  # seconds one model may take while fitting
        os.environ.setdefault("FAUXFLAKE_STATEMENT_TIMEOUT", str(self.model_timeout))  # the same limit inside the emulator
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

    def generate(self, scale: float | None = None, history: int = 3, only: set[str] | None = None):  # noqa: ANN201
        from .generate import Warehouse
        from .spec import Spec

        self.repair()
        if only is None:
            self.save()
            spec = Spec.load(self.path)
        else:  # a few tables again, mid-pass: no need to write and read the whole file
            spec = Spec.from_raw(copy.deepcopy(self.spec), self.path)
        if scale is None:
            # a few hundred rows per table: enough for filters to let some through, little enough to be quick
            smallest = min((float((t.raw.get("volume") or {}).get("rows_per_day", 1000)) for t in spec.tables.values()), default=1000.0)
            scale = min(1.0, max(1e-5, 100.0 / max(smallest, 1.0)))
        wh = Warehouse(self.emu, spec, scale)
        wh.advance_to(datetime.combine(spec.start, datetime.min.time()) + timedelta(days=history),
                      only={t for t in only if t in spec.tables} if only is not None else None)
        self.dirty = False
        return wh

    def add_column(self, table: str, col: str) -> bool:
        columns = self.tables[table].setdefault("columns", {})
        if col in columns:
            return False
        columns[col] = guess(col, None)
        self.emu.duck.execute(f'alter table {quoted(table)} add column if not exists "{col}" {duck_type(columns[col])}')
        self.learned.append(f"{table}.{col}: read by a model, added.")
        self.retyped.add(table)  # what was built from it (a view over `select *` keeps its old columns) is built again
        return True

    def undeclared_in(self, sql_key: str, database: str) -> bool:
        """Generate the three-part relations of `database` a model names and nothing provides."""
        from . import catalog

        t, node = self.tree(sql_key), self.nodes.get(sql_key)
        if t is None or node is None:
            return False
        changed = False
        for tb in t.find_all(exp.Table):
            if tb.catalog.upper() != database or not tb.db:
                continue
            fq = f"{database}.{tb.db.upper()}.{tb.name.upper()}"
            if fq in self.tables or fq in self.existing or fq in self.project_relations or tb.db.upper() == "INFORMATION_SCHEMA":
                continue
            usual = max((float((x.get("volume") or {}).get("rows_per_day", 0)) for x in self.tables.values()), default=0) or 100_000
            read = [c for c in self.columns_read(t, tb) if c != "_LOADED_AT"]
            known = catalog.view(fq, tuple(read))
            columns = {"_LOADED_AT": {"gen": "loaded_at"}, **{c: guess(c, known.get(c)) for c in dict.fromkeys([*known, *read])}}
            self.tables[fq] = {"volume": {"rows_per_day": int(usual), "weekly": [1] * 7},
                               "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]}, "columns": columns}
            self.retyped.add(fq)
            self.learned.append(f"{fq}: read by {node['name']} but not declared as a source; now generated.")
            changed = True
        self.dirty |= changed
        return changed

    def missing_relation(self, sql_key: str, message: str) -> bool:
        """A model reads a relation that nothing has created. If the project builds it later (a
        hard-coded name instead of ref(), as in a model that looks at its own previous output), it
        starts out as an empty view, like a first run after the table was created by hand. Anything
        else is a source nobody declared: it becomes a generated table."""
        m = _NO_TABLE.search(message)
        vague = re.search(r'Ambiguous reference to catalog or schema "([\w$]+)"', message)
        if vague and not m:
            # `snowflake.account_usage.query_history` when no database SNOWFLAKE exists and schemas of
            # that name do: the relation is one nobody declared, in that database
            return self.undeclared_in(sql_key, vague.group(1).upper())
        t = self.tree(sql_key) if m else None
        node = self.nodes.get(sql_key)
        if t is None or node is None:
            return False
        ctes = {c.alias.upper() for c in t.find_all(exp.CTE)}
        name = m.group("name").upper()
        within = re.search(r'Table with name "?(?:[\w$]+\.)*([\w$]+)\.[\w$]+"? does not exist', message)
        for tb in t.find_all(exp.Table):
            if tb.name.upper() != name or (not tb.db and name in ctes):
                continue
            if within and tb.db and tb.db.upper() != within.group(1).upper():
                continue  # the same name in another schema: not the one DuckDB missed
            fq = ".".join([(tb.catalog or node["database"]).upper(), (tb.db or node["schema"]).upper(), name])
            if fq in self.tables or fq in self.existing or fq.split(".")[1] == "INFORMATION_SCHEMA":
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
                continue  # a ref() to a model that did not build: that model's failure, not a missing table
            db, schema, _ = fq.split(".")
            self.emu.ensure_database(db, schema)
            read = self.columns_read(t, tb)
            if fq in self.project_relations:
                self.existing[fq] = {"columns": {c: duck_type(guess(c, None)) for c in read}}
                self.create_existing(fq)
                self.learned.append(f"{fq}: read by {node['name']} before the project builds it; starts as an empty relation.")
            else:
                usual = max((float((t.get("volume") or {}).get("rows_per_day", 0)) for t in self.tables.values()), default=0) or 100_000
                self.tables[fq] = {"volume": {"rows_per_day": int(usual), "weekly": [1] * 7},
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
        # `cast(ts as date) as day ... group by day`: `day` is the SELECT's own alias, not a column of the table
        own = [p.alias.upper() if isinstance(p, exp.Alias) and not any(
            c.name.upper() == p.alias.upper() for c in p.this.find_all(exp.Column)) else None for p in select.expressions]
        position = {id(p): i for i, p in enumerate(select.expressions)}
        out: list[str] = []
        for col in select.find_all(exp.Column):
            if isinstance(col.this, exp.Star) or col.find_ancestor(exp.Select) is not select:
                continue
            top = col
            while top.parent is not select and top.parent is not None:
                top = top.parent
            # inside a projection only the aliases defined before it are visible; elsewhere all of them
            visible = own[:position[id(top)]] if id(top) in position else own
            if not col.table and col.name.upper() in visible:
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

    def reads_itself_before_it_exists(self, uid: str, fq: str, message: str) -> bool:
        """dbt dropped the stand-in to build the model, and the model's query reads the model. Build it
        once against the stand-in: bootstrap() then records the table later runs start from."""
        if fq not in self.existing or self.existing[fq].get("as") == "table" \
                or f"Table with name {fq.split('.')[2]} does not exist" not in message:
            return False
        self.create_existing(fq)
        conn = self.emu._fs.connect(database=self.nodes[uid]["database"], schema=self.nodes[uid]["schema"])
        try:
            self.run_model(conn, uid, fq)
        finally:
            conn.close()
        return self.existing[fq].get("as") == "table"

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
        self.unions[fq] = [t for tables in by_schema.values() if len(tables) == most for t in tables]
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
            if uid not in node.get("depends_on", {}).get("nodes", []) or node.get("resource_type") != "model":
                continue
            trees = [self.tree(child)]
            try:  # what was compiled before the parent existed can be a placeholder: the Jinja itself says more
                trees.append(sqlglot.parse_one(self.sketch(child), read="snowflake"))
            except Exception:  # noqa: BLE001, S110
                pass
            for tree in trees:
                found: list[str] = []
                for tb in (tree.find_all(exp.Table) if tree is not None else ()):
                    if tb.name.upper() != name:
                        continue
                    found += self.columns_read(tree, tb)
                    select = tb.find_ancestor(exp.Select)
                    cte = select.parent if select is not None and isinstance(select.parent, exp.CTE) else None
                    listed = select is not None and all(isinstance(e, (exp.Star, exp.Column)) for e in select.expressions)
                    if cte is not None and listed and not select.args.get("joins"):  # select *, or dbt_utils.star written out
                        for other in tree.find_all(exp.Table):  # `with t as (select * from it) select a, b from t`
                            if other.name.upper() == cte.alias.upper() and not other.db:
                                found += self.columns_read(tree, other)
                out += [c for c in found if c not in out]
                if found:
                    break
        return out

    def create_existing(self, fq: str) -> None:
        from .generate import existing_view

        kind = self.emu.duck.execute("select table_type from information_schema.tables where table_catalog = ? and "
                                     "table_schema = ? and table_name = ?", fq.split(".")).fetchone()
        if kind is None or kind[0] == "VIEW":  # once the project has built the real thing, leave it alone
            self.emu.duck.execute(existing_view(fq, self.existing[fq].get("columns") or {}, replace=True))

    def sources_of(self, uid: str) -> list[str]:
        """The generated tables a model reads, directly or through the models before it."""
        ancestors = self._ancestors(uid)
        out = [self.source_names[dep] for dep in ancestors if dep in self.source_names]
        for node_id in [uid, *sorted(a for a in ancestors if a.startswith(("model.", "snapshot.")))]:
            tree = self.tree(node_id) if node_id in self.sql_of else None
            if tree is not None:  # and tables it, or a model before it, names without source()
                out += [t for t in self.real_tables(tree, {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}) if t in self.tables]
        return [t for t in dict.fromkeys(out) if t in self.tables]

    def columns_used(self, uid: str, how: str, fmt: str | None = None) -> list[str]:
        """Columns the model's SQL uses in a way that says what they hold, for errors that name no column:
        compared with the empty string, parsed with a date format, or read as an array."""
        out = self._columns_used(self.tree(uid), how, fmt)
        if not out:  # a view upstream may hold the expression that fails when this model reads it
            for dep in self._ancestors(uid):
                if dep.startswith("model.") or dep.startswith("snapshot."):
                    out += self._columns_used(self.tree(dep), how, fmt)
        return list(dict.fromkeys(out))

    @staticmethod
    def _columns_used(tree, how: str, fmt: str | None = None) -> list[str]:  # noqa: ANN001
        out: list[str] = []
        if tree is None:
            return out
        if how == "time_of_day":  # date || ' ' || time, read as a timestamp: the last column is a time of day
            for node in tree.find_all(exp.DPipe, exp.Concat):
                if isinstance(node.parent, (exp.DPipe, exp.Concat)):
                    continue
                # only `date || ' ' || time` handed straight to a timestamp cast or TO_TIMESTAMP: a surrogate
                # key is also a concatenation inside a cast
                reader = node.parent
                while isinstance(reader, exp.Paren):
                    reader = reader.parent
                timed = (isinstance(reader, (exp.Cast, exp.TryCast)) and reader.to.is_type("timestamp", "timestampntz", "timestampltz", "timestamptz", "datetime")) \
                    or isinstance(reader, (exp.StrToTime, exp.TsOrDsToTimestamp)) \
                    or (isinstance(reader, exp.Anonymous) and "to_timestamp" in reader.name.lower())
                operands = [o for o in node.flatten()] if isinstance(node, exp.DPipe) else list(node.expressions)
                columns = [o for o in operands if isinstance(o, exp.Column)]
                spaced = any(isinstance(o, exp.Literal) and o.is_string and o.name in (" ", "T") for o in operands)
                if timed and spaced and len(columns) == 2 and len(operands) == 3:
                    out.append(columns[-1].name.upper())
        elif how == "empty":
            for node in tree.find_all(exp.Nullif, exp.EQ, exp.NEQ):
                a, b = node.this, node.expression
                for col, other in ((a, b), (b, a)):
                    if isinstance(col, exp.Column) and isinstance(other, exp.Literal) and other.is_string and other.name == "":
                        out.append(col.name.upper())
        elif how == "literal":
            for node in tree.find_all(exp.EQ, exp.NEQ, exp.Nullif, exp.In, exp.Like, exp.ILike):
                others = [node.expression] if not isinstance(node, exp.In) else list(node.expressions)
                if isinstance(node.this, exp.Column) and any(isinstance(o, exp.Literal) and o.is_string and o.name == fmt for o in others):
                    out.append(node.this.name.upper())
                elif isinstance(node.expression, exp.Column) and isinstance(node.this, exp.Literal) and node.this.is_string and node.this.name == fmt:
                    out.append(node.expression.name.upper())
        elif how == "format":
            for node in tree.find_all(exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate, exp.TsOrDsToTimestamp):
                arg = node.this
                while isinstance(arg, (exp.Cast, exp.TryCast, exp.Paren, exp.Trim, exp.Nullif, exp.SplitPart, exp.Right, exp.Left)):
                    arg = arg.this
                written = node.args.get("format")
                if isinstance(arg, exp.Column) and isinstance(written, exp.Literal) and (fmt is None or _same_format(written.name, fmt)):
                    out.append(arg.name.upper())
        elif how == "array":
            for node in tree.find_all(exp.Cast, exp.TryCast):
                if node.to.is_type("array") and isinstance(node.this, exp.Column):
                    out.append(node.this.name.upper())
            for node in tree.find_all(exp.Explode, exp.Lateral, exp.Unnest):
                out += [c.name.upper() for c in node.find_all(exp.Column) if c.name.upper() not in ("VALUE", "THIS", "INDEX", "KEY")]
            for node in tree.find_all(exp.Func):  # array_except(col, [...]), array_contains(x, col), arrays_overlap(...)
                if node.sql_name().upper().startswith("ARRAY") or (isinstance(node, exp.Anonymous) and node.name.upper().startswith("ARRAY")):
                    out += [a.name.upper() for a in node.iter_expressions() if isinstance(a, exp.Column)]
        return list(dict.fromkeys(out))

    def bound(self, uid: str, kind: str, top: int) -> bool:
        """Columns the model casts to `kind` hold numbers up to `top`."""
        tree = self.tree(uid)
        target = exp.DataType.build(kind, dialect="snowflake")
        names = [c.name.upper() for cast in (tree.find_all(exp.Cast, exp.TryCast) if tree is not None else [])
                 if cast.to == target for c in cast.this.find_all(exp.Column)]
        changed = False
        for name in dict.fromkeys(n for c in names for n in [c, *self.origins(uid, c)]):
            for table in self.sources_of(uid):
                gen = (self.tables[table].get("columns") or {}).get(name)
                if not gen or gen.get("gen") != "dist" or max(gen.get("v") or [0]) <= top:
                    continue
                points = len(gen.get("p") or [0, 1])
                gen["v"] = [round(top * i / max(points - 1, 1), 4) for i in range(points)]
                gen.setdefault("p", [0, 1])
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: cast to {kind}, bounded at {top}.")
                changed = True
        self.dirty |= changed
        return changed

    def array_values(self, uid: str) -> None:
        """`array_except(skus, ['a', 'b'])`: the array column holds elements of that list."""
        tree = self.tree(uid)
        for node in (tree.find_all(exp.Func) if tree is not None else []):
            args = list(node.iter_expressions())
            lists = [a for a in args if isinstance(a, exp.Array) and a.expressions and all(isinstance(e, exp.Literal) and e.is_string for e in a.expressions)]
            columns = [a for a in args if isinstance(a, exp.Column)]
            if len(lists) != 1 or len(columns) != 1:
                continue
            for name in self.origins(uid, columns[0].name.upper()):
                for table in self.sources_of(uid):
                    gen = (self.tables[table].get("columns") or {}).get(name)
                    if gen and gen.get("gen") == "array" and not gen.get("values"):
                        gen["values"] = [e.name for e in lists[0].expressions]
                        self.retyped.add(table)
                        self.dirty = True

    def split_number(self, uid: str, name: str) -> bool:
        """`split_part(split_part(value, ':', 2), ',', 1) as field_id`, later read as a number: the text
        column holds delimited pieces, and that piece is a number. Generate text of that shape."""
        changed = False
        for node_id in [uid, *sorted(a for a in self._ancestors(uid) if a.startswith(("model.", "snapshot.")))]:
            tree = self.tree(node_id)
            for alias in (tree.find_all(exp.Alias) if tree is not None else []):
                if alias.alias.upper() != name:
                    continue
                arg, template = alias.this, "{n}"
                while isinstance(arg, (exp.Cast, exp.TryCast, exp.Paren, exp.Trim, exp.SplitPart, exp.Replace)):
                    if isinstance(arg, exp.SplitPart):
                        delimiter, part = arg.args.get("delimiter"), arg.args.get("part_index")
                        if not (isinstance(delimiter, exp.Literal) and isinstance(part, exp.Literal) and part.is_int):
                            break
                        template = delimiter.name.join([*["x"] * (int(part.name) - 1), template, "x"])
                    arg = arg.this
                if not isinstance(arg, exp.Column) or template == "{n}":
                    continue
                scope = alias.find_ancestor(exp.Select)
                produced = scope is not None and scope.find(exp.Lateral, exp.TableFromRows, exp.Unnest) is not None \
                    and arg.name.upper() in ("VALUE", "SEQ", "INDEX", "KEY", "PATH", "THIS")
                for table in ([] if produced else self.sources_of(uid)):  # `value` of a table function is no source column
                    columns = self.tables[table].get("columns") or {}
                    column = arg.name.upper()
                    if column in columns and columns[column] != {"gen": "template", "template": template}:
                        columns[column] = {"gen": "template", "template": template}
                        self.retyped.add(table)
                        self.learned.append(f"{table}.{column}: {name} is a piece of it read as a number; generated as {template!r}.")
                        changed = True
                if changed:
                    continue
                # the pieces come from an array written out as text (ARRAY_TO_STRING, then SPLIT_TO_TABLE on
                # '},{'): it is an array of objects whose first field is the number
                shape = {"gen": "array", "fields": {"id": "number", "value": "text"}}
                for written in tree.find_all(exp.ArrayToString):
                    source = written.this
                    for table in (self.sources_of(uid) if isinstance(source, exp.Column) else []):
                        columns = self.tables[table].get("columns") or {}
                        if source.name.upper() in columns and columns[source.name.upper()] != shape:
                            columns[source.name.upper()] = dict(shape)
                            self.retyped.add(table)
                            self.learned.append(f"{table}.{source.name.upper()}: written out and split into fields, one read as a number; "
                                                "generated as an array of {id, value} objects.")
                            changed = True
        self.dirty |= changed
        return changed

    def split_numbers(self, uid: str) -> bool:
        """`split_part(version, '.', 2)::number`: every piece the model reads as a number is one."""
        tree = self.tree(uid)
        shapes: dict[str, tuple[str, int]] = {}
        for cast in (tree.find_all(exp.Cast, exp.TryCast) if tree is not None else []):
            piece = cast.this.unnest()
            if not (cast.to.is_type("int", "bigint", "decimal", "double", "float") and isinstance(piece, exp.SplitPart)
                    and isinstance(piece.this, exp.Column)):
                continue
            delimiter, part = piece.args.get("delimiter"), piece.args.get("part_index")
            if isinstance(delimiter, exp.Literal) and isinstance(part, exp.Literal) and part.is_int and 0 < int(part.name) <= 8:
                column = piece.this.name.upper()
                shapes[column] = (delimiter.name, max(int(part.name), shapes.get(column, ("", 0))[1]))
        changed = False
        for column, (delimiter, count) in shapes.items():
            fresh = {"gen": "template", "template": delimiter.join(["{n}"] * max(count, 2))}
            for table in self.sources_of(uid):
                columns = self.tables[table].get("columns") or {}
                if column in columns and columns[column] != fresh and columns[column].get("gen") in (*_FREE, "sequence", "dist", "hash_id"):
                    if columns[column].get("gen") in ("sequence", "hash_id"):
                        self._unkey(table)  # `version` took for a key: nothing can reference 1.2.3
                    columns[column] = dict(fresh)
                    self.retyped.add(table)
                    self.learned.append(f"{table}.{column}: its pieces are read as numbers; generated as {fresh['template']!r}.")
                    changed = True
        self.dirty |= changed
        return changed

    def split_format(self, uid: str, column: str, fmt: str) -> str:
        """`to_date(split_part(col, ' - ', 2), fmt)`: the column holds several dates in that format,
        joined by the delimiter. The format that makes every part parse."""
        tree = self.tree(uid)
        for node in (tree.find_all(exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate, exp.TsOrDsToTimestamp) if tree is not None else []):
            arg, out = node.this, fmt
            while isinstance(arg, (exp.Cast, exp.TryCast, exp.Paren, exp.Trim, exp.Nullif, exp.SplitPart, exp.Right, exp.Left)):
                if isinstance(arg, exp.SplitPart):
                    delimiter, part = arg.args.get("delimiter"), arg.args.get("part_index")
                    if isinstance(delimiter, exp.Literal) and isinstance(part, exp.Literal) and part.is_int:
                        out = delimiter.name.join([out] * max(2, int(part.name)))
                elif isinstance(arg, exp.Right):
                    out = "run_" + out  # right(task_instance, 8): the date is how the text ends
                elif isinstance(arg, exp.Left):
                    out = out + "_run"
                arg = arg.this
            if isinstance(arg, exp.Column) and arg.name.upper() == column and out != fmt:
                return out
        return fmt

    def retype_column(self, uid: str, name: str, hint: str, fmt: str | None = None) -> bool:
        """The model converts column `name` to `hint`: make the sources it reads generate that."""
        changed = False
        tables = self.sources_of(uid)
        # and the source columns an output column is made from
        names = list(dict.fromkeys([name, *self.origins(uid, name, strict=hint == "DAY")]))
        for table, name in [(t, n) for t in tables for n in names]:
            columns = self.tables[table].get("columns") or {}
            if name not in columns:  # "Sum of Open Opportunities" is sum_of_open_opportunities in generated text
                flat = re.sub(r"\W+", "_", name).strip("_")
                name = next((c for c in columns if re.sub(r"\W+", "_", c).strip("_") == flat), name)
            gen = columns.get(name)
            if gen is None:
                continue
            kind = gen.get("gen")
            numeric = hint in ("BIGINT", "DOUBLE")
            fresh = None
            as_text = str(gen.get("type", "")).upper() in ("VARCHAR", "TEXT", "STRING") and kind in (
                "sequence", "dist", "fk", "time_offset", "event_time", "updated_at", "categorical")
            reads = {"BIGINT": ("sequence", "dist", "fk"), "DOUBLE": ("sequence", "dist", "fk"),
                     "DATE": ("time_offset", "event_time", "updated_at"), "TIMESTAMP": ("time_offset", "event_time", "updated_at"),
                     "EPOCH": ("sequence", "dist"), "BOOLEAN": ("categorical",)}.get(hint)
            if as_text and hint != "TEXT" and not fmt and (reads is None or kind in reads):
                continue  # already holds numbers, dates or booleans as text: a cast reads them
            if kind in ("lifecycle", "loaded_at"):
                continue
            if numeric and kind == "hash_id":
                fresh = {**gen, "gen": "sequence"}  # a text key read as a number: count instead of hashing
            elif numeric and kind == "fk":
                parent = self.tables.get(str(gen.get("ref", "")).upper(), {}).get("columns") or {}
                key = next((c for c, d in parent.items() if d.get("gen") == "hash_id"), None)
                if key is None:
                    continue
                parent[key] = {**parent[key], "gen": "sequence"}  # the reference follows its parent's key
                self.retyped.add(str(gen["ref"]).upper())
                self.learned.append(f"{gen['ref']}.{key}: read as a number through {table}.{name}, now a sequence.")
                changed = True
                continue
            elif numeric and kind in ("sequence", "dist"):
                continue
            elif fmt:
                fresh = {"gen": "time_offset", "format": fmt, "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
            elif hint == "EPOCH":
                if gen.get("epoch"):
                    continue
                fresh = {"gen": "time_offset", "epoch": True, "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
            elif hint == "BOOLEAN":
                if str(gen.get("type", "")).upper() == "BOOLEAN":
                    continue
                fresh = {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.5, False: 0.5}}
            elif hint == "TEXT":
                # the SQL treats it as text (compares it with '', or with a word) and may still cast it:
                # keep what it generates and hold it as text
                if kind in ("hash_id", "template", "pool", "object", "array") or str(gen.get("type", "")).upper() in ("VARCHAR", "TEXT", "STRING"):
                    continue
                if kind == "categorical" and str(gen.get("type", "")).upper() != "BOOLEAN":
                    continue
                fresh = {**gen, "type": "VARCHAR"}
            elif hint == "ARRAY":
                if kind == "array":
                    continue
                fresh = {"gen": "array"}
            elif hint == "DAY":  # `col + 1`: only a DATE takes a number of days
                if str(gen.get("type", "")).upper() == "DATE" or gen.get("format") or gen.get("epoch"):
                    continue
                fresh = {"gen": "time_offset", "type": "DATE", "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
            elif hint in ("DATE", "TIMESTAMP"):
                if kind in ("time_offset", "event_time", "updated_at") and not gen.get("format") and not gen.get("epoch"):
                    continue
                fresh = {"gen": "time_offset", **({"type": "DATE"} if hint == "DATE" else {}), "p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}
            else:
                fresh = guess(name, hint)
                if fresh["gen"] in _FREE:
                    continue
            if kind in ("sequence", "hash_id") and fresh.get("gen") not in ("sequence", "hash_id"):
                self._unkey(table)  # it was the table's key and is not one any more
            if "follows" in gen and fresh.get("gen") != "sequence":
                fresh["follows"] = gen["follows"]
            within_bounds(fresh, self.bounds.get((table, name)))
            if fresh == gen:
                continue
            columns[name] = fresh
            self.retyped.add(table)
            self.learned.append(f"{table}.{name}: used as {fresh.get('format') or hint}, retyped.")
            changed = True
        # relations that start out empty (read before they are built, or stand-ins): the declared type
        wanted = {"TEXT": "VARCHAR", "BIGINT": "BIGINT", "DOUBLE": "DOUBLE", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
                  "TIMESTAMP": "TIMESTAMP", "ARRAY": "JSON", "VARIANT": "JSON", "DAY": "DATE"}.get(hint)
        tree = self.tree(uid)
        if wanted and tree is not None and not changed:
            for fq in self.real_tables(tree, {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}):
                for name in names:
                    if fq in self.existing and (self.existing[fq].get("columns") or {}).get(name) not in (None, wanted):
                        self.existing[fq]["columns"][name] = wanted
                        self.create_existing(fq)
                    elif fq in self.pending and self.pending[fq].get(name) not in (None, wanted):
                        self.pending[fq][name] = wanted
                        self.create_pending(fq)
                    else:
                        continue
                    self.learned.append(f"{fq}.{name}: used as {hint}, declared {wanted}.")
                    changed = True
        self.dirty |= changed
        return changed

    def compared_with_a_hash(self, uid: str, name: str) -> bool:
        """An MD5 would not convert to a number: column `name` holds hashes (a surrogate key), and what the
        model compares it with was generated, or declared, as a number. That side is text too."""
        changed = False
        tree = self.tree(uid)
        own = _fq(self.nodes[uid]["relation_name"]) if (self.nodes.get(uid) or {}).get("relation_name") else None
        relations = self.real_tables(tree, {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}) if tree is not None else set()
        for fq in {*relations, *([own] if own else [])}:  # dbt's merge compares the model's key with the table standing for it
            for held in (self.existing.get(fq, {}).get("columns"), self.pending.get(fq)):
                if held and held.get(name) in ("BIGINT", "DOUBLE"):
                    held[name] = "VARCHAR"
                    self.create_existing(fq) if fq in self.existing else self.create_pending(fq)
                    self.learned.append(f"{fq}.{name}: compared with a hash, declared VARCHAR.")
                    changed = True
        for eq in (tree.find_all(exp.EQ) if tree is not None else []):
            sides = [eq.this.unnest(), eq.expression.unnest()]
            if all(isinstance(x, exp.Column) for x in sides) and any(x.name.upper() == name for x in sides):
                for other in sides:
                    if other.name.upper() != name:
                        changed |= self.retype_column(uid, other.name.upper(), "TEXT")
        return changed

    def _unkey(self, table: str) -> None:
        """A table lost its key column: what referenced it becomes an ordinary column."""
        for other, spec in self.tables.items():
            for col, gen in list((spec.get("columns") or {}).items()):
                if gen.get("gen") == "fk" and str(gen.get("ref", "")).upper() == table:
                    spec["columns"][col] = guess(col, None)
                    self.retyped.add(other)

    def repair(self) -> None:
        """Keep the spec loadable whatever was learned: a reference needs a parent with a key."""
        keyed = {t for t, spec in self.tables.items()
                 if any(d.get("gen") in ("sequence", "hash_id") for d in (spec.get("columns") or {}).values())}
        for table, spec in self.tables.items():
            for col, gen in list((spec.get("columns") or {}).items()):
                if gen.get("gen") == "fk" and str(gen.get("ref", "")).upper() not in keyed:
                    spec["columns"][col] = guess(col, None)

    def retype(self, value: str, hint: str, fmt: str | None = None, only: set[str] | None = None) -> bool:
        """The generated text `value` was converted to `hint`: retype the column(s) that made it.
        Generated text starts with its column's name, which is how the column is found."""
        tokens = value.split()
        changed = False
        for position, token in enumerate(tokens):
            name = token.rsplit("_", 1)[0].upper() if "_" in token else ""
            for table, t in self.tables.items():
                if only and table not in only:
                    continue  # a column of that name in a table the model does not read is another column
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
            box: dict = {}

            def parse() -> None:
                try:
                    box["tree"] = sqlglot.parse_one(self.sql_of[uid] if uid in self.sql_of else self.sketch(uid), read="snowflake")
                except BaseException:  # noqa: BLE001 - also RecursionError on SQL nested thousands deep
                    box["tree"] = None

            # in a thread of its own: generated SQL can nest deeper than the main thread's stack allows
            worker = threading.Thread(target=parse, name="fauxflake-parse")
            worker.start()
            worker.join()
            self.trees[uid] = box.get("tree")
        return self.trees[uid]

    def sketch(self, uid: str) -> str:
        """A model that never compiled still says which columns it reads. Its Jinja, with ref() and
        source() replaced by relation names, star() by `*` and everything else dropped, is close
        enough to SQL to read them from."""
        raw = self.nodes[uid].get("raw_code") or ""
        by_name = {n["name"]: n.get("relation_name") for n in self.nodes.values() if n.get("relation_name")}

        def expression(m: re.Match) -> str:
            inner = m.group(1)
            if re.search(r"\bstar\s*\(", inner):
                return "*"
            ref = re.search(r"\bref\s*\(\s*(?:'[^']*'\s*,\s*)?'([^']+)'", inner)
            if ref and by_name.get(ref.group(1)):
                return by_name[ref.group(1)]
            return "NULL"

        text = re.sub(r"\{#.*?#\}", "", raw, flags=re.S)
        text = re.sub(r"\{%.*?%\}", "", text, flags=re.S)
        return re.sub(r"\{\{(.*?)\}\}", expression, text, flags=re.S)

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
        uid = self.by_rel.get(fq) or self.relation_owner.get(fq)
        if uid and uid not in self.sql_of and (self.nodes.get(uid) or {}).get("compiled_code"):
            self.sql_of[uid] = self.nodes[uid]["compiled_code"].strip().rstrip(";")
        t = self.tree(uid) if uid else None
        if t is None:
            return None
        ctes = {c.alias.upper(): c.this for c in t.find_all(exp.CTE)}
        tables = self.real_tables(t, ctes)
        if not t.find(exp.Star):
            # a macro wrote out the columns it found (`select "A", "B" from source`): still everything of that table
            top = t.unnest()
            listed = isinstance(top, exp.Select) and top.expressions and all(
                isinstance(e, exp.Column) or (isinstance(e, exp.Alias) and isinstance(e.this, exp.Column) and e.alias.upper() == e.this.name.upper())
                for e in top.expressions) and not top.args.get("joins") and not top.args.get("group")
            return next(iter(tables)) if listed and len(tables) == 1 else None
        # `select *` all the way down to one relation. A CTE that picks or renames columns
        # (`select email, optedout as opted_out from t`) is a projection, whatever `select *` follows it
        return self._star_owner(t, ctes)

    def _star_owner(self, node: exp.Expression, ctes: dict[str, exp.Expression]) -> str | None:
        """`select s.*, p.sku from a s join b p ...`: the columns nobody lists are those of `s`."""
        for _ in range(8):
            node = node.unnest()
            while isinstance(node, exp.SetOperation):  # the same table stacked on itself
                node = node.left.unnest()
            if not isinstance(node, exp.Select) or node.args.get("from_") is None:
                return None
            stars = [e for e in node.expressions if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))]
            listed = len(node.expressions) > 2 and not node.args.get("joins") and not node.args.get("group") and all(
                isinstance(e, exp.Column) or (isinstance(e, exp.Alias) and isinstance(e.this, exp.Column) and e.alias.upper() == e.this.name.upper())
                for e in node.expressions)  # dbt_utils.star written out
            if not stars and not listed:
                return None
            owner = next((e.table.upper() for e in stars if isinstance(e, exp.Column) and e.table), None)
            relations = [node.args["from_"].this, *[j.this for j in node.args.get("joins") or []]]
            chosen = next((r for r in relations if owner and r.alias_or_name.upper() == owner), None) if owner else relations[0]
            if isinstance(chosen, exp.Subquery):
                node = chosen.this
            elif isinstance(chosen, exp.Table) and not chosen.db and chosen.name.upper() in ctes:
                node = ctes[chosen.name.upper()]
            elif isinstance(chosen, exp.Table) and chosen.catalog and chosen.db:
                return f"{chosen.catalog.upper()}.{chosen.db.upper()}.{chosen.name.upper()}"
            else:
                return None
        return None

    def _union_members(self, fq: str) -> bool:
        """Is `fq` a model that only stacks generated tables (dbt_utils.union_relations, directly or
        through a macro of the project)? Remember which."""
        if fq in self.unions:
            return True
        uid = self.by_rel.get(fq) or self.relation_owner.get(fq)
        if not uid or uid not in self.nodes:
            return False
        if uid not in self.sql_of and self.nodes[uid].get("compiled_code"):
            self.sql_of[uid] = self.nodes[uid]["compiled_code"].strip().rstrip(";")
        tree = self.tree(uid)
        top = tree.unnest() if tree is not None else None
        if not isinstance(top, exp.SetOperation):
            return False
        members: list[str] = []
        ctes = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}
        pile = [top]
        while pile:
            node = pile.pop().unnest()
            if isinstance(node, exp.SetOperation):
                pile += [node.left, node.right]
                continue
            source = node.args.get("from_") if isinstance(node, exp.Select) and not node.args.get("joins") else None
            behind = self.real_tables(source, ctes) if source is not None else set()  # also through `with a as (select ... from t)`
            if len(behind) != 1:
                return False
            members.append(next(iter(behind)))
        members = [m for m in dict.fromkeys(members) if m in self.tables or ((self.by_rel.get(m) or self.relation_owner.get(m)) and m != fq)]
        if not members:
            return False
        self.unions[fq] = members
        return True

    def blameable(self, fq: str, depth: int = 0) -> bool:
        if fq in self.tables or fq in self.existing or fq in self.pending or self._union_members(fq):
            return True
        parent = self.passthrough(fq) if depth <= 8 else None
        return bool(parent) and self.blameable(parent, depth + 1)

    def blame(self, fq: str, col: str, depth: int = 0) -> bool:
        if fq in self.tables:
            return self.add_column(fq, col)
        if self._union_members(fq):  # a model that unions tables by a column list: the tables need the column
            # and the other columns its children read, in one go: each round of a build shows only one
            owner = self.by_rel.get(fq) or self.relation_owner.get(fq)
            wanted = [col, *[c for c in (self.columns_children_read(owner) if owner in self.nodes else []) if c != col]]
            added = 0
            for name in wanted:
                for table in self.unions[fq]:
                    if table not in self.tables:  # a model stacked with others: whatever it is made of
                        if depth <= 8 and name == col and self.blameable(table, depth + 1) and self.blame(table, name, depth + 1):
                            added += 1
                        continue
                    if name not in self.tables[table].setdefault("columns", {}):
                        self.tables[table]["columns"][name] = guess(name, None)
                        self.retyped.add(table)
                        added += 1
            if added:
                self.learned.append(f"{fq}: {', '.join(wanted[:6])}{' ...' if len(wanted) > 6 else ''} read from it, "
                                    f"added to the {len(self.unions[fq])} tables it unions.")
                self.dirty = True
            return bool(added)
        if fq in self.pending:
            if col in self.pending[fq]:
                return False
            self.pending[fq][col] = duck_type(guess(col, None))
            self.create_pending(fq)
            return True
        if fq in self.existing:
            columns = self.existing[fq].setdefault("columns", {})
            added = col not in columns
            if added:
                columns[col] = duck_type(guess(col, None))
                self.create_existing(fq)
            # the stand-in is replaced when the model builds: what the model selects from needs it too
            parent = self.passthrough(fq) if depth <= 8 else None
            if parent and parent != fq and self.blameable(parent, depth + 1) and self.blame(parent, col, depth + 1):
                added = True
            return added
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
            for c in t.find_all(exp.Column):  # `s` can name a different table in each CTE: take the one in scope
                select = c.find_ancestor(exp.Select)
                if c.name.upper() != col or c.table.upper() != alias or select is None or select.args.get("from_") is None:
                    continue
                for rel in [select.args["from_"].this, *[j.this for j in select.args.get("joins") or []]]:
                    if rel.alias_or_name.upper() != alias:
                        continue
                    if isinstance(rel, exp.Table) and rel.catalog and rel.db:
                        candidates.add(f"{rel.catalog.upper()}.{rel.db.upper()}.{rel.name.upper()}")
                    elif isinstance(rel, exp.Table) and rel.name.upper() in ctes:
                        owner = self._star_owner(ctes[rel.name.upper()], ctes)
                        candidates |= {owner} if owner else self.real_tables(ctes[rel.name.upper()], ctes)
                    elif isinstance(rel, exp.Subquery):
                        owner = self._star_owner(rel.this, ctes)
                        candidates |= {owner} if owner else self.real_tables(rel.this, ctes)
        if alias and not candidates:
            for tb in t.find_all(exp.Table):
                if tb.alias_or_name.upper() != alias:
                    continue
                if not tb.db and tb.name.upper() in ctes:
                    candidates |= self.real_tables(ctes[tb.name.upper()], ctes)
                elif tb.catalog and tb.db:
                    candidates.add(f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}")
            if not candidates and alias in ctes:
                candidates = self.real_tables(ctes[alias], ctes)
            for sub in t.find_all(exp.Subquery):  # `(select * from t where ...) so ... so.col`
                if not candidates and sub.alias.upper() == alias:
                    candidates = self.real_tables(sub.this, ctes)
            if alias in ("DBT_INTERNAL_SOURCE", "DBT_INTERNAL_DEST"):
                # dbt's own merge on the model's unique key: what the model selects lacks the key, and so
                # does the empty table that stands for the model before its first build
                owners = {c for c in self.real_tables(t, ctes) if self.blameable(c)}
                return any([self.blame(c, col) for c in sorted(owners)])
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
        if len(candidates) > 1 and not alias:
            # read bare in a SELECT with one table and no join: that table's, whatever else the model reads
            certain: set[str] = set()
            for c in t.find_all(exp.Column):
                select = c.find_ancestor(exp.Select)
                if c.table or c.name.upper() != col or select is None or select.args.get("from_") is None or select.args.get("joins"):
                    continue
                certain |= self.real_tables(select.args["from_"], ctes) & candidates
            if certain:
                return any([self.blame(c, col) for c in sorted(certain)])
        if len(candidates) > 1 and not alias:
            # the model names the column with a table elsewhere (`opp.end_date`): that table owns it
            owners: set[str] = set()
            for c in t.find_all(exp.Column):
                if c.name.upper() != col or not c.table:
                    continue
                for tb in t.find_all(exp.Table):
                    if tb.alias_or_name.upper() != c.table.upper():
                        continue
                    owners |= {f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}"} if tb.catalog and tb.db \
                        else self.real_tables(ctes.get(tb.name.upper(), tb), ctes)
            if len(owners & candidates) == 1:
                candidates &= owners
        if len(candidates) > 1 and not alias:
            # an unqualified column belongs to the table in its SELECT's FROM
            mains: set[str] = set()
            for c in t.find_all(exp.Column):
                select = c.find_ancestor(exp.Select)
                source = select.args.get("from_") if select is not None and not c.table and c.name.upper() == col else None
                if source is not None:  # not a table joined to it, nor one a subquery in its WHERE reads
                    mains |= self.real_tables(source, ctes)
            if len(candidates & mains) == 1:
                candidates &= mains
            else:
                # `where updated_at >= (select max(updated_at) from <its own table>)`: each SELECT reads
                # the column from its own FROM table, and each of those tables lacks it
                singles: set[str] = set()
                for c in t.find_all(exp.Column):
                    select = c.find_ancestor(exp.Select)
                    source = select.args.get("from_") if select is not None and not c.table and c.name.upper() == col else None
                    if source is not None and not select.args.get("joins"):
                        owners = self.real_tables(source, ctes) & candidates
                        singles |= owners if len(owners) == 1 else set()
                if len(singles) > 1:
                    return any([self.blame(c, col) for c in sorted(singles)])
        if len(candidates) > 1 and not alias:
            # `left join mapping on form_url_clean = bizible_url`: a bare column in a join's ON that no relation
            # has yet is the joined relation's, the one the condition is there to bring in
            joined: set[str] = set()
            for c in t.find_all(exp.Column):
                join = c.find_ancestor(exp.Join)
                on = join.args.get("on") if join is not None and not c.table and c.name.upper() == col else None
                if on is not None and any(x is c for x in on.find_all(exp.Column)):
                    joined |= self.real_tables(join.this, ctes) & candidates
            if len(joined) == 1:
                candidates = joined
        if len(candidates) > 1 and not alias:
            # still several: the relation whose name shares the most words with the column's
            # (`pathfactory_content_type` and `..._to_pathfactory_mapping`)
            words = {w for w in re.split(r"[\W_]+", col.lower()) if len(w) > 2}
            score = {c: len(words & set(re.split(r"[\W_]+", c.split(".")[-1].lower()))) for c in candidates}
            best = max(score.values())
            if best and list(score.values()).count(best) == 1:
                candidates = {c for c, n in score.items() if n == best}
        if len(candidates) > 1 and self._unioned(t, ctes, candidates):
            # `select * from a union all select * from b`: the branches line up, so every one of them has it
            return any([self.blame(c, col) for c in sorted(candidates)])
        if len(candidates) != 1:
            return False
        owner = next(iter(candidates))
        if not self.blame(owner, col):
            return False
        if owner in self.tables:
            # one error names one column; the model usually reads many more from the same table
            # (a wide config table, read with `select *`): add them all now, not one run at a time
            for name in [*self.read_through(t, owner), *self.read_past_a_listing(t, ctes, owner, col)]:
                if name != col and re.fullmatch(r"[\w$ ]+", name):
                    self.add_column(owner, name)
        return True

    def read_past_a_listing(self, tree: exp.Expression, ctes: dict[str, exp.Expression], owner: str, col: str) -> list[str]:
        """`with d as (select "A", "B", max(t) as m from T group by "A", "B") select a, b, c, e from d`: the CTE
        was written out from the columns T had when the model compiled, so the SELECT that reads `c` from it
        reads its other missing columns from T too. All of them now, not one compile at a time."""
        out: list[str] = []
        for c in tree.find_all(exp.Column):
            select = c.find_ancestor(exp.Select)
            if c.table or c.name.upper() != col or select is None or select.args.get("joins") or select.args.get("from_") is None:
                continue
            source = select.args["from_"].this
            inner = ctes.get(source.name.upper()) if isinstance(source, exp.Table) and not source.db else None
            inner = inner.unnest() if inner is not None else None
            if not isinstance(inner, exp.Select) or inner.find(exp.Star) or self.real_tables(inner, ctes) != {owner}:
                continue
            given = {e.alias_or_name.upper() for e in inner.expressions}
            own = {e.alias.upper() for e in select.expressions if isinstance(e, exp.Alias)}
            for other in select.find_all(exp.Column):
                name = other.name.upper()
                if not other.table and other.find_ancestor(exp.Select) is select and name not in given and name not in out \
                        and (name not in own or any(isinstance(e, exp.Alias) and e.alias.upper() == name and any(
                            x.name.upper() == name for x in e.this.find_all(exp.Column)) for e in select.expressions)):
                    out.append(name)
        return out

    def read_through(self, tree: exp.Expression, fq: str) -> list[str]:
        """Columns a query certainly reads from table `fq`: qualified with its alias, or bare where it
        is the only table, also through a CTE or subquery that is a `select *` of it."""
        out: list[str] = []
        for tb in tree.find_all(exp.Table):
            if not (tb.catalog and tb.db) or f"{tb.catalog.upper()}.{tb.db.upper()}.{tb.name.upper()}" != fq:
                continue
            out += self.columns_read(tree, tb)
            select = tb.find_ancestor(exp.Select)
            starred = select is not None and any(
                isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)
                                            and (not e.table or e.table.upper() == tb.alias_or_name.upper()))
                for e in select.expressions)
            holder = select.parent if starred else None
            name = holder.alias.upper() if isinstance(holder, (exp.CTE, exp.Subquery)) and holder.alias else None
            if not name:
                continue
            if isinstance(holder, exp.CTE):
                for other in tree.find_all(exp.Table):
                    if other.name.upper() == name and not other.db:
                        out += self.columns_read(tree, other)
            else:
                outer = holder.find_ancestor(exp.Select)
                out += [c.name.upper() for c in (outer.find_all(exp.Column) if outer is not None else [])
                        if c.table and c.table.upper() == name and not isinstance(c.this, exp.Star)]
        return list(dict.fromkeys(out))

    def _unioned(self, tree: exp.Expression, ctes: dict[str, exp.Expression], candidates: set[str]) -> bool:
        """Are these tables the branches of one UNION of `select *`s?"""
        for union in tree.find_all(exp.SetOperation):
            if isinstance(union.parent, exp.SetOperation):
                continue
            members: set[str] = set()
            branches = [union]
            while branches:
                node = branches.pop().unnest()
                if isinstance(node, exp.SetOperation):
                    branches += [node.left, node.right]
                elif isinstance(node, exp.Select) and node.find(exp.Star):
                    members |= self.real_tables(node, ctes)
                else:
                    members = set()
                    break
            if candidates <= members and members:
                return True
        return False

    def _source_behind(self, fq: str, depth: int = 0) -> str | None:
        """The generated table a relation is a `select *` of, however many models lie between."""
        if fq in self.tables:
            return fq
        parent = self.passthrough(fq) if depth <= 8 else None
        return self._source_behind(parent, depth + 1) if parent else None

    def _columns_of(self, fq: str) -> list[str]:
        db, schema, name = fq.split(".")
        rows = self.emu.duck.execute("select column_name from information_schema.columns where table_catalog = ? and table_schema = ? "
                                     "and table_name = ? order by ordinal_position", [db, schema, name]).fetchall()
        return [r[0] for r in rows]

    def union_widths(self, sql_key: str, message: str) -> bool:
        """`select * from a union all select * from b` with branches of different widths: a branch that
        is a source read with `select *` (its columns were never stated) has the other branch's columns."""
        tree = self.tree(sql_key)
        if "same number of result columns" not in message or tree is None:
            return False
        ctes = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}
        changed = False
        for union in tree.find_all(exp.SetOperation):
            if isinstance(union.parent, exp.SetOperation):
                continue
            relations: list[str] = []
            pile = [union]
            while pile:
                node = pile.pop().unnest()
                if isinstance(node, exp.SetOperation):
                    pile += [node.left, node.right]
                    continue
                plain = isinstance(node, exp.Select) and node.expressions and all(
                    isinstance(e, (exp.Star, exp.Column)) for e in node.expressions)  # `select *`, or dbt_utils.star written out
                source = node.args.get("from_") if plain and not node.args.get("joins") else None
                tables = self.real_tables(source, ctes) if source is not None else set()  # not what a WHERE subquery reads
                if len(tables) != 1:
                    relations = []
                    break
                relations.append(next(iter(tables)))
            shapes = {r: self._columns_of(r) for r in relations}
            fixed = [cols for r, cols in shapes.items() if cols and self._source_behind(r) is None]
            target = fixed[0] if fixed else max(shapes.values(), key=len, default=[])
            for relation, cols in shapes.items():
                source = self._source_behind(relation)
                if source is None or cols == target or not target:
                    continue
                old = self.tables[source].get("columns") or {}
                extra = [] if fixed else [c for c in old if c not in target and old[c].get("gen") != "loaded_at"]
                fresh = {c: old.get(c) or guess(c, None) for c in [*target, *extra]}
                if list(fresh) == list(old):
                    continue
                self.tables[source]["columns"] = fresh
                self.retyped.add(source)
                self.learned.append(f"{source}: unioned with {len(target)} named columns, given the same ones.")
                changed = True
        self.dirty |= changed
        return changed

    def origins(self, uid: str, name: str, depth: int = 0, strict: bool = False) -> list[str]:
        """Source column names behind an output column: follows `x as name`, and the bare columns a
        CASE or COALESCE falls back to, through the models upstream."""
        out = [name]
        if depth > 4:
            return out
        for node_id in [uid, *sorted(self._ancestors(uid))]:
            tree = self.tree(node_id) if node_id == uid or node_id.startswith(("model.", "snapshot.")) else None
            if tree is None:
                continue
            for alias in tree.find_all(exp.Alias):
                if alias.alias.upper() != name:
                    continue
                inner = alias.this
                if strict:  # only the columns whose value it is: through casts, min/max and coalesce
                    found = [c for c in self._cast_reads(inner.this if isinstance(inner, (exp.Max, exp.Min, exp.AnyValue)) else inner)
                             if isinstance(c, exp.Column)] if not inner.find(exp.Add, exp.Sub, exp.Mul, exp.Div) else []
                    for col in found:
                        if col.name.upper() != name:
                            out += self.origins(node_id, col.name.upper(), depth + 1, strict=True)
                    continue
                found = [inner] if isinstance(inner, exp.Column) else []
                if isinstance(inner, exp.Case):  # what the branches give, not what they test
                    for e in [*(i.args.get("true") for i in inner.args.get("ifs") or []), inner.args.get("default")]:
                        found += list(e.find_all(exp.Column))[:2] if e is not None else []
                elif not found and isinstance(inner, (exp.Max, exp.Min, exp.AnyValue)) and isinstance(inner.this, exp.Column):
                    found.append(inner.this)  # max(ts) as max_ts: the same kind of value
                elif not found and not inner.find(exp.Window, exp.AggFunc, exp.Subquery):
                    # a cast, a concatenation, a function of one or two columns: `concat(period, '01') as period_id`
                    columns = list(inner.find_all(exp.Column))
                    found += columns if len({c.name.upper() for c in columns}) <= 2 else []
                for col in found:
                    if col.name.upper() != name:
                        out += self.origins(node_id, col.name.upper(), depth + 1)
        return list(dict.fromkeys(out))

    def udf_input(self, sql_key: str, message: str) -> bool:
        """A Python function of the project raised on generated text. What it failed to decode says
        what the column holds: base64 of JSON, or JSON."""
        if "Python exception occurred while executing the UDF" not in message:
            return False
        shape = None
        if re.search(r"Incorrect padding|base64|Invalid base64", message):
            value = "e30="  # base64 of {}
        elif re.search(r"Expecting value|JSONDecodeError|Extra data", message):
            value = "{}"
        elif re.search(r"Symbol\.iterator|is not iterable", message):
            # `for (const x of V)` in a JavaScript function: the argument is an array, here of {key, value}
            value, shape = "an array", {"gen": "array", "fields": {"key": "text", "value": "text"}}
        else:
            return False
        from . import objects

        state = self.emu._fs.results_cache.get(objects._KEY)
        functions = set(state.functions) if state else set()
        names: list[str] = []
        fields: list[tuple[str, str]] = []
        for node_id in [sql_key, *sorted(self._ancestors(sql_key))]:
            tree = self.tree(node_id) if node_id == sql_key or node_id.startswith(("model.", "snapshot.")) else None
            for call in (tree.find_all(exp.Anonymous) if tree is not None else []):
                if call.name.upper() in functions or call.name.lower() in functions:
                    for arg in call.expressions:
                        inner = arg.unnest()
                        while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                            inner = inner.this
                        key = None
                        if isinstance(inner, exp.Bracket) and isinstance(inner.this, exp.Column) and len(inner.expressions) == 1 \
                                and isinstance(inner.expressions[0], exp.Literal) and inner.expressions[0].is_string:
                            key = inner.expressions[0].name
                        elif isinstance(inner, exp.JSONExtract) and isinstance(inner.this, exp.Column) \
                                and isinstance(inner.expression, exp.JSONPath):
                            keys = [str(k.this) for k in inner.expression.expressions if isinstance(k, exp.JSONPathKey)]
                            key = keys[0] if len(keys) == 1 else None
                        if key and shape:  # `f(value['event_params'])`: that field of the object is what the function reads
                            fields += [(n, key) for n in self.origins(node_id, inner.this.name.upper())]
                            continue
                        names += [n for c in arg.find_all(exp.Column) for n in self.origins(node_id, c.name.upper())]
        changed = False
        for name, key in dict.fromkeys(fields):
            for table in self.sources_of(sql_key):
                columns = self.tables[table].get("columns") or {}
                gen = columns.get(name)
                if gen is None or gen.get("gen") not in (*_FREE, "object", "array"):
                    continue
                held = dict(gen.get("fields") or {}) if gen.get("gen") == "object" else {}
                if held.get(key) == shape:
                    continue
                held[key] = dict(shape)
                columns[name] = {**(gen if gen.get("gen") == "object" else {}), "gen": "object", "fields": held}
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: a function of the project reads its field {key!r} as an array; generated as one.")
                changed = True
        for name in dict.fromkeys(names):
            for table in self.sources_of(sql_key):
                gen = (self.tables[table].get("columns") or {}).get(name)
                if gen is None or gen.get("gen") not in (_FREE if shape is None else (*_FREE, "object", "array")) or gen == shape:
                    continue
                self.tables[table]["columns"][name] = dict(shape) if shape else {"gen": "constant", "value": value}
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: a function of the project decodes it; holds {value!r} instead of made-up text.")
                changed = True
        self.dirty |= changed
        return changed

    def pivot_values(self, sql_key: str, message: str) -> bool:
        """`dbt_utils.pivot('role', get_column_values(...), prefix='count_', suffix='_daily')` makes one
        column per value found, and the model then names some of them (count_admin_daily): those
        are values the column must hold."""
        m = next((m for m in (p.search(message) for p in _MISSING) if m), None)
        raw = self.nodes.get(sql_key, {}).get("raw_code") or ""
        if not m or "pivot(" not in raw:
            return False
        compiled = self.sql_of.get(sql_key) or ""
        changed = False
        for call in re.finditer(r"pivot\(\s*['\"](\w+)['\"](.*?)\)\s*\}\}", raw, re.S):
            column, rest = call.group(1).upper(), call.group(2)
            prefix = re.search(r"prefix\s*=\s*['\"]([^'\"]*)['\"]", rest)
            suffix = re.search(r"suffix\s*=\s*['\"]([^'\"]*)['\"]", rest)
            prefix, suffix = (prefix.group(1) if prefix else ""), (suffix.group(1) if suffix else "")
            holder = (m.groupdict().get("table") or "").upper()
            tree = self.tree(sql_key)
            sources = self.sources_of(sql_key)
            if not prefix and not suffix:
                # no prefix to tell the pivoted columns by: they are what the model reads from the CTE that
                # pivots (`stages_pivoted.application_review`) and that CTE does not otherwise select
                cte = next((c.this.unnest() for c in tree.find_all(exp.CTE) if c.alias.upper() == holder), None) if tree is not None and holder else None
                if not isinstance(cte, exp.Select):
                    continue
                own = {e.alias_or_name.upper() for e in cte.expressions}
                named = [c.name for c in tree.find_all(exp.Column) if c.table.upper() == holder and c.name.upper() not in own]
                looked = re.search(r"get_column_values\(\s*ref\(\s*['\"](\w+)['\"]", rest)
                where = next((uid for uid, n in self.nodes.items() if looked and n.get("name") == looked.group(1)
                              and n.get("resource_type") == "model"), None)
                sources = self.sources_of(where) if where else sources  # the model whose values were pivoted
            else:
                named = re.findall(r"(?i)\.\s*" + re.escape(prefix) + r"(\w+?)" + re.escape(suffix) + r"\b", compiled)
            values = [v.lower() for v in dict.fromkeys(named) if not re.search(r"_[0-9a-f]{12}$", v)]
            if not values:
                continue
            for table, name in [(t, n) for t in sources for n in self.origins(sql_key, column)]:
                columns = self.tables[table].get("columns") or {}
                gen = columns.get(name)
                if gen is None or (gen.get("gen") == "categorical" and set(values) <= {str(v) for v in gen.get("values") or {}}):
                    continue
                columns[name] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: pivoted into columns the model names; holds {', '.join(values)}.")
                changed = True
        self.dirty |= changed
        return changed

    def looked_for(self, sql_key: str) -> bool:
        """The model's Jinja asked the warehouse which tables a schema holds (and for which names),
        found none, and compiled to nothing. Give it the tables it asked for by name."""
        name = self.nodes.get(sql_key, {}).get("name")
        if not name:
            return False
        changed = False
        for command in [c for c in emulator.EMPTY_LOOKUPS if f'"node_id": "{sql_key}"' in c or f".{name}\"" in c]:
            try:
                tree = sqlglot.parse_one(emulator.objects._TRAILING_COMMENT.sub("", command), read="snowflake")
            except Exception:  # noqa: BLE001
                continue
            for select in tree.find_all(exp.Select):
                source = select.args.get("from_")
                table = source.this if source is not None else None
                if not isinstance(table, exp.Table) or table.db.upper() != "INFORMATION_SCHEMA" or select.args.get("where") is None:
                    continue
                database = (table.catalog or self.nodes[sql_key].get("database") or "").upper()
                wanted: dict[str, list[str]] = {"TABLE_SCHEMA": [], "TABLE_NAME": []}
                excluded: set[str] = set()
                for test in select.args["where"].find_all(exp.EQ, exp.In, exp.ILike, exp.Like):
                    if test.find_ancestor(exp.Not) is not None:
                        column = test.this.this if isinstance(test.this, (exp.Lower, exp.Upper)) else test.this
                        if isinstance(test, exp.In) and isinstance(column, exp.Column) and column.name.upper() == "TABLE_NAME":
                            excluded |= {v.name.upper() for v in test.expressions if isinstance(v, exp.Literal)}
                        continue
                    column = test.this.this if isinstance(test.this, (exp.Lower, exp.Upper)) else test.this
                    if not isinstance(column, exp.Column) or column.name.upper() not in wanted:
                        continue
                    values = test.expressions if isinstance(test, exp.In) else [test.expression]
                    wanted[column.name.upper()] += [v.name.upper() for v in values
                                                    if isinstance(v, exp.Literal) and v.is_string and "%" not in v.name]
                for schema in wanted["TABLE_SCHEMA"]:
                    names = wanted["TABLE_NAME"]
                    held = [t for t in self.tables if t.startswith(f"{database}.{schema}.") and t.rsplit(".", 1)[1] not in excluded]
                    if not names and database and not held:
                        names = ["EVENT"]  # it takes whatever tables the schema holds: one stands for them
                    for found in names:
                        fq = f"{database}.{schema}.{found}"
                        if not database or fq in self.tables or not re.fullmatch(r"[\w$]+", schema + found):
                            continue
                        if fq in self.project_relations or fq in self.existing:
                            continue  # it asked whether its own table is there yet: the project builds that one
                        usual = max((float((t.get("volume") or {}).get("rows_per_day", 0)) for t in self.tables.values()), default=0) or 100_000
                        columns = {"_LOADED_AT": {"gen": "loaded_at"},
                                   **{c: guess(c, None) for c in self.columns_children_read(sql_key) if c != "_LOADED_AT"}}
                        self.tables[fq] = {"volume": {"rows_per_day": int(usual), "weekly": [1] * 7},
                                           "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]}, "columns": columns}
                        self.retyped.add(fq)
                        self.learned.append(f"{fq}: {name} looks for it in INFORMATION_SCHEMA and found nothing; now generated.")
                        changed = True
        self.dirty |= changed
        return changed

    def jinja_columns(self, sql_key: str, message: str) -> bool:
        """The model's SQL does not parse: a macro wrote it from the columns it found in the tables it
        reads (`if 'TYPE' in columns ... type not in (...)`), and they have none of them yet. Give those
        tables the columns the macro tests for by name and the ones the models downstream read."""
        if not re.search(r"Parser Error|syntax error|Expected |Required keyword|Invalid expression", message):
            return False
        code = (self.sql_of.get(sql_key) or self.nodes.get(sql_key, {}).get("compiled_code") or "").upper().replace('"', "")
        tables = [t for t in self.tables if t in code or t.split(".", 1)[1] in code]
        if not tables:
            return False
        macros, pile = {}, list(self.nodes.get(sql_key, {}).get("depends_on", {}).get("macros", []))
        while pile and len(macros) < 60:
            uid = pile.pop()
            macro = getattr(self, "macros", {}).get(uid)
            if macro is None or uid in macros or uid.startswith(("macro.dbt.", "macro.dbt_snowflake.")):
                continue
            macros[uid] = macro.get("macro_sql") or ""
            pile += macro.get("depends_on", {}).get("macros", [])
        tested = [c for text in macros.values() for c in re.findall(r"'([A-Z][A-Z0-9_]*)'\s+(?:not\s+)?in\s+[\w.\[\]]*column", text)]
        wanted = list(dict.fromkeys([*tested, *self.columns_children_read(sql_key)]))
        changed = False
        for table in tables:
            columns = self.tables[table].setdefault("columns", {})
            for name in wanted:
                if name not in columns:
                    columns[name] = guess(name, None)
                    self.retyped.add(table)
                    changed = True
        if changed:
            self.learned.append(f"{self.nodes[sql_key]['name']}: its SQL is written from the columns of {len(tables)} table(s) and did not "
                                f"parse; gave them {', '.join(wanted[:8])}{' ...' if len(wanted) > 8 else ''}.")
        self.dirty |= changed
        return changed

    def ambiguous_column(self, sql_key: str, message: str) -> bool:
        """Two joined tables both have a column the model reads without saying from which. In the real
        warehouse only one has it: the one the model also names it with (`opp.end_date`). The other
        got it from an earlier guess; take it back."""
        m = re.search(r"Ambiguous reference to column name \"([^\"]+)\" \(use: '([^']+)' or '([^']+)'\)", message)
        tree = self.tree(sql_key) if m else None
        if tree is None:
            return False
        col = m.group(1).upper()
        aliases = [m.group(i).rsplit(".", 1)[0].strip('"').upper() for i in (2, 3)]
        named = {a: sum(1 for c in tree.find_all(exp.Column) if c.name.upper() == col and c.table.upper() == a) for a in aliases}
        keep = [a for a in aliases if named[a]]
        if len(keep) != 1:
            # neither is named with it: a CTE whose SELECT writes the column out has it for certain
            wrote = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}
            keep = [a for a in aliases if a in wrote and any(e.alias.upper() == col for e in wrote[a].find_all(exp.Alias))]
        if len(keep) != 1:
            # no other evidence: the column is read from the table in FROM, not from one joined to it
            firsts = {s.args["from_"].this.alias_or_name.upper() for c in tree.find_all(exp.Column)
                      if c.name.upper() == col and not c.table
                      for s in [c.find_ancestor(exp.Select)] if s is not None and s.args.get("from_") is not None}
            keep = [a for a in aliases if a in firsts]
        if len(keep) != 1:
            return False
        drop = next(a for a in aliases if a != keep[0])
        ctes = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}
        changed = False
        for tb in tree.find_all(exp.Table):
            if tb.alias_or_name.upper() != drop:
                continue
            for fq in self.real_tables(tb, ctes) if tb.catalog and tb.db else self.real_tables(ctes.get(tb.name.upper(), tb), ctes):
                source = self._source_behind(fq)
                columns = (self.tables.get(source or "", {}).get("columns") or {})
                if source and col in columns and self.named_with(source, col):
                    # another model reads `<this table>.<col>`: it is the other side that guessed
                    changed |= self._take_back(tree, ctes, keep[0], col, drop)
                    continue
                if col in columns and columns[col].get("gen") in (*_FREE, "dist", "time_offset", "categorical"):
                    del columns[col]
                    self.retyped.add(source)
                    self.learned.append(f"{source}.{col}: the model reads {col} unqualified next to {keep[0]}, which has it; removed here.")
                    changed = True
        self.dirty |= changed
        return changed

    def named_with(self, fq: str, col: str) -> bool:
        """Does some model read `col` from `fq` by the table's name or alias (`invoices.charge`)?"""
        short = fq.rsplit(".", 1)[-1]
        for uid, sql in list(self.sql_of.items()):
            text = sql.upper()
            if col not in text or short not in text:
                continue
            tree = self.tree(uid)
            if tree is None:
                continue
            for c in tree.find_all(exp.Column):
                select = c.find_ancestor(exp.Select)
                if c.name.upper() != col or not c.table or select is None or select.args.get("from_") is None:
                    continue
                for rel in [select.args["from_"].this, *[j.this for j in select.args.get("joins") or []]]:
                    if not (isinstance(rel, exp.Table) and rel.catalog and rel.db and rel.alias_or_name.upper() == c.table.upper()):
                        continue
                    name = f"{rel.catalog.upper()}.{rel.db.upper()}.{rel.name.upper()}"
                    if name == fq or (short in name and self._source_behind(name) == fq):
                        return True
        return False

    def _take_back(self, tree, ctes: dict, alias: str, col: str, other: str) -> bool:  # noqa: ANN001
        changed = False
        for tb in tree.find_all(exp.Table):
            if tb.alias_or_name.upper() != alias:
                continue
            for fq in self.real_tables(tb, ctes) if tb.catalog and tb.db else self.real_tables(ctes.get(tb.name.upper(), tb), ctes):
                source = self._source_behind(fq)
                columns = (self.tables.get(source or "", {}).get("columns") or {})
                if col in columns and columns[col].get("gen") in (*_FREE, "dist", "time_offset", "categorical") \
                        and not self.named_with(source, col):
                    del columns[col]
                    self.retyped.add(source)
                    self.learned.append(f"{source}.{col}: the model reads {col} unqualified next to {other}, which has it; removed here.")
                    changed = True
        return changed

    def few_values(self, sql_key: str) -> bool:
        """`dbt_utils.pivot('feature', get_column_values(...))` writes one column per value it finds.
        On made-up text that is one column per row; the column really holds a handful of values."""
        raw = self.nodes.get(sql_key, {}).get("raw_code") or ""
        names = re.findall(r"get_column_values\([^,]+,\s*['\"](\w+)['\"]", raw) + re.findall(r"pivot\(\s*['\"](\w+)['\"]", raw)
        changed = False
        for column in dict.fromkeys(n.upper() for n in names):
            for table, name in [(t, n) for t in self.sources_of(sql_key) for n in self.origins(sql_key, column)]:
                gen = (self.tables[table].get("columns") or {}).get(name)
                if gen is None or gen.get("gen") not in _FREE:
                    continue
                values = [f"{name.lower()}_{i}" for i in range(1, 7)]
                self.tables[table]["columns"][name] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
                self.retyped.add(table)
                self.learned.append(f"{table}.{name}: pivoted into one column per value; generated with {len(values)} values.")
                changed = True
        self.dirty |= changed
        return changed

    def runaway(self, sql_key: str, message: str) -> bool:
        """The model did not finish, or filled the disk: on generated data that is a join that pairs
        every row with every row. Tables joined on a range (`a.day between b.start and b.finish`) are
        small reference tables in real life: generate them small."""
        if not re.search(r"statement stopped after|stopped after \d+s|No space left on device|max_temp_directory_size|"
                         r"Out of Memory|could not allocate|exceeds the limit", message, re.I):
            return False
        changed = self.few_values(sql_key)
        tree = self.tree(sql_key)
        if tree is None:
            return changed
        ctes = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}
        targets: set[str] = set()
        for join in tree.find_all(exp.Join):
            on = join.args.get("on")
            equal = [e for e in (on.find_all(exp.EQ) if on is not None else [])
                     if isinstance(e.this, exp.Column) and isinstance(e.expression, exp.Column) and e.this.table != e.expression.table]
            if equal or join.args.get("using"):
                continue  # joined on a key
            for fq in self.real_tables(join.this, ctes):
                source = self._source_behind(fq) or (fq if fq in self.tables else None)
                owner = self.by_rel.get(fq) or self.relation_owner.get(fq) or ""
                targets |= {source} if source else {t for t in self.sources_of(owner) if t in self.tables}
        for table in sorted(targets):
            volume = self.tables[table].setdefault("volume", {})
            rows = float(volume.get("rows_per_day", 1000))
            if rows <= 1000:
                continue
            volume["rows_per_day"] = max(1000, int(rows / 100))
            self.retyped.add(table)
            self.learned.append(f"{table}: joined on a range by {self.nodes.get(sql_key, {}).get('name', sql_key)}, which did not "
                                f"finish; generated as a small reference table ({volume['rows_per_day']} rows a day before scaling).")
            changed = True
        self.dirty |= changed
        return changed

    def learn(self, sql_key: str, message: str) -> bool:
        """Apply whatever one error message teaches. True when the spec changed."""
        if self.runaway(sql_key, message) or self.looked_for(sql_key) or self.jinja_columns(sql_key, message) \
                or self.ambiguous_column(sql_key, message):
            return True
        if self.udf_input(sql_key, message) or self.pivot_values(sql_key, message) or self.missing_column(sql_key, message) or self.missing_relation(sql_key, message) \
                or self.union_widths(sql_key, message):
            return True
        named = _FROM_COLUMN.search(message)
        bad = _BAD_CAST.search(message)
        if bad and named and bad.group("from").startswith(("TIMESTAMP", "DATE")) and re.match(r"DECIMAL|DOUBLE|BIGINT|INTEGER|HUGEINT", bad.group("to")):
            # a column the SQL casts to a number before reading it as a time holds epoch numbers
            return self.retype_column(sql_key, named.group(1).upper(), "EPOCH")
        if bad and named and re.match(r"DECIMAL|DOUBLE|BIGINT|INTEGER|HUGEINT", bad.group("from")) and bad.group("to").startswith(("DATE", "TIMESTAMP")):
            # `max_ts::date` on a column generated as a number: Snowflake has no such cast, so it is a time
            if self.retype_column(sql_key, named.group(1).upper(), "TIMESTAMP"):
                return True
        for pattern, fixed in _CONVERT:
            m = pattern.search(message)
            if m:
                hint = fixed or _HINTS.get(m.group("type").upper())
                fmt = re.search(r'according to format specifier "([^"]+)"', message)
                fmt = fmt.group(1) if fmt else None
                if not hint:
                    return False
                if named and hint in ("BIGINT", "DOUBLE") and re.fullmatch(r"[0-9a-f]{32}", m.group("value")) \
                        and self.compared_with_a_hash(sql_key, named.group(1).upper()):
                    return True
                if named and self.retype_column(sql_key, named.group(1).upper(), hint, fmt):
                    tree = self.tree(sql_key)
                    kinds = {"DATE": ("date",), "TIMESTAMP": ("timestamp", "timestampntz", "timestampltz", "timestamptz"), "BOOLEAN": ("boolean",),
                             "DOUBLE": ("float", "double", "decimal", "int", "bigint"), "BIGINT": ("float", "double", "decimal", "int", "bigint")}.get(hint)
                    if kinds and not fmt and tree is not None:
                        # `a::date ... b::date ... c::date`: the error names one; the model casts the others the same way
                        sources = self.sources_of(sql_key)
                        for c in dict.fromkeys(c.this.name.upper() for c in tree.find_all(exp.Cast, exp.TryCast)
                                               if c.to.is_type(*kinds) and isinstance(c.this, exp.Column)):
                            gens = [(self.tables[t].get("columns") or {}).get(c) for t in sources]
                            gens = [g for g in gens if g]
                            if any(g.keys() & {"format", "epoch"} for g in gens):
                                continue  # already learned how this one is written
                            if hint in ("DOUBLE", "BIGINT", "BOOLEAN") and not all(g.get("gen") in _FREE for g in gens):
                                continue  # a key or a reference read as a number is a lesson of its own
                            self.retype_column(sql_key, c, hint)
                    return True  # DuckDB named the column it was reading
                if named and not fmt and self.tree(sql_key) is not None:
                    # UNPIVOT (headcount FOR month IN ("2020-04-30", ...)): the value comes from every listed column
                    listed = [c.name.upper() for pivot in self.tree(sql_key).find_all(exp.Pivot)
                              if pivot.args.get("unpivot") and any(e.name.upper() == named.group(1).upper() for e in pivot.expressions)
                              for field in pivot.args.get("fields") or [] for c in field.find_all(exp.Column)]
                    if any([self.retype_column(sql_key, c, hint) for c in dict.fromkeys(listed)]):
                        return True
                precision = re.search(r"to DECIMAL\((\d+),\s*(\d+)\)", message)
                if precision and re.fullmatch(r"-?[\d.]+", m.group("value")):
                    # a number that does not fit NUMBER(p, s): the column holds smaller ones
                    top = 10 ** (int(precision.group(1)) - int(precision.group(2))) - 1
                    return self.bound(sql_key, f"DECIMAL({precision.group(1)}, {precision.group(2)})", top)
                if hint in ("DATE", "TIMESTAMP") and named and re.fullmatch(r".{6,}01", m.group("value")) and \
                        not re.fullmatch(r"\d{4}-\d\d-\d\d", m.group("value")):
                    # period || '01' read as a date: the column holds a year and a month
                    if self.retype_column(sql_key, named.group(1).upper(), "DATE", "%Y%m"):
                        return True
                if hint == "TIMESTAMP" and re.fullmatch(r"\d{4}-\d\d-\d\d \d{4}-\d\d-\d\d[ T][\d:.]+", m.group("value")):
                    return any([self.retype_column(sql_key, c, "TIMESTAMP", "%H:%M:%S") for c in self.columns_used(sql_key, "time_of_day")])
                if hint in ("BIGINT", "DOUBLE") and named and self.split_number(sql_key, named.group(1).upper()):
                    return True
                if m.group("value") == "" and hint in ("BIGINT", "DOUBLE") and self.split_numbers(sql_key):
                    return True
                if m.group("value") == "" and hint in ("BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"):
                    # `nullif(col, '')` on a column generated as a number or a date: the SQL treats it as text
                    return any([self.retype_column(sql_key, c, "TEXT") for c in self.columns_used(sql_key, "empty")])
                if hint in ("DATE", "TIMESTAMP") and re.fullmatch(r"-?\d+(\.\d+)?", m.group("value")):
                    tree = self.tree(sql_key)
                    direct = [c.this.name.upper() for c in (tree.find_all(exp.Cast, exp.TryCast) if tree is not None else [])
                              if c.to.is_type("date", "timestamp", "timestampntz", "timestampltz", "timestamptz")
                              and isinstance(c.this, exp.Column)]
                    if any([self.retype_column(sql_key, c, hint) for c in dict.fromkeys(direct)]):
                        return True
                compared = self.columns_used(sql_key, "literal", m.group("value"))
                if compared and hint in ("BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN") and \
                        any([self.retype_column(sql_key, c, "TEXT") for c in compared]):
                    return True  # the value is a word the SQL compares a column with: that column is text
                if hint == "VARIANT":  # text pasted into JSON ('[' || ids || ']'): numbers fit, words do not
                    pasted = [t.rsplit("_", 1)[0].upper() for t in re.findall(r"[a-z][a-z0-9_]*_[0-9a-f]{12}", m.group("value"))]
                    if pasted and any([self.retype_column(sql_key, c, "BIGINT") for c in dict.fromkeys(pasted)]):
                        return True
                if fmt and any([self.retype_column(sql_key, c, hint, self.split_format(sql_key, c, fmt))
                                for c in self.columns_used(sql_key, "format", fmt)]):
                    return True
                if hint == "ARRAY" and any([self.retype_column(sql_key, c, "ARRAY") for c in self.columns_used(sql_key, "array")]):
                    self.array_values(sql_key)
                    return True
                for token in m.group("value").split():  # generated text starts with its column's name
                    if "_" in token and self.retype_column(sql_key, token.rsplit("_", 1)[0].upper(), hint, fmt):
                        return True
                return self.retype(m.group("value"), hint, fmt, only=set(self.sources_of(sql_key)))
        if "Failed to cast value to numerical" in message:
            # coalesce(payload['minutes'], minutes)::float where `minutes` was generated as an object
            tree = self.tree(sql_key)
            names, changed = [], False
            for cast in (tree.find_all(exp.Cast, exp.TryCast) if tree is not None else []):
                hint = "DOUBLE" if cast.to.is_type("float", "double", "decimal", "int", "bigint") else \
                    "BOOLEAN" if cast.to.is_type("boolean") else None
                for leaf in self._cast_reads(cast.this) if hint else []:
                    if isinstance(leaf, exp.Column) and hint == "DOUBLE":
                        names.append(leaf.name.upper())
                    elif isinstance(leaf, exp.JSONExtract) and isinstance(leaf.this, exp.Column):
                        changed |= self.retype_field(sql_key, leaf, hint)
            return any([self.retype_column(sql_key, c, "DOUBLE") for c in dict.fromkeys(names)]) or changed
        if re.search(r"argument types '[-+]\((?:TIMESTAMP[^,]*, (?:INTEGER|BIGINT|TINYINT|SMALLINT)|(?:INTEGER|BIGINT)[^,]*, TIMESTAMP)", message):
            # `term_date + 1`: Snowflake adds days to a DATE and refuses a TIMESTAMP, so the column is a DATE
            tree = self.tree(sql_key)
            names = []
            for node in (tree.find_all(exp.Add, exp.Sub) if tree is not None else []):
                sides = [node.this.unnest(), node.expression.unnest()]
                if any(isinstance(x, exp.Literal) and x.is_int for x in sides):
                    names += [x.name.upper() for x in sides if isinstance(x, exp.Column)]
            if any([self.retype_column(sql_key, c, "DAY") for c in dict.fromkeys(names)]):
                return True
        m = _NOT_STRUCT.search(message)
        if m:
            return self.make_variant(m.group("col"), m.group("field"))
        m = _NO_FUNCTION.search(message)
        if m and "VARCHAR" in m.group("types"):
            return self.wrong_type(sql_key, m.group("name").lower())
        return False

    @staticmethod
    def _cast_reads(node: exp.Expression) -> list[exp.Expression]:
        """The columns and object fields whose value a cast converts as it is: through COALESCE, IFF,
        CASE and arithmetic, not through a function that makes new text (SPLIT_PART, REGEXP_SUBSTR)."""
        if isinstance(node, (exp.Column, exp.JSONExtract)):
            return [node]
        if isinstance(node, (exp.Paren, exp.Cast, exp.TryCast, exp.Neg)):
            return Fitter._cast_reads(node.this)
        if isinstance(node, (exp.Coalesce, exp.Nullif, exp.Binary)) and not isinstance(node, (exp.DPipe, exp.Predicate, exp.Connector)):
            return [leaf for arg in node.iter_expressions() for leaf in Fitter._cast_reads(arg)]
        if isinstance(node, exp.If):
            return [leaf for arg in (node.args.get("true"), node.args.get("false")) if arg is not None for leaf in Fitter._cast_reads(arg)]
        if isinstance(node, exp.Case):
            results = [i.args.get("true") for i in node.args.get("ifs") or []] + [node.args.get("default")]
            return [leaf for arg in results if arg is not None for leaf in Fitter._cast_reads(arg)]
        return []

    def retype_field(self, uid: str, access: exp.JSONExtract, hint: str) -> bool:
        """`col:field::int` on an object column whose field was generated as text."""
        keys = [str(k.this) for k in access.expression.expressions if isinstance(k, exp.JSONPathKey)] \
            if isinstance(access.expression, exp.JSONPath) else []
        if not keys:
            return False
        col, changed = access.this.name.upper(), False
        for table in self.sources_of(uid):
            gen = (self.tables[table].get("columns") or {}).get(col)
            fields = gen.get("fields") if gen and gen.get("gen") == "object" else None
            for path, field in list((fields or {}).items()):
                if path.lower() not in (".".join(keys).lower(), keys[-1].lower()) or field.get("gen") not in _FREE or field.get("format"):
                    continue
                fresh = {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.5, False: 0.5}} if hint == "BOOLEAN" \
                    else {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
                fields[path] = fresh
                self.retyped.add(table)
                self.learned.append(f"{table}: field {col}:{path} used as {hint}, retyped.")
                changed = True
        self.dirty |= changed
        return changed

    def output_columns(self, uid: str) -> list[str] | None:
        """The columns a model's SQL writes out by name, or None when a `*` reaches a real relation
        (then they depend on that relation, not on the SQL alone)."""
        tree = self.tree(uid) if uid in self.sql_of else None
        if tree is None:
            return None
        ctes = {c.alias.upper(): c.this for c in tree.find_all(exp.CTE)}

        def columns(node: exp.Expression, depth: int = 0) -> list[str] | None:
            node = node.unnest()
            if isinstance(node, exp.SetOperation):
                return columns(node.left, depth + 1)
            if not isinstance(node, exp.Select) or depth > 12:
                return None
            out: list[str] = []
            for e in node.expressions:
                star = isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
                if not star:
                    if not e.alias_or_name:
                        return None
                    out.append(e.alias_or_name.upper())
                    continue
                if isinstance(e, exp.Star) and (e.args.get("except_") or e.args.get("replace") or e.args.get("rename")):
                    return None
                source = node.args.get("from_")
                relations = [source.this, *[j.this for j in node.args.get("joins") or []]] if source is not None else []
                if isinstance(e, exp.Column):
                    relations = [r for r in relations if r.alias_or_name.upper() == e.table.upper()]
                if not relations:
                    return None
                for r in relations:
                    inner = r.this if isinstance(r, exp.Subquery) else \
                        ctes.get(r.name.upper()) if isinstance(r, exp.Table) and not r.db else None
                    found = columns(inner, depth + 1) if inner is not None else None
                    if found is None:
                        return None
                    out += found
            return out

        return columns(tree)

    def test_names_a_column_the_model_does_not_write(self, node: dict, message: str) -> bool:
        """`unique: status_id` on a model whose SELECT renames it to `id`: the test fails in the
        warehouse as well, whatever the data."""
        m = next((m for m in (p.search(message) for p in _MISSING) if m), None)
        tested = node.get("attached_node") or next((d for d in node["depends_on"]["nodes"] if d.startswith("model.")), None)
        if not m or not tested or len([d for d in node["depends_on"]["nodes"] if d in self.nodes]) != 1:
            return False
        if tested not in self.sql_of and (self.nodes.get(tested) or {}).get("compiled_code"):
            self.sql_of[tested] = self.nodes[tested]["compiled_code"].strip().rstrip(";")
        written = self.output_columns(tested)
        return written is not None and m.group("col").upper().strip('"') not in written

    def learn_upstream(self, uid: str, message: str, depth: int = 0, seen: set[str] | None = None) -> bool:
        """A view runs when it is read: the statement that failed can be the SQL of a view the model
        (or the test) selects from. Offer the error to those, nearest first."""
        seen = seen if seen is not None else set()
        parents = [d for d in ((self.nodes.get(uid) or {}).get("depends_on") or {}).get("nodes") or []
                   if d in self.nodes and d not in seen and self.nodes[d].get("resource_type") == "model"]
        seen.update(parents)
        for parent in parents:
            node = self.nodes[parent]
            lazy = (node.get("config") or {}).get("materialized") in ("view", "ephemeral")
            if not lazy:
                continue
            if parent not in self.sql_of and node.get("compiled_code"):
                self.sql_of[parent] = node["compiled_code"].strip().rstrip(";")
            if parent in self.sql_of and self.learn(parent, message):
                return True
            if depth < 4 and self.learn_upstream(parent, message, depth + 1, seen):
                return True
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
                    names.update(self.origins(sql_key, arg.name.upper()))
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
        self.macros = manifest.get("macros") or {}
        self.project_relations = {_fq(n["relation_name"]) for n in nodes.values() if n.get("relation_name")}
        self.relation_owner = {_fq(n["relation_name"]): uid for uid, n in nodes.items() if n.get("relation_name")}
        self.source_names = {uid: _fq(f'{n["database"]}.{n["schema"]}.{n.get("identifier") or n["name"]}')
                             for uid, n in manifest.get("sources", {}).items()}
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

        def called(uid: str) -> list[str]:  # the macros the model's own SQL calls (not its hooks)
            raw = nodes[uid].get("raw_code") or ""
            return [m for m in nodes[uid]["depends_on"].get("macros", []) if re.search(r"\b" + re.escape(m.split(".")[-1]) + r"\s*\(", raw)]

        self.deferred = {uid for uid in graph if _INSPECTS.search(nodes[uid].get("raw_code") or "")
                         or any(inspects(m) for m in called(uid))}
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
        body = self.sql_of[uid]
        if self.nodes.get(uid, {}).get("resource_type") == "snapshot":
            # what `dbt snapshot` adds to the query's columns, so the models that read the snapshot bind
            body = ("select _s.*, 'x' as dbt_scd_id, current_timestamp::timestamp_ntz as dbt_updated_at, "
                    f"current_timestamp::timestamp_ntz as dbt_valid_from, null::timestamp_ntz as dbt_valid_to from (\n{body}\n) as _s")
        statement = f"create or replace table {target} as (\n{body}\n)"
        fast: list[str] | None = None  # the statement as DuckDB SQL, to retry without transpiling again
        fallbacks = 0
        seen: set[str] = set()
        began = time.time()
        failure = None
        for _ in range(400):
            cur = conn.cursor()
            try:
                if time.time() - began > self.model_timeout:
                    raise TimeoutError
                if fast is None:
                    cur.execute(statement)  # the path dbt takes
                else:
                    for part in fast:
                        emulator.run_guarded(cur, lambda part=part: cur._duck_conn.execute(part))
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
                if "INTERRUPT" in message.upper() or "statement stopped after" in message or isinstance(e, TimeoutError):
                    message = f"stopped after {self.model_timeout:.0f}s: the query runs away on the generated data"
                    self.runaway(uid, message)
                else:
                    known, self.retyped = self.retyped, set()
                    learned = self.learn(uid, message)
                    fresh, self.retyped = self.retyped, known | self.retyped
                    if learned and fresh and message not in seen and len(seen) < 25:
                        # its own source was generated wrongly: make those tables again and run it again,
                        # so a model with ten mistyped columns costs one pass and not ten
                        seen.add(message)
                        try:
                            self.generate(only=fresh)
                            fast = None
                            continue
                        except Exception:  # noqa: BLE001, S110 - the next full generation will say why
                            pass
                failure = message
                break
            finally:
                cur.close()
        if time.time() - began > 20:
            self.log(f"    slow: {self.nodes[uid]['name']} took {time.time() - began:.0f}s")
        return failure

    def learn_from_models(self, manifest: dict, database: str, passes: int = 6) -> dict[str, str]:
        """Returns {model: error} for the models that still fail after the spec stopped changing."""
        order = [uid for uid in self.load(manifest)]
        if self.deferred:
            self.log(f"  {len(self.deferred)} model(s) inspect the warehouse while compiling, or read one that does: left to the build")
        self.hidden_dependencies()
        nodes = manifest["nodes"]
        for uid in order:  # `(log_date || ' ' || log_time)::timestamp`: known from the SQL before anything runs
            for column in self._columns_used(self.tree(uid), "time_of_day"):
                self.retype_column(uid, column, "TIMESTAMP", "%H:%M:%S")
        sources = {uid: _fq(f'{n["database"]}.{n["schema"]}.{n.get("identifier") or n["name"]}')
                   for uid, n in manifest.get("sources", {}).items()}
        failures: dict[str, str] = {}
        todo = set(order)
        if os.environ.get("FAUXFLAKE_FIT_ONLY"):  # for working on the fitter: these models and what they read
            wanted = set(Path(os.environ["FAUXFLAKE_FIT_ONLY"]).read_text().split())
            picked = {uid for uid in order if nodes[uid]["name"] in wanted}
            todo = picked | {a for uid in picked for a in self._ancestors(uid) if a in todo}
            order = [uid for uid in order if uid in todo]
        for n_pass in range(1, passes + 1):
            self.generate()
            before = len(self.learned)
            self.retyped = set()
            failures = {uid: m for uid, m in failures.items() if uid not in todo}
            conn = self.emu._fs.connect(database=database, schema="CI")
            started, done = time.time(), 0
            trace = open(os.environ["FAUXFLAKE_FIT_TRACE"], "a") if os.environ.get("FAUXFLAKE_FIT_TRACE") else None  # noqa: SIM115
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
                if trace:
                    trace.write(f"{nodes[uid]['name']}\n")
                    trace.flush()
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
            Path(os.environ["FAUXFLAKE_FIT_DEBUG"] + ".learned.txt").write_text("\n".join(self.learned))
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

    def learn_from_build(self, project, analysis, rounds: int = 14) -> dict:  # noqa: ANN001
        """Run `dbt build`, read the results, fix what the spec can fix. Returns the last summary."""
        summary: dict = {}
        selected: list[str] = []
        outcome: dict[str, tuple[str, str, str]] = {}  # node -> (kind, status, message), across rounds
        os.environ["FAUXFLAKE_STATEMENT_TIMEOUT"] = "300"  # a real build, at build scale: more room than a trial run gets
        for n_round in range(1, rounds + 1):
            # at the scale and history `fauxflake build` uses, so that what is reported here is what a build gives
            self.generate(scale=0.001, history=60)
            before = len(self.learned)
            self.retyped = set()
            results_path = project.target_path / "run_results.json"
            results_path.unlink(missing_ok=True)  # what is there is from the compile step
            if n_round > 1:
                # every round is the build a fresh warehouse gives (what `fauxflake build` will run): what the
                # round before left behind goes, and relations the project reads before building them
                # start empty again
                from .generate import existing_view

                for node in self.nodes.values():
                    if node.get("resource_type") in ("model", "snapshot", "seed") and node.get("relation_name") \
                            and _fq(node["relation_name"]).count(".") == 2:
                        for kind in ("table", "view"):
                            try:
                                self.emu.duck.execute(f"drop {kind} if exists {quoted(_fq(node['relation_name']))}")
                            except Exception:  # noqa: BLE001, S110 - it is the other kind
                                pass
                for fq, rel in self.existing.items():
                    try:
                        self.emu.ensure_database(*fq.split(".")[:2])
                        self.emu.duck.execute(existing_view(fq, rel.get("columns") or {}, table=rel.get("as") == "table"))
                    except Exception:  # noqa: BLE001, S110
                        pass
            # for working on the fitter: FAUXFLAKE_FIT_SELECT="+model_a +model_b" builds only those
            selected = ["--select", *os.environ["FAUXFLAKE_FIT_SELECT"].split()] if os.environ.get("FAUXFLAKE_FIT_SELECT") else []
            project.invoke(["build", *selected], silent=True)
            if not results_path.exists():
                self.log(f"  dbt build did not run: {project.last_error[:600] or 'no results were written'}")
                return {"error": project.last_error or "dbt build wrote no results"}
            results = json.loads(results_path.read_text())["results"]
            manifest = json.loads((project.target_path / "manifest.json").read_text())
            nodes = self.nodes = manifest["nodes"]
            self.macros = manifest.get("macros") or {}
            self.project_relations = {_fq(n["relation_name"]) for n in nodes.values() if n.get("relation_name")}
            self.relation_owner = {_fq(n["relation_name"]): uid for uid, n in nodes.items() if n.get("relation_name")}
            self.source_names = {uid: _fq(f'{n["database"]}.{n["schema"]}.{n.get("identifier") or n["name"]}')
                                 for uid, n in manifest.get("sources", {}).items()}
            for uid, n in nodes.items():  # what the build compiled, also for the models loop 1 left alone
                code = n.get("compiled_code")
                if code and n.get("resource_type") in ("model", "snapshot") and n.get("language", "sql") == "sql":
                    if self.sql_of.get(uid) != code.strip().rstrip(";"):
                        self.sql_of[uid] = code.strip().rstrip(";")
                        self.trees.pop(uid, None)
                    if n.get("relation_name") and _fq(n["relation_name"]).count(".") == 2:
                        self.by_rel[_fq(n["relation_name"])] = uid
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
                    if r["status"] == "error":  # the test's own SQL failed: the same lessons as for a model
                        compiled = self._compiled(project, node)
                        if compiled:
                            self.sql_of[node["unique_id"]] = compiled
                            self.trees.pop(node["unique_id"], None)
                            if not self.learn(node["unique_id"], message) and not self.learn_upstream(node["unique_id"], message) \
                                    and self.test_names_a_column_the_model_does_not_write(node, message) and node["name"] not in expected:
                                expected.append(node["name"])
                                accepted += 1
                                outcome.pop(r["unique_id"], None)
                                self.learned.append(f"test {node['name']}: names a column its model does not write; listed as an expected failure.")
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
                    if self.reads_itself_before_it_exists(uid, fq, message):
                        continue
                    if not self.learn(uid, message):
                        self.learn_upstream(uid, message)  # the SQL that failed may be a view this model selects from
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
            if os.environ.get("FAUXFLAKE_FIT_DEBUG"):
                Path(os.environ["FAUXFLAKE_FIT_DEBUG"] + f".build{n_round}.json").write_text(json.dumps(
                    {"failed": {r["unique_id"]: (r.get("message") or "")[:2000] for r in failed},
                     "slow": sorted(((round(r.get("execution_time") or 0), r["unique_id"]) for r in results), reverse=True)[:15],
                     "learned": self.learned[before:], "statements": dict(emulator.FAILED_STATEMENTS)}, indent=1))
                emulator.FAILED_STATEMENTS.clear()
            if not failed or (len(self.learned) == before and not accepted):
                break
            # next round: what failed, what reads a source whose values changed, and everything downstream
            changed = [f"source:{n['source_name']}.{n['name']}+" for uid, n in manifest.get("sources", {}).items()
                       if self.source_names.get(uid) in self.retyped]
            # and the models that name a changed table themselves, or find it by looking at the warehouse
            hit = {fq.upper() for fq in self.retyped} | {fq.split(".", 1)[1].upper() for fq in self.retyped}
            for node in nodes.values():
                code = (node.get("compiled_code") or "").upper().replace('"', "") if node.get("resource_type") in ("model", "snapshot") else ""
                if code and any(name in code for name in hit):
                    again.add(node["name"])
            for fq, members in self.unions.items():
                owner = self.by_rel.get(fq) or self.relation_owner.get(fq)
                if owner in nodes and any(m in self.retyped for m in members):
                    again.add(nodes[owner]["name"])
            others = len(self.learned) - before - len(self.retyped)  # lessons about something else than a source's values
            if not again or len(again) + len(changed) > 300 or (others > 0 and not changed and not again):
                selected = []
            else:
                selected = ["--select", *[f"{name}+" for name in sorted(again)], *sorted(changed)]
        if not self.spec["dbt"].get("expected_failures"):
            self.spec["dbt"].pop("expected_failures", None)
        self.save()
        return summary

    def _selected(self, selected: list[str]) -> set[str] | None:
        """The nodes `--select name+ source:x.y+ ...` covers; None when everything is built."""
        if not selected:
            return None
        children: dict[str, list[str]] = {}
        for uid, node in self.nodes.items():
            for parent in node.get("depends_on", {}).get("nodes", []):
                children.setdefault(parent, []).append(uid)
        by_name = {node["name"]: uid for uid, node in self.nodes.items() if node.get("resource_type") in ("model", "snapshot", "seed")}
        sources = {f"source:{uid.split('.', 2)[2]}": uid for uid in self.source_names}
        pile = [by_name.get(s.rstrip("+")) or sources.get(s.rstrip("+")) for s in selected if not s.startswith("--")]
        chosen: set[str] = set()
        while pile:
            uid = pile.pop()
            if uid and uid not in chosen:
                chosen.add(uid)
                pile += children.get(uid, [])
        return chosen

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
            if gen["gen"] in ("time_offset", "event_time", "updated_at"):
                if gen.get("unique"):
                    return False
                # a date nobody shares (a sheet with one row per month): stays a date, one day per row
                columns[col] = {"p": [0, 0.5, 1], "seconds": [0, 86400, 604800], **gen, "gen": "time_offset", "unique": True}
            elif not any(d["gen"] in ("sequence", "hash_id") for d in columns.values()):
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
