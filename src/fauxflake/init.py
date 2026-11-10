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

import json
import re
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

LOADED_AT = ["_LOADED_AT", "_FIVETRAN_SYNCED", "_AIRBYTE_EXTRACTED_AT", "_SDC_BATCHED_AT", "_ETL_LOADED_AT",
             "LOADED_AT", "_INGESTED_AT", "INGESTED_AT", "_PARTITIONTIME"]
EVENT_TIME = ["OCCURRED_AT", "EVENT_TIMESTAMP", "EVENT_TIME", "EVENT_AT", "ORDERED_AT", "CREATED_AT", "CREATED",
              "TIMESTAMP", "CREATED_DATE", "DATE"]
UPDATED_AT = ["UPDATED_AT", "MODIFIED_AT", "LAST_MODIFIED", "UPDATED", "_UPDATED_AT"]
AMOUNT = {"p": [0, 0.5, 0.9, 0.99, 1], "v": [0, 20, 100, 500, 5000]}
WEEK = {"p": [0, 0.5, 1], "seconds": [0, 86400, 604800]}


class SourceUse:
    """What the project's SQL reveals about one source table."""

    def __init__(self) -> None:
        self.columns: dict[str, str | None] = {}  # column -> type hint from a cast
        self.star = False

    def add(self, column: str, type_hint: str | None = None) -> None:
        column = column.upper()
        self.columns[column] = self.columns.get(column) or type_hint


def _table_id(table: exp.Table) -> str:
    return ".".join(p.upper() for p in (table.catalog, table.db, table.name) if p)


def scan_model(sql: str, sources: dict[str, SourceUse],
               models: dict[str, dict[str, tuple[str, str]]] | None = None) -> dict[str, tuple[str, str]]:
    """Record source columns used by one model. Returns {output column: (source, source column)}
    for outputs that are a plain (possibly cast or renamed) source column."""
    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except sqlglot.errors.SqlglotError:
        return {}
    passthrough: dict[str, str] = {}  # CTE name -> source it is `select *` of
    lineage: dict[str, dict[str, tuple[str, str]]] = {}  # CTE name -> its outputs
    last: dict[str, tuple[str, str]] = {}
    for scope in traverse_scope(tree):
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        # which source (if any) each alias in this scope stands for, and prior renames
        direct: dict[str, str] = {}
        renames: dict[str, dict[str, tuple[str, str]]] = {}
        selected = {alias: src for alias, (_, src) in scope.selected_sources.items()}
        for alias, node in selected.items():
            if isinstance(node, exp.Table):
                if _table_id(node) in sources:
                    direct[alias.upper()] = _table_id(node)
                elif node.name.upper() in passthrough:
                    direct[alias.upper()] = passthrough[node.name.upper()]
                elif models and _table_id(node) in models:  # another model: follow its lineage
                    renames[alias.upper()] = models[_table_id(node)]
            else:  # a CTE or subquery scope
                name = alias.upper()
                if name in passthrough:
                    direct[name] = passthrough[name]
                elif name in lineage:
                    renames[name] = lineage[name]
        only = next(iter(direct.values())) if len(direct) == 1 and not renames and len(selected) == 1 else None

        def resolve(col: exp.Column):
            table = col.table.upper()
            if table in direct or (not table and only):
                return (direct.get(table) or only), col.name.upper()
            if table in renames:
                return renames[table].get(col.name.upper())
            if not table and len(renames) == 1 and len(selected) == 1:
                return next(iter(renames.values())).get(col.name.upper())
            return None

        for col in scope.columns:
            hit = resolve(col)
            if hit:
                cast = col.parent if isinstance(col.parent, (exp.Cast, exp.TryCast)) else None
                sources[hit[0]].add(hit[1], cast.to.sql(dialect="duckdb") if cast else None)

        outputs: dict[str, tuple[str, str]] = {}
        stars = [p for p in select.expressions if isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))]
        for proj in select.expressions:
            inner = proj.unalias()
            while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                inner = inner.this
            if isinstance(inner, exp.Column) and not isinstance(inner.this, exp.Star):
                hit = resolve(inner)
                if hit:
                    outputs[proj.alias_or_name.upper()] = hit
        name = scope.expression.parent.alias.upper() if isinstance(scope.expression.parent, exp.CTE) else None
        if stars and only and len(select.expressions) == 1 and not select.args.get("joins"):
            if name:
                passthrough[name] = only
            else:
                sources[only].star = True
        if stars and len(renames) == 1 and len(selected) == 1:
            outputs = {**next(iter(renames.values())), **outputs}
        if name:
            lineage[name] = outputs
        last = outputs
    return last


def guess(column: str, hint: str | None) -> dict:
    """A generator for a column we know only by name and (maybe) a cast."""
    c, h = column.upper(), (hint or "").upper()
    if h.startswith(("DECIMAL", "NUMERIC")) and not h.endswith(",0)"):
        return {"gen": "dist", "type": h, **AMOUNT}
    if h in ("DOUBLE", "FLOAT", "REAL"):
        return {"gen": "dist", "type": "DOUBLE", **AMOUNT}
    if h == "BOOLEAN" or re.match(r"^(IS|HAS|WAS)_", c):
        return {"gen": "categorical", "type": "BOOLEAN", "values": {True: 0.5, False: 0.5}}
    if h == "DATE" or c.endswith(("_DATE", "_DAY")):
        return {"gen": "time_offset", "type": "DATE", **WEEK}
    if h.startswith("TIMESTAMP") or c.endswith(("_AT", "_TS", "_TIME", "_TIMESTAMP")):
        return {"gen": "time_offset", **WEEK}
    if h in ("BIGINT", "INTEGER", "INT", "SMALLINT") or h.endswith(",0)") or c.endswith(("_ID", "_COUNT", "_QTY", "_QUANTITY")) or c == "ID":
        return {"gen": "dist", "type": "BIGINT", "p": [0, 1], "v": [1, 1000]}
    if re.search(r"(AMOUNT|PRICE|TOTAL|COST|REVENUE|VALUE|BALANCE|RATE|SCORE)", c):
        return {"gen": "dist", "type": "DECIMAL(18,2)", **AMOUNT}
    if "EMAIL" in c:
        return {"gen": "template", "template": "user_{hex}@example.com"}
    return {"gen": "template", "template": f"{column.lower()}_{{hex}}"}


def first(candidates: list[str], columns) -> str | None:
    return next((c for c in candidates if c in columns), None)


def build_spec(manifest: dict, database: str) -> tuple[dict, list[str]]:
    notes: list[str] = []
    src_nodes = manifest["sources"]
    ids = {uid: f'{s["database"]}.{s["schema"]}.{s.get("identifier") or s["name"]}'.upper() for uid, s in src_nodes.items()}
    uses = {name: SourceUse() for name in ids.values()}
    for uid, s in src_nodes.items():
        for col, meta in (s.get("columns") or {}).items():
            uses[ids[uid]].add(col, (meta.get("data_type") or "").upper() or None)

    # how each model's outputs map back to source columns
    outputs: dict[str, dict[str, tuple[str, str]]] = {}
    by_relation: dict[str, dict[str, tuple[str, str]]] = {}

    def scan(uid: str) -> None:
        node = manifest["nodes"].get(uid)
        if uid in outputs or not node or node["resource_type"] != "model":
            return
        outputs[uid] = {}  # guards against cycles
        for parent in node["depends_on"]["nodes"]:
            scan(parent)
        if node.get("compiled_code"):
            outputs[uid] = scan_model(node["compiled_code"], uses, by_relation)
        if node.get("relation_name"):
            by_relation[node["relation_name"].replace('"', "").upper()] = outputs[uid]

    for uid in manifest["nodes"]:
        scan(uid)

    def locate(node_id: str, column: str) -> tuple[str, str] | None:
        if node_id in ids:
            return ids[node_id], column.upper()
        return outputs.get(node_id, {}).get(column.upper())

    keys: dict[str, str] = {}
    fks: dict[tuple[str, str], tuple[str, str]] = {}
    accepted: dict[tuple[str, str], list] = {}
    for node in manifest["nodes"].values():
        meta = node.get("test_metadata")
        if node["resource_type"] != "test" or not meta or not node.get("attached_node"):
            continue
        column = node.get("column_name") or meta["kwargs"].get("column_name")
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

    tables: dict[str, dict] = {}
    for uid, name in ids.items():
        use, src = uses[name], src_nodes[uid]
        cols = dict(use.columns)
        if not cols:
            notes.append(f"{name}: no columns found (only `select *`?). Declare them, or run `fauxflake profile`.")
            continue
        loaded = (src.get("loaded_at_field") or (src.get("config") or {}).get("loaded_at_field") or "").upper()
        loaded = loaded if loaded in cols else first(LOADED_AT, cols)
        key = keys.get(name) or first([f"{name.split('.')[-1].rstrip('S')}_ID", "ID"], cols)
        event = first([c for c in EVENT_TIME if c != loaded], cols)
        updated = first(UPDATED_AT, cols)
        if not keys.get(name):
            notes.append(f"{name}: no `unique` test found, {'assuming ' + key if key else 'no key'}.")
        out: dict[str, dict] = {}
        for col, hint in cols.items():
            if col == key:
                numeric = (hint or "").upper().startswith(("BIGINT", "INT", "DECIMAL", "NUMBER")) or (not hint and col.endswith("ID"))
                out[col] = {"gen": "sequence" if numeric else "hash_id"}
            elif (name, col) in fks:
                out[col] = {"gen": "fk", "ref": fks[(name, col)][0], "skew": 1.5}
            elif (name, col) in accepted:
                values = accepted[(name, col)]
                out[col] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
            elif col == loaded:
                out[col] = {"gen": "loaded_at"}
            elif col == event:
                out[col] = {"gen": "event_time"}
            elif col == updated:
                out[col] = {"gen": "updated_at"}
            else:
                out[col] = guess(col, hint)
        tables[name] = {
            "volume": {"rows_per_day": 1_000_000, "weekly": [1, 1, 1, 1, 1, 1, 1]},  # placeholder
            "lateness": {"p": [0, 0.5, 0.99, 1], "seconds": [1, 30, 3600, 86400]},  # placeholder
            "columns": out,
        }
    # a column named like another table's key is almost always a reference to it
    key_owner = {c: t for t, spec in tables.items() for c, d in spec["columns"].items() if d["gen"] in ("sequence", "hash_id")}
    for name, spec in tables.items():
        for col, d in spec["columns"].items():
            owner = key_owner.get(col)
            if owner and owner != name and d["gen"] not in ("sequence", "hash_id", "fk"):
                spec["columns"][col] = {"gen": "fk", "ref": owner, "skew": 1.5}
                notes.append(f"{name}.{col}: assumed to reference {owner} (same name as its key; no relationships test).")
    # a foreign key needs its parent to have a key
    for (child, col), (parent, pcol) in fks.items():
        if child in tables and parent in tables and tables[parent]["columns"].get(pcol, {}).get("gen") not in ("sequence", "hash_id"):
            notes.append(f"{child}.{col}: references {parent}.{pcol}, which is not that table's key; generated as a plain column.")
            tables[child]["columns"][col] = guess(col, None)
    return {"seed": 42, "start": "2025-01-01", "dbt": {"database": database}, "tables": tables}, notes


HEADER = """# Written by `fauxflake init` from the dbt project. Keys, relationships and categories come
# from your tests; columns and types from how models use each source. Volumes, lateness and
# distributions are placeholders: run `fauxflake profile` to measure them, or edit by hand.
"""


def write_spec(project, path: Path, database: str) -> list[str]:
    """`project` is a DbtProject pointed at a running emulator."""
    project.invoke(["compile"], quiet=True)  # best effort: models that fail to compile are skipped
    manifest = json.loads((project.target_path / "manifest.json").read_text())
    spec, notes = build_spec(manifest, database)
    path.write_text(HEADER + yaml.safe_dump(spec, sort_keys=False, default_flow_style=None, width=110))
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
