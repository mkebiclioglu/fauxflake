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


FLATTEN_COLUMNS = {"SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"}


class SourceUse:
    """What the project's SQL reveals about one source table."""

    def __init__(self) -> None:
        self.columns: dict[str, str | None] = {}  # column -> type hint from a cast
        self.paths: dict[str, dict[str, str | None]] = {}  # variant column -> {"a.b": type hint}
        self.arrays: set[str] = set()  # variant columns that are flattened or indexed
        self.formats: dict[str, str] = {}  # text column -> strftime format it is parsed with
        self.json_text: set[str] = set()  # text columns holding JSON (passed to PARSE_JSON)
        self.texts: set[str] = set()  # columns compared with string literals
        self.numbers: set[str] = set()  # columns that are summed, averaged or used in arithmetic
        self.star = False

    def add(self, column: str, type_hint: str | None = None) -> None:
        column = column.upper()
        self.columns[column] = self.columns.get(column) or type_hint


def _table_id(table: exp.Table) -> str:
    return ".".join(p.upper() for p in (table.catalog, table.db, table.name) if p)


def scan_model(sql: str, sources: dict[str, SourceUse],
               models: dict[str, dict[str, tuple[str, str]]] | None = None,
               links: list[set[tuple[str, str]]] | None = None) -> dict[str, tuple[str, str]]:
    """Record source columns used by one model. Returns {output column: (source, source column)}
    for outputs that are a plain (possibly cast or renamed) source column."""
    try:
        tree = sqlglot.parse_one(sql, read="snowflake")
    except sqlglot.errors.SqlglotError:
        return {}
    passthrough: dict[str, str] = {}  # CTE name -> source it is `select *` of
    computed: dict[str, set[str]] = {}  # CTE name -> columns it adds next to the star
    lineage: dict[str, dict[str, tuple[str, str]]] = {}  # CTE name -> its outputs
    last: dict[str, tuple[str, str]] = {}
    positional: dict[int, list] = {}  # select -> source column behind each projection, by position
    for scope in traverse_scope(tree):
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        # which source (if any) each alias in this scope stands for, and prior renames
        direct: dict[str, str] = {}
        added: dict[str, set[str]] = {}
        renames: dict[str, dict[str, tuple[str, str]]] = {}
        selected = {alias: src for alias, (_, src) in scope.selected_sources.items()}
        # inside LATERAL FLATTEN(input => t.col) the outer table is visible too
        selected.update({a: src for a, src in getattr(scope, "lateral_sources", {}).items() if a not in selected})
        for alias, node in selected.items():
            if isinstance(node, exp.Table):
                if _table_id(node) in sources:
                    direct[alias.upper()] = _table_id(node)
                elif node.name.upper() in passthrough:
                    direct[alias.upper()] = passthrough[node.name.upper()]
                    added[alias.upper()] = computed.get(node.name.upper(), set())
                elif models and _table_id(node) in models:  # another model: follow its lineage
                    renames[alias.upper()] = models[_table_id(node)]
            else:  # a CTE or subquery scope
                name = alias.upper()
                if name in passthrough:
                    direct[name] = passthrough[name]
                    added[name] = computed.get(name, set())
                elif name in lineage:
                    renames[name] = lineage[name]
        # a lateral FLATTEN next to the table only contributes its own fixed columns
        laterals = {a.upper() for a, src in selected.items() if getattr(src, "is_udtf", False)}
        others = {a.upper() for a in selected} - set(direct) - laterals
        only = next(iter(direct.values())) if len(direct) == 1 and not renames and not others else None

        def resolve(col: exp.Column):
            table = col.table.upper()
            if not table and laterals and col.name.upper() in FLATTEN_COLUMNS:
                return None
            if table in direct or (not table and only):
                owner = table or next(iter(direct))
                if col.name.upper() in added.get(owner, ()):
                    return None  # computed in the CTE (a row_number, say), not a source column
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
                use, parent = sources[hit[0]], col.parent
                use.add(hit[1], cast.to.sql(dialect="duckdb") if cast else None)
                if isinstance(parent, (exp.Sum, exp.Avg, exp.Add, exp.Sub, exp.Mul, exp.Div)):
                    use.numbers.add(hit[1])
                if isinstance(parent, exp.ParseJSON):
                    use.json_text.add(hit[1])  # parse_json(col):a.b  ->  col is text holding {"a": {"b": ...}}
                    col, parent = parent, parent.parent
                    if not (isinstance(parent, exp.JSONExtract) and parent.this is col):
                        use.paths.setdefault(hit[1], {})
                elif isinstance(parent, (exp.EQ, exp.NEQ, exp.Like, exp.ILike, exp.In)):
                    other = parent.expression if not isinstance(parent, exp.In) else (parent.expressions or [None])[0]
                    other = parent.this if other is col else other
                    if isinstance(other, exp.Literal) and other.is_string and not re.fullmatch(r"[\d\s:.\-/T+Z]*", other.name):
                        use.texts.add(hit[1])  # id = 'USLACKBOT': not a number
                if isinstance(parent, exp.JSONExtract) and parent.this is col:
                    # v:a.b::int tells us the column is a variant holding {"a": {"b": <int>}}
                    keys = [k.name for k in parent.expression.expressions if isinstance(k, exp.JSONPathKey)]
                    outer = parent.parent
                    hint = outer.to.sql(dialect="duckdb") if isinstance(outer, (exp.Cast, exp.TryCast)) else None
                    if keys:
                        use.paths.setdefault(hit[1], {})[".".join(keys)] = hint
                    else:
                        use.arrays.add(hit[1])
                elif isinstance(parent, exp.Bracket) or col.find_ancestor(exp.Explode, exp.Lateral, exp.Unnest) is not None:
                    use.arrays.add(hit[1])
                elif isinstance(parent, (exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate)) and parent.this is col \
                        and isinstance(parent.args.get("format"), exp.Literal):
                    use.formats[hit[1]] = parent.args["format"].name  # a text column holding dates

        outputs: dict[str, tuple[str, str]] = {}
        stars = [p for p in select.expressions if isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))]
        positional[id(select)] = by_position = []
        for proj in select.expressions:
            inner = proj.unalias()
            while isinstance(inner, (exp.Cast, exp.TryCast, exp.Paren)):
                inner = inner.this
            hit = resolve(inner) if isinstance(inner, exp.Column) and not isinstance(inner.this, exp.Star) else None
            by_position.append(hit)
            if hit:
                outputs[proj.alias_or_name.upper()] = hit
        name = scope.expression.parent.alias.upper() if isinstance(scope.expression.parent, exp.CTE) else None
        if stars and only and not select.args.get("joins"):
            if name:  # `select *, row_number() ... as rn from source` still passes every column through
                passthrough[name] = only
                computed[name] = {p.alias_or_name.upper() for p in select.expressions if p not in stars} \
                    | added.get(next(iter(direct)), set())
            else:
                sources[only].star = True
        if stars and len(renames) == 1 and len(selected) == 1:
            outputs = {**next(iter(renames.values())), **outputs}
        if name:
            lineage[name] = outputs
        last = outputs
    if links is not None:
        # UNION branches line up by position, and aligned columns must share a type
        def branches(node):
            node = node.unnest()
            return branches(node.left) + branches(node.right) if isinstance(node, exp.SetOperation) else [node]

        for union in tree.find_all(exp.SetOperation):
            if isinstance(union.parent, exp.SetOperation):
                continue
            rows = [positional.get(id(b), []) for b in branches(union)]
            for aligned in zip(*rows):
                group = {hit for hit in aligned if hit}
                if len(group) > 1:
                    links.append(group)
    return last


def guess(column: str, hint: str | None) -> dict:
    """A generator for a column we know only by name and (maybe) a cast."""
    c, h = column.upper(), (hint or "").upper()
    if h in ("VARIANT", "JSON", "OBJECT", "STRUCT") or h.startswith(("OBJECT", "MAP")):
        return {"gen": "object", "fields": {}}  # col::variant: semi-structured, shape unknown
    if h == "ARRAY" or h.endswith("[]"):
        return {"gen": "array"}
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
    if h in ("VARCHAR", "TEXT"):
        return {"gen": "template", "template": f"{column.lower()}_{{hex}}"}
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
    links: list[set[tuple[str, str]]] = []

    def scan(uid: str) -> None:
        node = manifest["nodes"].get(uid)
        if uid in outputs or not node or node["resource_type"] != "model":
            return
        outputs[uid] = {}  # guards against cycles
        for parent in node["depends_on"]["nodes"]:
            scan(parent)
        if node.get("compiled_code"):
            outputs[uid] = scan_model(node["compiled_code"], uses, by_relation, links)
        if node.get("relation_name"):
            by_relation[node["relation_name"].replace('"', "").upper()] = outputs[uid]

    for uid in manifest["nodes"]:
        scan(uid)

    # what is known about one column of a union holds for the columns aligned with it
    for _ in range(3):
        for group in links:
            members = [(uses[s], c) for s, c in group if s in uses]
            if any(c in u.numbers for u, c in members):
                for u, c in members:
                    u.numbers.add(c)
            if any(c in u.texts for u, c in members):
                for u, c in members:
                    u.texts.add(c)
            hint = next((u.columns.get(c) for u, c in members if u.columns.get(c)), None)
            for u, c in members:
                if hint and not u.columns.get(c):
                    u.columns[c] = hint

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
            if col in use.texts and not hint:
                hint = "VARCHAR"
            if col in use.numbers and not hint and col not in use.texts:
                hint = "BIGINT"
            if col == key:
                numeric = (hint or "").upper().startswith(("BIGINT", "INT", "DECIMAL", "NUMBER")) or (not hint and col.endswith("ID"))
                out[col] = {"gen": "sequence" if numeric else "hash_id"}
            elif (name, col) in fks:
                out[col] = {"gen": "fk", "ref": fks[(name, col)][0], "skew": 1.5}
            elif (name, col) in accepted:
                values = accepted[(name, col)]
                out[col] = {"gen": "categorical", "values": {v: round(1 / len(values), 4) for v in values}}
            elif col in use.paths:
                out[col] = {"gen": "object", "fields": {path: guess(path.split(".")[-1], h) for path, h in sorted(use.paths[col].items())}}
                if "id" in out[col]["fields"]:
                    out[col]["keyed_by"] = "id"  # {"id": 7, "login": "x"}: the login belongs to the id
                if col in use.json_text:
                    out[col]["type"] = "VARCHAR"
            elif col in use.arrays:
                out[col] = {"gen": "array"}
            elif col in use.formats:
                out[col] = {"gen": "time_offset", "format": use.formats[col], **WEEK}
            elif col == loaded:
                out[col] = {"gen": "loaded_at"}
            elif col == event:
                out[col] = {"gen": "event_time"}
            elif col == updated:
                out[col] = {"gen": "updated_at"}
            else:
                out[col] = guess(col, hint)
        # author_id + author_username: the name belongs to the id, so it must not vary between rows
        for col, d in out.items():
            if col.endswith("_ID") and d["gen"] in ("dist", "fk", "template"):
                for sibling, sd in out.items():
                    if sibling != col and sibling.startswith(col[:-2]) and sd["gen"] in ("template", "categorical"):
                        sd["follows"] = col
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


_MISSING_ENV = re.compile(r"Env var required but not provided: '(\w+)'")


def provide_env(project) -> dict[str, str]:
    """Projects often read database and schema names from the environment. Parse until dbt
    stops asking, giving each missing variable a placeholder named after itself."""
    import os

    provided: dict[str, str] = {}
    for _ in range(200):
        if project.invoke(["parse"], quiet=True):
            break
        missing = _MISSING_ENV.search(project.last_error)
        if not missing:
            raise RuntimeError(f"dbt could not parse the project:\n{project.last_error}")
        var = missing.group(1)
        value = re.sub(r"^(DBT_)?(SNOWFLAKE_)?((DATABASE|SCHEMA|DB)_)?", "", var.upper()) or "FAUXFLAKE"
        provided[var] = os.environ[var] = value
    return provided


def write_spec(project, path: Path, database: str) -> list[str]:
    """`project` is a DbtProject pointed at a running emulator."""
    env = provide_env(project)
    project.invoke(["compile"], quiet=True)  # best effort: models that fail to compile are skipped
    manifest = json.loads((project.target_path / "manifest.json").read_text())
    spec, notes = build_spec(manifest, database)
    if env:
        spec["dbt"]["env"] = env
        notes.insert(0, f"set placeholders for {len(env)} environment variable(s) the project requires (dbt.env in the spec)")
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
