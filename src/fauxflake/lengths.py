"""The VARCHAR length Snowflake gives each text column a CREATE TABLE ... AS SELECT (or a view) makes: DESCRIBE says
VARCHAR(5) for a column read from a VARCHAR(5) one, VARCHAR(7) for 'initial', VARCHAR(17) for a VARCHAR(5) || a
VARCHAR(12). dbt reads these: an incremental model widens a column to the length its new rows have.

Every rule here was measured on a live account (2026-11-26); anything not covered is VARCHAR(16777216), as Snowflake
gives most functions (REPLACE, LPAD, TO_CHAR, casts to VARCHAR without a length)."""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope

MAX = 16777216
_SAME = (exp.Trim, exp.Lower, exp.Reverse, exp.Substring, exp.Left, exp.Right, exp.SplitPart, exp.Max, exp.Min,
         exp.AnyValue, exp.FirstValue, exp.LastValue, exp.Mode, exp.Collate)
_SAME_NAMES = {"LTRIM", "RTRIM", "SUBSTR", "SPLIT_PART", "MODE", "LAG", "NTH_VALUE", "ANY_VALUE", "REVERSE", "LOWER", "LEFT", "RIGHT",
               "FIRST_VALUE", "LAST_VALUE", "COLLATE"}
_FIXED = {"MD5": 32, "MD5_HEX": 32, "SHA2": 128, "SHA2_HEX": 128, "UUID_STRING": 36}


def _text_types() -> tuple:
    T = exp.DataType.Type
    return (T.VARCHAR, T.CHAR, T.TEXT, T.NVARCHAR, T.NCHAR)


def _sum(parts: list[int | None]) -> int | None:
    return None if any(p is None for p in parts) else min(sum(parts), MAX)


def _widest(parts: list[int | None]) -> int | None:
    return None if not parts or any(p is None for p in parts) else max(parts)


def length(e: exp.Expression, column: callable) -> int | None:  # noqa: C901, PLR0911, PLR0912
    """The length of a text expression, or None for VARCHAR(16777216) (or not text). `column` gives a column's."""
    if isinstance(e, exp.Alias):
        return length(e.this, column)
    if isinstance(e, exp.Paren):
        return length(e.this, column)
    if isinstance(e, exp.Column):
        return column(e)
    if isinstance(e, exp.Literal):
        return max(1, len(e.this)) if e.is_string else None
    if isinstance(e, (exp.Cast, exp.TryCast)):
        if e.to.this not in _text_types():
            return None
        sized = [int(p.this.name) for p in e.to.expressions if isinstance(p, exp.DataTypeParam) and p.this.is_int]
        if sized:
            return sized[0]
        return 1 if e.to.this == exp.DataType.Type.CHAR else None
    if isinstance(e, (exp.DPipe, exp.Concat)):
        parts = [e.this, e.expression] if isinstance(e, exp.DPipe) else list(e.expressions)
        return _sum([length(p, column) for p in parts])
    if isinstance(e, (exp.Upper, exp.Initcap)):
        inner = length(e.this, column)
        return None if inner is None else min(inner * 3, MAX)
    if isinstance(e, exp.Case):
        branches = [i.args.get("true") for i in e.args.get("ifs") or []] + [e.args.get("default")]
        return _widest([length(b, column) if b is not None else None for b in branches])
    if isinstance(e, exp.If):
        return _widest([length(e.args.get("true"), column), length(e.args.get("false"), column) if e.args.get("false") else None])
    if isinstance(e, (exp.Coalesce, exp.Greatest, exp.Least)):
        return _widest([length(a, column) for a in [e.this, *e.expressions]])
    if isinstance(e, exp.DecodeCase) and len(e.expressions) >= 3:
        rest = e.expressions[1:]
        values = [rest[i] for i in range(1, len(rest), 2)] + ([rest[-1]] if len(rest) % 2 else [])
        return _widest([length(v, column) for v in values])
    if isinstance(e, exp.Nullif):
        return length(e.this, column)
    if isinstance(e, exp.Window):
        return length(e.this, column)
    if isinstance(e, (exp.Lag, exp.Lead)):
        default = e.args.get("default")
        return _widest([length(e.this, column)] + ([length(default, column)] if default is not None else []))
    if isinstance(e, exp.MD5):
        return 32
    if isinstance(e, (exp.SHA2,)):
        return 128
    if isinstance(e, exp.Hex) or (isinstance(e, exp.Anonymous) and e.name.upper() == "HEX_ENCODE"):
        inner = length(e.this if not isinstance(e, exp.Anonymous) else e.expressions[0], column)
        return None if inner is None else min(inner * 8, MAX)
    if isinstance(e, (exp.ToBase64, exp.Base64Encode)) or (isinstance(e, exp.Anonymous) and e.name.upper() == "BASE64_ENCODE"):
        inner = length(e.this if not isinstance(e, exp.Anonymous) else e.expressions[0], column)
        return None if inner is None else min(4 * -(-inner * 4 // 3), MAX)
    if isinstance(e, _SAME):
        return length(e.this, column)
    if isinstance(e, exp.Anonymous):
        name = e.name.upper()
        if name in _FIXED:
            return _FIXED[name]
        if name in ("NVL", "IFNULL", "GREATEST", "LEAST"):
            return _widest([length(a, column) for a in e.expressions])
        if name in ("IFF",) and len(e.expressions) == 3:
            return _widest([length(a, column) for a in e.expressions[1:]])
        if name == "DECODE" and len(e.expressions) >= 3:
            rest = e.expressions[1:]
            values = [rest[i] for i in range(1, len(rest), 2)] + ([rest[-1]] if len(rest) % 2 else [])
            return _widest([length(v, column) for v in values])
        if name in ("UPPER", "INITCAP") and e.expressions:
            inner = length(e.expressions[0], column)
            return None if inner is None else min(inner * 3, MAX)
        if name in _SAME_NAMES and e.expressions:
            return length(e.expressions[0], column)
    if isinstance(e, exp.Uuid):
        return 36
    return None


def derive(sql: str, schema: dict, catalog: str | None = None, db: str | None = None) -> list[int | None] | None:
    """The length of each column the query makes (None: VARCHAR(16777216)), or None when it cannot be told.
    `schema`: {database: {schema: {table: {column: 'VARCHAR(5)' | ...}}}}."""
    tree = sqlglot.parse_one(sql, read="snowflake")
    # (lateral aliases are not expanded: `a + 1 as b, b * 2 as c` reads b's length once, where expanding them grows
    # exponentially on a chain)
    tree = qualify(tree, schema=schema, dialect="snowflake", catalog=catalog, db=db, validate_qualify_columns=False, identify=False,
                   quote_identifiers=False, expand_alias_refs=False)
    found: dict[int, list[int | None]] = {}
    names: dict[int, list[str]] = {}

    def outputs(scope: Scope) -> tuple[list[str], list[int | None]]:
        key = id(scope.expression)
        if key in found:
            return names[key], found[key]
        if isinstance(scope.expression, exp.SetOperation):
            left, right = (outputs(s) for s in scope.set_operation_scopes[:2])
            # (a NULL in one branch takes the other's length)
            nulls = [[isinstance(p.unalias(), exp.Null) for p in s.expression.selects] if isinstance(s.expression, exp.Select) else []
                     for s in scope.set_operation_scopes[:2]]
            merged = []
            for i in range(min(len(left[1]), len(right[1]))):
                pair = [v for v, null in ((left[1][i], nulls[0][i] if i < len(nulls[0]) else False),
                                          (right[1][i], nulls[1][i] if i < len(nulls[1]) else False)) if not null]
                merged.append(_widest(pair) if pair else None)
            result = (left[0], merged)
        else:
            select = scope.expression
            earlier: dict[str, int | None] = {}  # the lengths of this SELECT's aliases so far (a lateral alias reads one)
            sizes = []
            for p in select.selects:
                size = length(p, lambda c, s=scope, e=earlier: _column(c, s, e))
                earlier.setdefault(p.alias_or_name.upper(), size)
                sizes.append(size)
            result = ([p.alias_or_name for p in select.selects], sizes)
        names[key], found[key] = result
        return result

    def _column(c: exp.Column, scope: Scope, earlier: dict | None = None) -> int | None:
        source = scope.sources.get(c.table)
        if source is None and earlier and c.name.upper() in earlier:
            return earlier[c.name.upper()]
        if isinstance(source, Scope):
            cols, sizes = outputs(source)
            upper = [n.upper() for n in cols]
            return sizes[upper.index(c.name.upper())] if c.name.upper() in upper else None
        if isinstance(source, exp.Table):
            kind = schema.get(source.catalog, {}).get(source.db, {}).get(source.name, {}).get(c.name)
            sized = re.fullmatch(r"VARCHAR\((\d+)\)", kind or "")
            return int(sized[1]) if sized and int(sized[1]) < MAX else None
        return None

    root = None
    for scope in traverse_scope(tree):
        outputs(scope)
        root = scope
    return outputs(root)[1] if root is not None else None


def note(cursor, command: str) -> None:  # noqa: ANN001
    """After a CREATE TABLE / VIEW ... AS SELECT: record each text column's length, as Snowflake derives it."""
    if not isinstance(command, str) or len(command) > 200_000 or not re.match(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*create\b", command):
        return  # (a statement of hundreds of kilobytes is generated, a pivot over many values: its columns keep the default)
    try:
        tree = sqlglot.parse_one(command, read="snowflake")
    except Exception:  # noqa: BLE001
        return
    if not isinstance(tree, exp.Create) or str(tree.args.get("kind") or "").upper() not in ("TABLE", "VIEW"):
        return
    query = tree.expression
    while isinstance(query, exp.Subquery):  # (`as ( select ... )`, as dbt writes it)
        query = query.this
    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
    if not isinstance(query, exp.Query) or not isinstance(target, exp.Table) or isinstance(tree.this, exp.Schema):
        return
    duck, conn = cursor._duck_conn, cursor._conn
    database = (target.catalog or conn.database or "").upper() if not target.args.get("catalog") or not target.args["catalog"].quoted else target.catalog
    schema_name = (target.db or conn.schema or "").upper() if not target.args.get("db") or not target.args["db"].quoted else target.db
    name = target.name if target.this.quoted else target.name.upper()
    made = duck.execute("select column_name, data_type from duckdb_columns() where database_name = ? and schema_name = ? and table_name = ? "
                        "order by column_index", [database, schema_name, name]).fetchall()
    if not made:
        return
    try:
        schema = _schema_of(duck, query, conn)
        sizes = derive(query.sql("snowflake"), schema, (conn.database or "").upper(), (conn.schema or "").upper())
    except Exception:  # noqa: BLE001 - a length is a description, never a reason to fail the statement
        return
    if sizes is None or len(sizes) != len(made):
        return
    from fakesnow import info_schema

    lengths = [(column, size or MAX) for (column, kind), size in zip(made, sizes) if kind == "VARCHAR"]
    if lengths:
        duck.execute(info_schema.insert_text_lengths_sql(database, schema_name, name.replace("'", "''"),
                                                         [(c.replace("'", "''"), s) for c, s in lengths]))


def _schema_of(duck, query: exp.Expression, conn) -> dict:  # noqa: ANN001
    """The columns of each table the query reads, as qualify wants them, with their VARCHAR lengths."""
    schema: dict = {}
    ctes = {c.alias_or_name.upper() for c in query.find_all(exp.CTE)}
    for table in query.find_all(exp.Table):
        if not table.name or (not table.db and table.name.upper() in ctes):
            continue
        db = table.catalog.upper() if table.catalog else (conn.database or "").upper()
        sch = table.db.upper() if table.db else (conn.schema or "").upper()
        name = table.name if table.this.quoted else table.name.upper()
        rows = duck.execute(
            "select c.column_name, c.data_type, ext.ext_character_maximum_length from duckdb_columns() c "
            "left join _fs_global._fs_information_schema._fs_columns_ext ext on ext.ext_table_catalog = c.database_name "
            "and ext.ext_table_schema = c.schema_name and ext.ext_table_name = c.table_name and ext.ext_column_name = c.column_name "
            "where c.database_name = ? and c.schema_name = ? and c.table_name = ? order by c.column_index", [db, sch, name]).fetchall()
        if rows:
            schema.setdefault(db, {}).setdefault(sch, {})[name] = {
                column: f"VARCHAR({size or MAX})" if kind == "VARCHAR" else "INT" for column, kind, size in rows}
    return schema
