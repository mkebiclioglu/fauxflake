"""PRIMARY KEY, UNIQUE and FOREIGN KEY constraints, as Snowflake keeps them: recorded, shown, never enforced.

Snowflake enforces only NOT NULL: a primary key over NULLs and duplicates is accepted, and so is every row inserted
after it. DuckDB enforces all of them (and names them its own way), so none reaches DuckDB. Each is a row of
`_fx_constraints`, keyed by the DuckDB oid of its table: a table replaced (`create or replace`) or dropped is a new
oid, and its constraints are gone, as on Snowflake; a renamed one keeps them. What was measured on a live account:

* `ALTER TABLE t ADD [CONSTRAINT n] PRIMARY KEY | UNIQUE | FOREIGN KEY (cols) [REFERENCES r [(cols)]] [RELY | NORELY]`,
  with `SYS_CONSTRAINT_<uuid>` as the name of one not named; a primary key added so leaves its columns nullable,
* inline constraints of CREATE TABLE (a column's PRIMARY KEY makes it NOT NULL there),
* `MODIFY | ALTER CONSTRAINT n RELY | NORELY`, `RENAME CONSTRAINT a TO b`, `DROP CONSTRAINT n`, `DROP PRIMARY KEY`,
  `DROP UNIQUE (cols)`, `DROP FOREIGN KEY (cols)` (dropping a key drops the foreign keys that point at it),
* `MODIFY | ALTER [COLUMN] c SET NOT NULL, [COLUMN] d DROP NOT NULL, ...`,
* SHOW PRIMARY | UNIQUE | IMPORTED | EXPORTED KEYS [IN TABLE | SCHEMA | DATABASE | ACCOUNT x],
* INFORMATION_SCHEMA.TABLE_CONSTRAINTS, REFERENTIAL_CONSTRAINTS and TABLE_PRIVILEGES (each table's OWNERSHIP),
* the errors: 1450-1459 and 1475 for the constraint itself, 904 for a column of its own table, 1431 for one of
  the referenced table, 2003 for a table that is not there, 2001 for MODIFY of a constraint that is not.
"""

from __future__ import annotations

import re
import uuid

from .account import _ID, _LEAD, _NAME, _error, _select, _status, without_trailing_comments

_NS = "_fs_global.main"
TABLE = f"{_NS}._fx_constraints"
LIVE = f"{_NS}._fx_live_constraints"

SETUP = [
    f"""create table if not exists {TABLE} (oid bigint, ref_oid bigint, name varchar, kind varchar, columns varchar[],
          ref_columns varchar[], ref_name varchar, rely boolean, made timestamptz, altered timestamptz)""",
    # the constraints of the tables that are there, under the names those have now
    f"""create or replace view {LIVE} as
          select t.database_name, t.schema_name, t.table_name, c.*,
                 r.database_name as ref_database, r.schema_name as ref_schema, r.table_name as ref_table
          from {TABLE} c join duckdb_tables() t on t.table_oid = c.oid left join duckdb_tables() r on r.table_oid = c.ref_oid""",
]

# INFORMATION_SCHEMA views (read through `views()`, a database's own rows)
_VIEWS = {
    "TABLE_CONSTRAINTS": f"""select database_name as CONSTRAINT_CATALOG, schema_name as CONSTRAINT_SCHEMA, name as CONSTRAINT_NAME,
          database_name as TABLE_CATALOG, schema_name as TABLE_SCHEMA, table_name as TABLE_NAME, kind as CONSTRAINT_TYPE,
          'NO' as IS_DEFERRABLE, 'YES' as INITIALLY_DEFERRED, 'NO' as ENFORCED, null::varchar as COMMENT,
          made as CREATED, altered as LAST_ALTERED, case when rely then 'YES' else 'NO' end as RELY
          from {LIVE} where database_name = {{db}}""",
    "REFERENTIAL_CONSTRAINTS": f"""select database_name as CONSTRAINT_CATALOG, schema_name as CONSTRAINT_SCHEMA, name as CONSTRAINT_NAME,
          ref_database as UNIQUE_CONSTRAINT_CATALOG, ref_schema as UNIQUE_CONSTRAINT_SCHEMA, ref_name as UNIQUE_CONSTRAINT_NAME,
          'FULL' as MATCH_OPTION, 'NO ACTION' as UPDATE_RULE, 'NO ACTION' as DELETE_RULE, null::varchar as COMMENT,
          made as CREATED, altered as LAST_ALTERED
          from {LIVE} where kind = 'FOREIGN KEY' and database_name = {{db}}""",
    # the role that made a table owns it (the emulator's tables are all made by the session's role)
    # the owner's OWNERSHIP of each table and view, then what was granted on it (relations.py keeps both)
    "TABLE_PRIVILEGES": f"""select coalesce(r.owner, {_NS}._sf_current_role()) as GRANTOR, coalesce(r.owner, {_NS}._sf_current_role()) as GRANTEE,
          'ROLE' as GRANTED_TO, o.database_name as TABLE_CATALOG, o.schema_name as TABLE_SCHEMA, o.table_name as TABLE_NAME,
          'OWNERSHIP' as PRIVILEGE_TYPE, 'YES' as IS_GRANTABLE, 'NO' as WITH_HIERARCHY, coalesce(r.created_on, {_NS}._fx_started()) as CREATED
          from (select database_name, schema_name, table_name from duckdb_tables() where not internal
                union all select database_name, schema_name, view_name from duckdb_views() where not internal) o
          left join {_NS}._fx_relations r on r.database_name = o.database_name and r.schema_name = o.schema_name and r.name = o.table_name
          where o.database_name = {{db}} and o.schema_name not ilike '_fs%' and o.schema_name <> 'information_schema'
          union all
          select g.granted_by, g.grantee, 'ROLE', g.database_name, g.schema_name, g.name, g.privilege,
                 case when g.grant_option then 'YES' else 'NO' end, 'NO', g.created_on
          from {_NS}._fx_grants g where g.database_name = {{db}}""",
}


def views(name: str, database: str | None) -> str | None:
    """The SELECT for INFORMATION_SCHEMA.<name> of a database (None: not one of these views)."""
    body = _VIEWS.get(name.upper())
    if body is None:
        return None
    return body.replace("{db}", "'" + database.replace("'", "''") + "'" if database else "current_database()")


_ALTER = re.compile(_LEAD + r"alter\s+(?:iceberg\s+)?table\s+(?:if\s+exists\s+)?(?P<table>" + _NAME + r")\s+(?P<rest>.*?)\s*;?\s*$")
_ADD = re.compile(r"(?is)add\s+(?:constraint\s+(?P<name>" + _ID + r")\s+)?(?P<kind>primary\s+key|unique|foreign\s+key)\s*"
                  r"\((?P<cols>[^)]*)\)(?:\s*references\s+(?P<ref>" + _NAME + r")\s*(?:\((?P<refcols>[^)]*)\))?)?(?P<props>.*)$")
_MODIFY = re.compile(r"(?is)(?:modify|alter)\s+constraint\s+(?P<name>" + _ID + r")\s*(?P<props>.*)$")
_RENAME = re.compile(r"(?is)rename\s+constraint\s+(?P<old>" + _ID + r")\s+to\s+(?P<new>" + _ID + r")\s*$")
_DROP_NAMED = re.compile(r"(?is)drop\s+constraint\s+(?P<name>" + _ID + r")(?:\s+(?:cascade|restrict))?\s*$")
_DROP_KIND = re.compile(r"(?is)drop\s+(?P<kind>primary\s+key|unique|foreign\s+key)\s*(?:\((?P<cols>[^)]*)\))?(?:\s+(?:cascade|restrict))?\s*$")
_NULLS = re.compile(r"(?is)(?:modify|alter)\s+(?!constraint\b)(?P<clauses>.*\bnot\s+null\b.*)$")
_CLAUSE = re.compile(r"(?is)\s*(?:column\s+)?(?P<col>" + _ID + r")\s+(?P<what>set|drop)\s+not\s+null\s*$")
_SHOW = re.compile(_LEAD + r"show\s+(?:terse\s+)?(?P<kind>primary|unique|imported|exported)\s+keys"
                   r"(?:\s+in\s+(?P<scope>account|database|schema|table)(?:\s+(?P<name>" + _NAME + r"))?)?\s*;?\s*$")
_SHOW_LIKE = re.compile(_LEAD + r"show\s+(?:terse\s+)?(?:primary|unique|imported|exported)\s+keys\s+(?P<like>like)\b")
_CREATE = re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?(?:(?:local|global)\s+)?(?:temp|temporary|volatile|transient)?\s*"
                     r"(?:iceberg\s+)?table\b.*?\b(?:primary\s+key|unique|references|foreign\s+key)\b")

_KIND = {"primary key": "PRIMARY KEY", "unique": "UNIQUE", "foreign key": "FOREIGN KEY"}


def _ident(text: str) -> str:
    text = text.strip()
    return text[1:-1].replace('""', '"') if text.startswith('"') else text.upper()


def _parts(name: str) -> list[str]:
    return [_ident(p) for p in re.findall(_ID, name)]


def _columns(text: str | None) -> list[str]:
    return [_ident(c) for c in re.findall(_ID, text or "")]


def _rely(props: str, default: bool | None) -> bool | None:
    words = {w.upper() for w in re.findall(r"[A-Za-z]+", props or "")}
    return False if "NORELY" in words else True if "RELY" in words else default


def _where(cursor) -> tuple[str, str]:  # noqa: ANN001
    db, schema = cursor._duck_conn.execute("select current_database(), current_schema()").fetchone()
    return db, schema


def _table(cursor, name: str, written: str | None = None) -> tuple[int, str, str, str]:  # noqa: ANN001
    """oid, database, schema and name of a table, or Snowflake's error for one that is not there."""
    parts = _parts(name)
    db, schema = _where(cursor)
    full = [db, schema][: 3 - len(parts)] + parts if len(parts) < 3 else parts[-3:]
    found = cursor._duck_conn.execute(
        "select table_oid, database_name, schema_name, table_name from duckdb_tables() where database_name = ? and "
        "schema_name = ? and table_name = ?", full).fetchone()
    if found is None:
        shown = ".".join(full) if written is None else written
        raise _error(f"SQL compilation error:\nTable '{shown}' does not exist or not authorized.", errno=2003, sqlstate="42S02")
    return found


def _is_view(cursor, name: str) -> bool:  # noqa: ANN001
    parts = _parts(name)
    db, schema = _where(cursor)
    full = [db, schema][: 3 - len(parts)] + parts if len(parts) < 3 else parts[-3:]
    return cursor._duck_conn.execute("select count(*) from duckdb_views() where database_name = ? and schema_name = ? "
                                     "and view_name = ?", full).fetchone()[0] > 0


def _names(cursor, oid: int) -> list[str]:  # noqa: ANN001
    return [r[0] for r in cursor._duck_conn.execute(
        "select column_name from duckdb_columns() where table_oid = ? order by column_index", [oid]).fetchall()]


def _rows(cursor, where: str, args: list) -> list[tuple]:  # noqa: ANN001
    return cursor._duck_conn.execute(
        f"select name, kind, columns, ref_oid, ref_columns, rely from {TABLE} where {where}", args).fetchall()


def _fail(errno: int, text: str, sqlstate: str = "42601"):  # noqa: ANN202
    return _error("SQL compilation error:\n" + text, errno=errno, sqlstate=sqlstate)


def add(cursor, oid: int, table: str, name: str | None, kind: str, cols: list[str], ref: str | None,  # noqa: ANN001, PLR0913
        refcols: list[str], rely: bool) -> None:
    """Record one constraint on the table of `oid` (named `table`), as Snowflake checks it."""
    duck = cursor._duck_conn
    have = _names(cursor, oid)
    for c in cols:
        if c not in have:
            shown = f'"{c}"' if c != c.upper() or not re.fullmatch(r"[A-Z_][A-Z0-9_$]*", c) else c
            raise _fail(904, f"invalid identifier '{shown}'", "42000")
    existing = _rows(cursor, "oid = ?", [oid])
    named = name or f"SYS_CONSTRAINT_{uuid.uuid4()}"
    if any(n == named for n, *_ in existing):
        raise _fail(1452, f"constraint '{named}' already exists for table '{table}'")
    if kind == "PRIMARY KEY" and any(k == "PRIMARY KEY" for _, k, *_ in existing):
        raise _fail(1451, f"primary key already exists for table '{table}'")
    if kind != "FOREIGN KEY":
        same = next((k for _, k, c, *_ in existing if k != "FOREIGN KEY" and list(c) == cols), None)
        if same == "PRIMARY KEY":
            raise _fail(1459, "Primary Key with the signature already exists on the table")
        if same is not None:
            raise _fail(1455, f"constraint with the same signature already exists on table '{table}'")
    ref_oid, ref_name = None, None
    if kind == "FOREIGN KEY":
        ref_oid, _db, _schema, ref_table = _table(cursor, ref or "")
        keys = [(n, k, list(c)) for n, k, c, *_ in _rows(cursor, "oid = ? and kind <> 'FOREIGN KEY'", [ref_oid])]
        if not refcols:
            match = next(((n, c) for n, k, c in keys if k == "PRIMARY KEY" and len(c) == len(cols)), None)
            if match is None:
                raise _fail(1457, "could not find matching constraints for the foreign key")
            ref_name, refcols = match
        else:
            theirs = _names(cursor, ref_oid)
            for c in refcols:
                if c not in theirs:
                    raise _fail(1431, f"column '{c}' does not exist")
            if len(refcols) != len(cols):
                raise _fail(1475, "Number of columns in foreign key specification does not match number of columns in "
                                  "foreign key reference")
            ref_name = next((n for n, _k, c in keys if c == refcols), None)
            if ref_name is None:
                raise _fail(1456, "primary or unique key does not exist for the columns referenced by the foreign key")
    duck.execute(f"insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?, get_current_timestamp(), get_current_timestamp())",
                 [oid, ref_oid, named, kind, cols, refcols or None, ref_name, bool(rely)])


def _drop(cursor, oid: int, name: str) -> None:  # noqa: ANN001
    """One constraint, and the foreign keys that point at it when it is a key."""
    cursor._duck_conn.execute(f"delete from {TABLE} where (ref_oid = ? and ref_name = ?) or (oid = ? and name = ?)",
                              [oid, name, oid, name])


def _alter(cursor, original, m):  # noqa: ANN001, ANN202, C901, PLR0911, PLR0912
    rest = m.group("rest")
    if a := _ADD.match(rest):
        oid, *_x, table = _table(cursor, m.group("table"), written=_written(cursor, m.group("table")))
        add(cursor, oid, table, _ident(a.group("name")) if a.group("name") else None,
            _KIND[re.sub(r"\s+", " ", a.group("kind").lower())], _columns(a.group("cols")), a.group("ref"),
            _columns(a.group("refcols")), bool(_rely(a.group("props"), False)))
        return _status(cursor, original, "Statement executed successfully.")
    if a := _MODIFY.match(rest):
        oid, *_x = _table(cursor, m.group("table"), written=_written(cursor, m.group("table")))
        name = _ident(a.group("name"))
        if not _rows(cursor, "oid = ? and name = ?", [oid, name]):
            raise _error(f"SQL compilation error:\nObject '{name}' does not exist or not authorized.", errno=2001)
        rely = _rely(a.group("props"), None)
        if rely is not None:
            cursor._duck_conn.execute(f"update {TABLE} set rely = ?, altered = get_current_timestamp() where oid = ? and name = ?",
                                      [rely, oid, name])
        return _status(cursor, original, "Statement executed successfully.")
    if a := _RENAME.match(rest):
        oid, *_x = _table(cursor, m.group("table"), written=_written(cursor, m.group("table")))
        old, new = _ident(a.group("old")), _ident(a.group("new"))
        if not _rows(cursor, "oid = ? and name = ?", [oid, old]):
            raise _fail(1450, f"constraint '{a.group('old')}' does not exist")
        cursor._duck_conn.execute(f"update {TABLE} set name = ?, altered = get_current_timestamp() where oid = ? and name = ?", [new, oid, old])
        cursor._duck_conn.execute(f"update {TABLE} set ref_name = ? where ref_oid = ? and ref_name = ?", [new, oid, old])
        return _status(cursor, original, "Statement executed successfully.")
    if a := _DROP_NAMED.match(rest):
        oid, *_x = _table(cursor, m.group("table"), written=_written(cursor, m.group("table")))
        name = _ident(a.group("name"))
        if not _rows(cursor, "oid = ? and name = ?", [oid, name]):
            raise _fail(1450, f"constraint '{a.group('name')}' does not exist")
        _drop(cursor, oid, name)
        return _status(cursor, original, "Statement executed successfully.")
    if a := _DROP_KIND.match(rest):
        oid, *_x = _table(cursor, m.group("table"), written=_written(cursor, m.group("table")))
        kind, cols = _KIND[re.sub(r"\s+", " ", a.group("kind").lower())], _columns(a.group("cols"))
        found = [n for n, k, c, *_ in _rows(cursor, "oid = ? and kind = ?", [oid, kind]) if not cols or list(c) == cols]
        if not found:
            what = {"PRIMARY KEY": "primary key", "UNIQUE": "UNIQUE KEYon", "FOREIGN KEY": "FOREIGN KEYon"}[kind]
            raise _fail(1450, f"constraint '{what}' does not exist")
        for name in found:
            _drop(cursor, oid, name)
        return _status(cursor, original, "Statement executed successfully.")
    if (a := _NULLS.match(rest)) and all(_CLAUSE.match(c) for c in _split(a.group("clauses"))):
        # each column on its own (DuckDB takes one ALTER COLUMN a statement)
        for clause in _split(a.group("clauses")):
            c = _CLAUSE.match(clause)
            column = c.group("col") if c.group("col").startswith('"') else c.group("col").upper()
            column = column if column.startswith('"') else f'"{column}"'
            try:
                original(cursor, f"alter table {_qualified(cursor, m.group('table'))} alter column {column} "
                                 f"{c.group('what').lower()} not null")
            except Exception as e:  # noqa: BLE001
                if "NULL" in str(e).upper() and c.group("what").lower() == "set":
                    raise _fail(1471, f"Column '{_ident(c.group('col'))}' contains null values. Not null constraint cannot be "
                                      "added.\n") from e
                raise
        return _status(cursor, original, "Statement executed successfully.")
    return None


def _split(clauses: str) -> list[str]:
    """Comma-separated clauses, outside parentheses and quotes."""
    out, depth, quote, start = [], 0, None, 0
    for i, ch in enumerate(clauses):
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(clauses[start:i])
            start = i + 1
    out.append(clauses[start:])
    return [c for c in out if c.strip()]


def _written(cursor, name: str) -> str:  # noqa: ANN001
    parts = _parts(name)
    db, schema = _where(cursor)
    return ".".join(([db, schema][: 3 - len(parts)] + parts) if len(parts) < 3 else parts)


def _qualified(cursor, name: str) -> str:  # noqa: ANN001
    _oid, db, schema, table = _table(cursor, name)
    return ".".join('"' + p.replace('"', '""') + '"' for p in (db, schema, table))


_KEY_COLUMNS = [["created_on", "TIMESTAMP_LTZ"], ["database_name", "TEXT"], ["schema_name", "TEXT"], ["table_name", "TEXT"],
                ["column_name", "TEXT"], ["key_sequence", "NUMBER"], ["constraint_name", "TEXT"], ["rely", "TEXT"],
                ["comment", "TEXT"]]
_FK_COLUMNS = [["created_on", "TIMESTAMP_LTZ"], ["pk_database_name", "TEXT"], ["pk_schema_name", "TEXT"], ["pk_table_name", "TEXT"],
               ["pk_column_name", "TEXT"], ["fk_database_name", "TEXT"], ["fk_schema_name", "TEXT"], ["fk_table_name", "TEXT"],
               ["fk_column_name", "TEXT"], ["key_sequence", "NUMBER"], ["update_rule", "TEXT"], ["delete_rule", "TEXT"],
               ["fk_name", "TEXT"], ["pk_name", "TEXT"], ["deferrability", "TEXT"], ["rely", "TEXT"], ["comment", "TEXT"]]


def _show(cursor, original, m):  # noqa: ANN001, ANN202
    kind, scope = m.group("kind").lower(), (m.group("scope") or "").lower()
    db, schema = _where(cursor)
    # (EXPORTED KEYS IN TABLE p: the foreign keys that point at p)
    oid_col, db_col, schema_col = ("ref_oid", "ref_database", "ref_schema") if kind == "exported" else ("oid", "database_name", "schema_name")
    where, args = [], []
    if scope == "table":
        try:
            oid, *_x = _table(cursor, m.group("name") or "", written=".".join(_parts(m.group("name") or "")) or None)
        except Exception:
            if not _is_view(cursor, m.group("name") or ""):
                raise
            oid = -1  # (a view has no keys: no rows)
        where.append(f"{oid_col} = ?")
        args.append(oid)
    elif scope == "schema":
        parts = _parts(m.group("name")) if m.group("name") else [schema]
        where.append(f"{db_col} = ? and {schema_col} = ?")
        args += [parts[0], parts[1]] if len(parts) == 2 else [db, parts[-1]]
    elif scope == "database":
        where.append(f"{db_col} = ?")
        args.append(_parts(m.group("name"))[-1] if m.group("name") else db)
    elif scope != "account":
        where.append(f"{db_col} = ? and {schema_col} = ?")
        args += [db, schema]
    wanted = {"primary": "kind = 'PRIMARY KEY'", "unique": "kind = 'UNIQUE'"}.get(kind, "kind = 'FOREIGN KEY'")
    rows = cursor._duck_conn.execute(
        f"select made, database_name, schema_name, table_name, columns, name, rely, ref_database, ref_schema, ref_table, "
        f"ref_columns, ref_name from {LIVE} where {wanted}" + "".join(" and " + w for w in where), args).fetchall()
    out = []
    for created, d, s, t, cols, name, rely, rd, rs, rt, rcols, rname in rows:
        for i, col in enumerate(cols):
            if kind in ("primary", "unique"):
                out.append([created, d, s, t, col, i + 1, name, "true" if rely else "false", None])
            else:
                out.append([created, rd, rs, rt, rcols[i], d, s, t, col, i + 1, "NO ACTION", "NO ACTION", name, rname,
                            "NOT DEFERRABLE", "true" if rely else "false", None])
    if kind in ("primary", "unique"):
        out.sort(key=lambda r: (r[1], r[2], r[3], r[4], r[6]))
        return _select(cursor, original, _KEY_COLUMNS, out)
    out.sort(key=lambda r: (r[1], r[2], r[3], r[4], r[12]))
    return _select(cursor, original, _FK_COLUMNS, out)


def inline(tree) -> tuple[object, list[dict]]:  # noqa: ANN001
    """CREATE TABLE's own constraints, taken out of the statement: the statement without them (a column's PRIMARY KEY
    left as NOT NULL), and each as add() takes it."""
    from sqlglot import exp

    schema = tree.this if isinstance(tree.this, exp.Schema) else None
    if schema is None:
        return tree, []
    found: list[dict] = []

    def ref_of(reference) -> tuple[str, list[str]]:  # noqa: ANN001
        target = reference.this
        table = target.this if isinstance(target, exp.Schema) else target
        cols = [i.name if i.quoted else i.name.upper() for i in (target.expressions if isinstance(target, exp.Schema) else [])]
        return table.sql(dialect="snowflake"), cols

    for coldef in list(schema.expressions):
        if isinstance(coldef, exp.ColumnDef):
            col = coldef.this.name if coldef.this.quoted else coldef.this.name.upper()
            kept = []
            for c in coldef.args.get("constraints") or []:
                name = c.this.name if isinstance(c.this, exp.Identifier) else None
                named = (c.this.name if c.this.quoted else c.this.name.upper()) if name else None
                kind = c.args.get("kind")
                if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                    found.append({"name": named, "kind": "PRIMARY KEY", "cols": [col], "rely": "RELY" in str(kind).upper().split()})
                    if not any(isinstance(k.args.get("kind"), exp.NotNullColumnConstraint) for k in coldef.args.get("constraints") or []):
                        kept.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
                elif isinstance(kind, exp.UniqueColumnConstraint):
                    found.append({"name": named, "kind": "UNIQUE", "cols": [col], "rely": "RELY" in str(kind).upper().split()})
                elif isinstance(kind, exp.Reference):
                    ref, refcols = ref_of(kind)
                    found.append({"name": named, "kind": "FOREIGN KEY", "cols": [col], "ref": ref, "refcols": refcols,
                                  "rely": "RELY" in str(kind).upper().split()})
                else:
                    kept.append(c)
            coldef.set("constraints", kept)
            continue
        named = None
        node = coldef
        if isinstance(coldef, exp.Constraint):
            named = coldef.this.name if coldef.this.quoted else coldef.this.name.upper()
            node = coldef.expressions[0] if coldef.expressions else None
        cols_of = lambda n: [i.name if i.quoted else i.name.upper() for i in n]  # noqa: E731
        text = str(node).upper().split() if node is not None else []
        if isinstance(node, exp.PrimaryKey):
            found.append({"name": named, "kind": "PRIMARY KEY", "cols": cols_of(node.expressions), "rely": "RELY" in text})
        elif isinstance(node, exp.UniqueColumnConstraint) and isinstance(node.this, exp.Schema):
            found.append({"name": named, "kind": "UNIQUE", "cols": cols_of(node.this.expressions), "rely": "RELY" in text})
        elif isinstance(node, exp.ForeignKey):
            ref, refcols = ref_of(node.args["reference"])
            found.append({"name": named, "kind": "FOREIGN KEY", "cols": cols_of(node.expressions), "ref": ref,
                          "refcols": refcols, "rely": "RELY" in text})
        else:
            continue
        schema.expressions.remove(coldef)
    return tree, found


def intercept(cursor, command: str, original):  # noqa: ANN001, ANN201
    """Handle a statement about constraints. None for everything else."""
    head = re.match(_LEAD + r"(\w+)", command)
    if not head or head.group(1).lower() not in ("alter", "show", "create"):
        return None
    command = without_trailing_comments(command)
    if head.group(1).lower() == "show":
        if (like := _SHOW_LIKE.match(command)) is not None:  # (SHOW ... KEYS takes no LIKE, measured)
            raise _error(f"SQL compilation error:\nsyntax error line 1 at position {like.start('like')} unexpected 'like'.",
                         errno=1003, sqlstate="42000")
        m = _SHOW.match(command)
        return _show(cursor, original, m) if m else None
    if head.group(1).lower() == "alter":
        m = _ALTER.match(command)
        return _alter(cursor, original, m) if m else None
    if not _CREATE.match(command):
        return None
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(command, read="snowflake")
    except Exception:  # noqa: BLE001 - left to the statement's own path
        return None
    if not isinstance(tree, exp.Create) or str(tree.args.get("kind", "")).upper() != "TABLE":
        return None
    tree, found = inline(tree)
    if not found:
        return None
    done = cursor.execute(tree.sql(dialect="snowflake"))  # (the table, as the statement makes it, without its keys)
    target = tree.this.this
    oid, *_x, table = _table(cursor, target.sql(dialect="snowflake"))
    for c in [c for c in found if c["kind"] != "FOREIGN KEY"] + [c for c in found if c["kind"] == "FOREIGN KEY"]:
        add(cursor, oid, table, c["name"], c["kind"], c["cols"], c.get("ref"), c.get("refcols") or [], c["rely"])
    return done
