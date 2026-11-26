"""What Snowflake keeps about a table or view besides its rows, and dynamic and interactive tables.

Each CREATE, ALTER, COMMENT, DROP and DML statement that succeeds is noted in `_fs_global.main._fx_relations`: when the
relation was made and last changed, by which role, its kind (transient, temporary, dynamic, interactive), its clustering
key, comment, change tracking, a view's text and a dynamic table's definition. SHOW TABLES / OBJECTS / VIEWS /
DYNAMIC TABLES / INTERACTIVE TABLES and INFORMATION_SCHEMA.TABLES read it, with Snowflake's columns in Snowflake's order
(measured on a live account, 2026-11-25).

A dynamic table is a table holding its query's rows: computed when it is created (INITIALIZE = ON_CREATE, the default)
and again at each `ALTER DYNAMIC TABLE ... REFRESH`. One made ON_SCHEDULE cannot be read until it is refreshed, and none
can be written to, as in Snowflake. Its sources get change tracking, as Snowflake turns it on for them.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from snowflake.connector.errors import ProgrammingError

TABLE = "_fs_global.main._fx_relations"
_LEAD = r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*"
_NAME = r'(?:"(?:[^"]|"")+"|[\w$]+)(?:\s*\.\s*(?:"(?:[^"]|"")+"|[\w$]+)){0,2}'
_TRAILING = re.compile(r"(?s)(?:\s*;)?\s*/\*(?:(?!\*/).)*\*/\s*;?\s*$")

COLUMNS = ("database_name varchar, schema_name varchar, name varchar, kind varchar, transient boolean, created_on timestamptz, "
           "last_altered timestamptz, last_ddl timestamptz, owner varchar, cluster_by varchar, comment varchar, "
           "change_tracking boolean, retention integer, secure boolean, text varchar, dynamic json, row_count bigint")

_CREATE = re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?(?P<mods>(?:(?:local|global)\s+)?(?:(?:transient|temporary|temp|volatile|"
                     r"secure|recursive|materialized|dynamic|interactive|iceberg|hybrid|event)\s+)*)(?P<kind>table|view)\s+"
                     r"(?P<exists>if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*)$")
_ALTER = re.compile(_LEAD + r"alter\s+(?P<kind>table|view|dynamic\s+table|interactive\s+table|materialized\s+view)\s+(?:if\s+exists\s+)?"
                    r"(?P<name>" + _NAME + r")\s+(?P<rest>.*)$")
_COMMENT_ON = re.compile(_LEAD + r"comment\s+(?:if\s+exists\s+)?on\s+(?P<kind>table|view|dynamic\s+table|column)\s+(?P<name>" + _NAME +
                         r")\s+is\s+(?P<text>'(?:[^']|'')*'|\$\$.*?\$\$)\s*;?\s*$")
_DROP = re.compile(_LEAD + r"drop\s+(?:table|view|dynamic\s+table|interactive\s+table|materialized\s+view|external\s+table)\s+(?:if\s+exists\s+)?"
                   r"(?P<name>" + _NAME + r")")
_DML = re.compile(_LEAD + r"(?:insert\s+(?:overwrite\s+)?(?:all\s+|first\s+)?into|update|delete\s+from|merge\s+into|truncate\s+(?:table\s+)?(?:if\s+exists\s+)?|"
                  r"copy\s+into)\s+(?P<name>" + _NAME + r")")
_SHOW = re.compile(_LEAD + r"show\s+(?P<terse>terse\s+)?(?P<kind>dynamic|interactive)\s+tables(?P<rest>.*)$")
_STRING = r"'(?:[^'\\]|''|\\.)*'|\$\$.*?\$\$"  # (a quote doubled, or escaped as sqlglot writes it)


def ensure(duck) -> None:  # noqa: ANN001
    ensure_grants(duck)
    WAITING.clear()
    SCHEDULED.clear()
    duck.execute(f"create table if not exists {TABLE} ({COLUMNS})")
    started = now().isoformat(sep=" ")
    duck.execute(f"create or replace macro _fs_global.main._fx_started() as '{started}'::timestamptz")


def now() -> datetime:
    from . import emulator

    if emulator.FROZEN_NOW is not None:
        return emulator.FROZEN_NOW.replace(tzinfo=timezone.utc) if emulator.FROZEN_NOW.tzinfo is None else emulator.FROZEN_NOW
    return datetime.now(timezone.utc)


def _unquote(part: str) -> str:
    part = part.strip()
    return part[1:-1].replace('""', '"') if part.startswith('"') else part.upper()


def _parts(name: str) -> list[str]:
    return [_unquote(p) for p in re.findall(r'"(?:[^"]|"")+"|[^.\s]+', name)]


def qualified(cursor, name: str) -> tuple[str, str, str]:  # noqa: ANN001
    parts = _parts(name)
    conn = cursor._conn
    while len(parts) < 3:
        parts.insert(0, ((conn.schema if len(parts) == 1 else conn.database) or "").upper())
    return parts[-3], parts[-2], parts[-1]


def _text(literal: str | None) -> str | None:
    if literal is None:
        return None
    if literal.startswith("$$"):
        return literal[2:-2]
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), literal[1:-1].replace("''", "'"))


def _close(text: str, at: int) -> int:
    """The index after the parenthesis that closes the one at `at` (skipping what is quoted, $$ ... $$ too)."""
    depth, i, quote = 0, at, None
    while i < len(text):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif text.startswith("$$", i):
            end = text.find("$$", i + 2)
            i = len(text) if end < 0 else end + 2
            continue
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


def _head(rest: str) -> tuple[str, str | None]:
    """A CREATE's options (after the column list, before AS) and the query after AS, if any."""
    text = rest.lstrip()
    if text.startswith("("):
        text = text[_close(text, 0):]
    i, quote, depth = 0, None, 0
    while i < len(text):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif text.startswith("$$", i):
            end = text.find("$$", i + 2)
            i = len(text) if end < 0 else end + 2
            continue
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and re.match(r"(?i)as\b", text[i:]) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            return text[:i], text[i + 2:].strip().rstrip(";").strip()
        i += 1
    return text, None


def _option(options: str, name: str) -> str | None:
    m = re.search(r"(?is)\b" + name + r"\s*=?\s*(" + _STRING + r"|[^\s,()]+)", options)
    return m.group(1) if m else None


def _clustering(options: str) -> str | None:
    m = re.search(r"(?is)\bcluster\s+by\s*(?:linear\s*)?\(", options)
    if not m:
        return None
    start = m.end() - 1
    return options[start + 1:_close(options, start) - 1].strip()


def _immutable_where(options: str) -> str | None:
    m = re.search(r"(?is)\bimmutable\s+where\s*\(", options)
    if not m:
        return None
    start = m.end() - 1
    return options[start + 1:_close(options, start) - 1].strip()


def _flag(value: str | None) -> bool | None:
    return None if value is None else value.strip("'").upper() in ("TRUE", "ON", "YES", "1")


def _upsert(duck, fq: tuple[str, str, str], **values) -> None:  # noqa: ANN001
    keys = list(values)
    found = duck.execute(f"select count(*) from {TABLE} where database_name = ? and schema_name = ? and name = ?", list(fq)).fetchone()[0]
    if found:
        duck.execute(f"update {TABLE} set " + ", ".join(f"{k} = ?" for k in keys) + " where database_name = ? and schema_name = ? and name = ?",
                     [values[k] for k in keys] + list(fq))
    else:
        duck.execute(f"insert into {TABLE} (database_name, schema_name, name, {', '.join(keys)}) values (?, ?, ?, {', '.join('?' for _ in keys)})",
                     [*fq, *(values[k] for k in keys)])


def _row(duck, fq) -> dict | None:  # noqa: ANN001
    cur = duck.execute(f"select * from {TABLE} where database_name = ? and schema_name = ? and name = ?", list(fq))
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], row)) if row else None


def _split_top(text: str) -> list[str]:
    parts, depth, quote, start, i = [], 0, None, 0, 0
    while i < len(text):
        c = text[i]
        if quote:
            if c == quote:
                quote = None
        elif text.startswith("$$", i):
            end = text.find("$$", i + 2)
            i = len(text) if end < 0 else end + 2
            continue
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
        i += 1
    parts.append(text[start:])
    return [p.strip() for p in parts if p.strip()]


_COLUMN_COMMENT = re.compile(r"(?is)^(?P<name>\"(?:[^\"]|\"\")+\"|[\w$]+)\s.*?\bcomment\s+(?P<text>" + _STRING + r")")


def _comment_columns(cursor, fq, pairs) -> None:  # noqa: ANN001
    """Column comments, kept as DuckDB's own (INFORMATION_SCHEMA.COLUMNS and DESCRIBE read them). An empty one is none."""
    for column, text in pairs:
        value = "NULL" if not text else "'" + text.replace("'", "''") + "'"
        target = _sql_name((*fq, _unquote(column)))
        try:
            cursor._duck_conn.execute(f"comment on column {target} is {value}")
        except Exception:  # noqa: BLE001, S110 - (a column that is not there: Snowflake has refused already)
            pass


def _column_list_comments(rest: str) -> list[tuple[str, str]]:
    text = rest.lstrip()
    if not text.startswith("("):
        return []
    found = []
    for part in _split_top(text[1:_close(text, 0) - 1]):
        m = _COLUMN_COMMENT.match(part)
        if m:
            found.append((m.group("name"), _text(m.group("text"))))
    return found


def _count(duck, fq) -> int | None:  # noqa: ANN001
    """The table's rows now (DuckDB's own estimate keeps counting deleted rows until it checkpoints)."""
    try:
        return duck.execute(f"select count(*) from {_sql_name(fq)}").fetchone()[0]
    except Exception:  # noqa: BLE001
        return None


def _role(duck) -> str:  # noqa: ANN001
    try:
        return duck.execute("select role from _fs_global.main._fx_session").fetchone()[0]
    except Exception:  # noqa: BLE001
        return "SYSADMIN"


def _in_transaction(cursor) -> bool:  # noqa: ANN001
    return bool(getattr(cursor._conn, "_fauxflake_txn", False))


def _command(command: str) -> str:
    return _TRAILING.sub("", command).strip()


# ---------------------------------------------------------------- noting what statements did

def note(cursor, command) -> None:  # noqa: ANN001
    """After a statement succeeded: what it did to a relation's metadata."""
    if not isinstance(command, str):
        return
    text = _command(command)
    head = re.match(_LEAD + r"(\w+)", text)
    if not head or head.group(1).lower() not in ("create", "alter", "comment", "drop", "insert", "update", "delete", "merge", "truncate", "copy"):
        return
    duck = cursor._duck_conn
    try:
        _note(cursor, duck, text, head.group(1).lower())
    except Exception:  # noqa: BLE001, S110 - metadata is best kept, never a reason for a statement to fail
        pass


def _note(cursor, duck, text: str, verb: str) -> None:  # noqa: ANN001, C901
    at = now()
    if _in_transaction(cursor) and verb not in ("insert", "update", "delete", "merge", "truncate", "copy"):
        return  # (never written inside a transaction: one that wrote to a database of the project cannot touch the registry's)
    if verb == "create":
        m = _CREATE.match(text)
        if not m:
            return
        fq = qualified(cursor, m.group("name"))
        mods = m.group("mods").lower().split()
        existing = _row(duck, fq)
        if m.group("exists") and existing and existing.get("created_on") is not None:
            return  # IF NOT EXISTS of one that was there: unchanged
        options, query = _head(m.group("rest"))
        kind = m.group("kind").upper()
        if kind == "VIEW":
            kind = "MATERIALIZED VIEW" if "materialized" in mods else "VIEW"
        elif "dynamic" in mods:
            kind = "DYNAMIC TABLE"
        elif "interactive" in mods:
            kind = "INTERACTIVE TABLE"
        elif any(t in mods for t in ("temporary", "temp", "volatile")):
            kind = "TEMPORARY TABLE"
        clone = re.search(r"(?is)^\s*clone\s+(" + _NAME + r")", m.group("rest"))
        source = _row(duck, qualified(cursor, clone.group(1))) if clone else None
        comment = _text(_option(options, "comment"))
        values = dict(kind=kind, transient="transient" in mods, created_on=at, last_altered=at, last_ddl=at, owner=_role(duck),
                      cluster_by=_clustering(options), comment=comment, change_tracking=bool(_flag(_option(options, "change_tracking"))),
                      retention=int(_option(options, "data_retention_time_in_days") or (0 if kind == "TEMPORARY TABLE" else 1)),
                      secure="secure" in mods, text=text if kind in ("VIEW", "MATERIALIZED VIEW") else None, dynamic=None)
        if source is not None:  # CLONE: what the source had, made now (and transient if the statement says so)
            for k in ("cluster_by", "comment", "change_tracking", "retention"):
                values[k] = source.get(k)
            values["transient"] = "transient" in mods or bool(source.get("transient"))
        if kind not in ("VIEW", "MATERIALIZED VIEW"):
            values["row_count"] = _count(duck, fq)
        if not re.search(r"(?is)\bcopy\s+grants\b", options):
            _grants_follow(duck, fq, dropped=True)  # (made again: the old one's grants went with it)
        SCHEDULED.pop(tuple(fq), None)
        WAITING.discard(tuple(fq))
        _upsert(duck, fq, **values)
        _comment_columns(cursor, fq, _column_list_comments(m.group("rest")))
        return
    if verb == "comment":
        m = _COMMENT_ON.match(text)
        if m and m.group("kind").lower() != "column":
            _upsert(duck, qualified(cursor, m.group("name")), comment=_text(m.group("text")), last_altered=at, last_ddl=at)
        elif m:
            parts = _parts(m.group("name"))
            table = qualified(cursor, ".".join('"' + p.replace('"', '""') + '"' for p in parts[:-1]))
            _comment_columns(cursor, table, [('"' + parts[-1].replace('"', '""') + '"', _text(m.group("text")))])
        return
    if verb == "drop":
        m = _DROP.match(text)
        if m:
            fq = qualified(cursor, m.group("name"))
            duck.execute(f"delete from {TABLE} where database_name = ? and schema_name = ? and name = ?", list(fq))
            _grants_follow(duck, fq, dropped=True)
            SCHEDULED.pop(tuple(fq), None)
            WAITING.discard(tuple(fq))
        return
    if verb == "alter":
        m = _ALTER.match(text)
        if not m:
            return
        fq, rest = qualified(cursor, m.group("name")), m.group("rest").strip().rstrip(";")
        renamed = re.match(r"(?is)rename\s+to\s+(" + _NAME + r")\s*$", rest)
        if renamed:
            to = qualified(cursor, renamed.group(1))
            duck.execute(f"update {TABLE} set database_name = ?, schema_name = ?, name = ?, last_ddl = ? "
                         "where database_name = ? and schema_name = ? and name = ?", [*to, at, *fq])
            _grants_follow(duck, fq, to=to)
            return
        swapped = re.match(r"(?is)swap\s+with\s+(" + _NAME + r")\s*$", rest)
        if swapped:
            other = qualified(cursor, swapped.group(1))
            duck.execute(f"update {TABLE} set name = '\x00' || name where database_name = ? and schema_name = ? and name = ?", list(fq))
            duck.execute(f"update {TABLE} set database_name = ?, schema_name = ?, name = ? where database_name = ? and schema_name = ? "
                         "and name = ?", [*fq, *other])
            duck.execute(f"update {TABLE} set database_name = ?, schema_name = ?, name = ? where database_name = ? and schema_name = ? "
                         "and name = ?", [*other, fq[0], fq[1], "\x00" + fq[2]])
            return
        altered = re.match(r"(?is)(?:alter|modify)\s+(?:column\s+)?(.*)$", rest)
        if altered and re.search(r"(?is)\bcomment\b", altered.group(1)):
            pairs = []
            for part in _split_top(altered.group(1)):
                c = re.match(r"(?is)^(?:column\s+)?(\"(?:[^\"]|\"\")+\"|[\w$]+)\s+(?:set\s+)?comment\s+(" + _STRING + r")\s*$", part)
                if c:
                    pairs.append((c.group(1), _text(c.group(2))))
                elif re.match(r"(?is)^(?:column\s+)?(\"(?:[^\"]|\"\")+\"|[\w$]+)\s+unset\s+comment\s*$", part):
                    pairs.append((part.split()[-3] if part.lower().startswith("column") else part.split()[0], ""))
            _comment_columns(cursor, fq, pairs)
        changes: dict = {"last_ddl": at}
        if re.match(r"(?is)(?:set\s+)?comment\s*=", rest) or re.match(r"(?is)set\b.*\bcomment\s*=", rest):
            changes["comment"] = _text(_option(rest, "comment"))
        if re.match(r"(?is)unset\b.*\bcomment\b", rest):
            changes["comment"] = None
        if re.search(r"(?is)\bchange_tracking\s*=", rest):
            changes["change_tracking"] = bool(_flag(_option(rest, "change_tracking")))
        if re.search(r"(?is)\bdata_retention_time_in_days\s*=", rest):
            changes["retention"] = int(_option(rest, "data_retention_time_in_days") or 1)
        if re.match(r"(?is)cluster\s+by\b", rest):
            changes["cluster_by"] = _clustering(rest)
        if re.match(r"(?is)drop\s+clustering\s+key\b", rest):
            changes["cluster_by"] = None
        if re.match(r"(?is)(add|drop|alter|modify|rename\s+column)\b", rest):
            changes["last_altered"] = at
        if re.match(r"(?is)set\s+secure\b", rest):
            changes["secure"] = True
        if re.match(r"(?is)unset\s+secure\b", rest):
            changes["secure"] = False
        if len(changes) > 1 or "last_altered" in changes:
            _upsert(duck, fq, **changes)
        return
    m = _DML.match(text)  # insert / update / delete / merge / truncate / copy: the rows changed now
    if m and not _in_transaction(cursor):
        fq = qualified(cursor, m.group("name"))
        _upsert(duck, fq, last_altered=at, row_count=_count(duck, fq))
    elif m:
        cursor._conn.__dict__.setdefault("_fx_altered", set()).add(qualified(cursor, m.group("name")))


def committed(cursor) -> None:  # noqa: ANN001
    """The tables a transaction wrote to were changed when it committed."""
    altered = cursor._conn.__dict__.pop("_fx_altered", None)
    if not altered:
        return
    at = now()
    for fq in altered:
        try:
            _upsert(cursor._duck_conn, fq, last_altered=at, row_count=_count(cursor._duck_conn, fq))
        except Exception:  # noqa: BLE001, S110
            pass


def rolled_back(cursor) -> None:  # noqa: ANN001
    cursor._conn.__dict__.pop("_fx_altered", None)


# ---------------------------------------------------------------- dynamic and interactive tables

_CREATE_SPECIAL = re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?(?P<transient>transient\s+)?(?P<kind>dynamic|interactive)\s+"
                             r"(?P<iceberg>iceberg\s+)?table\s+(?P<exists>if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*)$")
_ALTER_SPECIAL = re.compile(_LEAD + r"alter\s+(?P<kind>dynamic|interactive)\s+table\s+(?P<exists>if\s+exists\s+)?(?P<name>" + _NAME +
                            r")\s+(?P<rest>.*)$")
_WRITE = re.compile(_LEAD + r"(?P<verb>insert\s+(?:overwrite\s+)?into|update|delete\s+from|merge\s+into|truncate\s+(?:table\s+)?)\s*(?P<name>" + _NAME + r")")


def _error(message: str, errno: int, sqlstate: str) -> ProgrammingError:
    return ProgrammingError(msg=message, errno=errno, sqlstate=sqlstate)


def _status(original, cursor, text: str):  # noqa: ANN001, ANN202
    return original(cursor, "select '" + text.replace("'", "''") + "' as \"status\"")


def _sql_name(fq) -> str:  # noqa: ANN001
    return ".".join('"' + p.replace('"', '""') + '"' for p in fq)


def intercept(cursor, command, original):  # noqa: ANN001, ANN201
    """CREATE / ALTER / SHOW of dynamic and interactive tables, and writes to a dynamic table (refused). None otherwise."""
    if not isinstance(command, str):
        return None
    text = _command(command)
    head = re.match(_LEAD + r"(\w+)", text)
    word = head.group(1).lower() if head else ""
    if word in ("grant", "revoke") or (word == "show" and re.match(_LEAD + r"show\s+grants\s+on\b", text)):
        done = grants_intercept(cursor, command, original)
        if done is not None:
            return done
    if word == "create" and (m := _CREATE_SPECIAL.match(text)):
        return _create(cursor, original, m, text)
    if word == "create" and (m := _CLONE.match(text)):
        return _clone(cursor, original, m, text)
    if word == "create" and _CTAS_COLUMNS.match(text):
        done = _ctas_with_columns(cursor, original, text)
        if done is not None:
            return done
    if word == "alter" and (m := _ALTER_SPECIAL.match(text)):
        return _alter(cursor, original, m)
    if word == "alter" and (m := _ALTER_TYPE.match(text)):
        _check_type_change(cursor, m)
    if word == "show" and (m := _SHOW.match(text)):
        return _show(cursor, original, m)
    if word == "select" and (m := _GET_DDL.match(text)):
        return _get_ddl(cursor, original, m)
    if word in ("insert", "update", "delete", "merge", "truncate") and not getattr(cursor._conn, "_fx_refreshing", False) \
            and (m := _WRITE.match(text)):
        row = _row(cursor._duck_conn, qualified(cursor, m.group("name")))
        if row and row.get("kind") == "DYNAMIC TABLE":
            verb = m.group("verb").split()[0].upper()
            raise _error(f"SQL Compilation error: Invalid Operation {verb} on Dynamic Table", 2706, "23001")
    if word in ("select", "with", "insert", "merge", "create", "update", "delete", "copy") and SCHEDULED:
        keep_fresh(cursor, text)
    if word in ("select", "with") and "dynamic" not in text.lower():
        uninitialized(cursor, text)
    return None


_ALTER_TYPE = re.compile(_LEAD + r"alter\s+table\s+(?:if\s+exists\s+)?(?P<name>" + _NAME + r")\s+(?:alter|modify)\s+(?:column\s+)?"
                         r"(?P<column>\"(?:[^\"]|\"\")+\"|[\w$]+)\s+(?:set\s+data\s+)?type\s+(?P<type>[\w ]+?(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?)"
                         r"(?:\s+collate\s+'(?P<collate>[^']*)')?\s*;?\s*$")


def _snowflake_type(text: str) -> str:
    """A type as DESCRIBE writes it: VARCHAR(16777216), NUMBER(38,0), TIMESTAMP_NTZ(9)."""
    import sqlglot
    from sqlglot import exp

    dtype = sqlglot.parse_one(f"cast(null as {text})", read="snowflake").to
    T = exp.DataType.Type
    params = [int(p.this.name) for p in dtype.expressions if isinstance(p, exp.DataTypeParam) and p.this.is_int]
    if dtype.this in (T.VARCHAR, T.CHAR, T.TEXT, T.NVARCHAR, T.NCHAR):
        return f"VARCHAR({params[0] if params and dtype.this != T.CHAR else (params[0] if params else 1) if dtype.this == T.CHAR else 16777216})"
    if dtype.this in (T.DECIMAL, T.BIGDECIMAL):
        return f"NUMBER({params[0] if params else 38},{params[1] if len(params) > 1 else 0})"
    if dtype.this in (T.INT, T.BIGINT, T.SMALLINT, T.TINYINT):
        return "NUMBER(38,0)"
    if dtype.this in (T.FLOAT, T.DOUBLE):
        return "FLOAT"
    named = {T.TIMESTAMP: "TIMESTAMP_NTZ(9)", T.TIMESTAMPNTZ: "TIMESTAMP_NTZ(9)", T.TIMESTAMPLTZ: "TIMESTAMP_LTZ(9)",
             T.TIMESTAMPTZ: "TIMESTAMP_TZ(9)", T.TIME: "TIME(9)"}
    return named.get(dtype.this, dtype.sql("snowflake").upper())


def _check_type_change(cursor, m) -> None:  # noqa: ANN001, C901
    """ALTER TABLE ... ALTER COLUMN c SET DATA TYPE t [COLLATE 'spec'], refused as Snowflake refuses it (measured
    2026-11-26): another kind of type (002108), a shorter VARCHAR (040050), another scale (040052), another collation
    (040053, the spec's case aside)."""
    fq = qualified(cursor, m.group("name"))
    column = m.group("column")
    column = column[1:-1].replace('""', '"') if column.startswith('"') else column.upper()
    cur = cursor._conn.cursor()
    try:
        described = {row[0]: row[1] for row in cur.execute(f"describe table {_sql_name(fq)}").fetchall()}
    except Exception:  # noqa: BLE001 - a table that is not there is the statement's own error to give
        return
    finally:
        cur.close()
    if column not in described:
        return
    try:
        new = _snowflake_type(m.group("type"))
    except Exception:  # noqa: BLE001
        return
    found = re.fullmatch(r"(.*?)(?: COLLATE '([^']*)')?", described[column])
    old, old_spec = found[1], found[2]
    new_spec = m.group("collate")
    kind = lambda t: re.sub(r"\(.*", "", t)  # noqa: E731
    shown = column.upper() if re.fullmatch(r"[A-Z_][A-Z0-9_$]*", column) else f'"{column}"'
    if kind(old) != kind(new):
        raise _error(f"SQL compilation error: cannot change column {shown} from type {old} to {new}", 2108, "22000")
    if (old_spec or "").lower() != (new_spec or "").lower():
        written = lambda t, spec: f"\"{t}{f' COLLATE {chr(39)}{spec}{chr(39)}' if spec else ''}\""  # noqa: E731
        raise _error(f"SQL compilation error: cannot change column {shown} from type {written(old, old_spec)} to "
                     f"{written(new, new_spec)} because they have incompatible collations.", 40053, "22000")
    sizes = lambda t: [int(x) for x in re.findall(r"\d+", t)]  # noqa: E731
    if kind(old) == "VARCHAR" and sizes(new)[0] < sizes(old)[0]:
        raise _error(f"SQL compilation error: cannot change column {shown} from type {old} to {new} because reducing the "
                     "byte-length of a varchar is not supported.", 40050, "22000")
    if kind(old) == "NUMBER" and sizes(new)[1] != sizes(old)[1]:
        raise _error(f"SQL compilation error: cannot change column {shown} from type {old} to {new} because changing the "
                     "scale of a number is not supported.", 40052, "22000")


WAITING: set[tuple[str, str, str]] = set()  # dynamic tables made ON_SCHEDULE and not yet refreshed
SCHEDULED: dict[tuple[str, str, str], dict] = {}  # dynamic tables: target lag (seconds, or DOWNSTREAM), last refresh, state


def _lag_seconds(lag: str | None) -> float | None | str:
    if lag is None:
        return None
    if lag.upper() == "DOWNSTREAM":
        return "DOWNSTREAM"
    m = re.match(r"(?i)\s*(\d+)\s*(second|minute|hour|day)s?\s*$", lag)
    return int(m.group(1)) * {"second": 1, "minute": 60, "hour": 3600, "day": 86400}[m.group(2).lower()] if m else None


def _schedule(fq, dynamic: dict | None) -> None:  # noqa: ANN001
    if dynamic is None:
        SCHEDULED.pop(tuple(fq), None)
        return
    at = dynamic.get("data_timestamp")
    SCHEDULED[tuple(fq)] = {"lag": _lag_seconds(dynamic.get("target_lag")), "state": dynamic.get("scheduling_state", "ACTIVE"),
                            "scheduler": dynamic.get("scheduler"), "at": datetime.fromisoformat(at) if at else None}


def _named(fq, text: str) -> bool:  # noqa: ANN001
    return re.search(r'(?i)(?<![\w$])"?' + re.escape(fq[2]) + r'"?(?![\w$])', text) is not None


def keep_fresh(cursor, text: str) -> None:  # noqa: ANN001
    """Snowflake refreshes a dynamic table on its own, within its target lag. Before a statement reads one whose lag has
    passed on the emulator's clock (simulated time too), it is refreshed: as fresh as Snowflake would have kept it."""
    if not SCHEDULED or _in_transaction(cursor) or getattr(cursor._conn, "_fx_refreshing", False):
        return
    at = now()
    for fq, plan in list(SCHEDULED.items()):
        if plan["state"] != "ACTIVE" or plan["at"] is None or not isinstance(plan["lag"], int) or not _named(fq, text):
            continue
        if (at - plan["at"]).total_seconds() >= plan["lag"]:
            try:
                refresh(cursor, fq)
            except Exception:  # noqa: BLE001, S110 - a refresh that fails leaves the rows Snowflake would have kept too
                pass


def uninitialized(cursor, text: str) -> None:  # noqa: ANN001
    """Reading a dynamic table made ON_SCHEDULE that was never refreshed is an error (measured: 002741)."""
    if not WAITING:
        return
    try:
        waiting = cursor._duck_conn.execute(f"select database_name, schema_name, name from {TABLE} where kind = 'DYNAMIC TABLE' "
                                            "and json_extract_string(dynamic, '$.initialized') = 'false'").fetchall()
    except Exception:  # noqa: BLE001
        return
    for fq in waiting:
        if re.search(r'(?i)\b' + re.escape(fq[2]) + r'\b', text):
            from sqlglot import exp, parse_one

            try:
                tables = parse_one(text, read="snowflake").find_all(exp.Table)
            except Exception:  # noqa: BLE001
                return
            for t in tables:
                if qualified(cursor, ".".join(p for p in (t.catalog, t.db, t.name) if p)) == tuple(fq) or (
                        t.name.upper() == fq[2] and not t.db):
                    name = ".".join(fq)
                    raise _error(f"Dynamic Table '{name}' is not initialized. Please run a manual refresh or wait for a scheduled refresh "
                                 "before querying.", 2741, "55000")


def _dynamic_options(options: str) -> dict:
    lag = _option(options, "target_lag")
    lag = _text(lag) if lag and lag.startswith("'") else lag
    refresh = (_option(options, "refresh_mode") or "AUTO").strip("'").upper()
    initialize = (_option(options, "initialize") or "ON_CREATE").strip("'").upper()
    scheduler = _option(options, "scheduler")
    return {
        "target_lag": None if lag is None else ("DOWNSTREAM" if lag.upper() == "DOWNSTREAM" else lag),
        "configured_refresh_mode": refresh,
        "refresh_mode": "ADAPTIVE" if refresh == "AUTO" else refresh,  # (measured: AUTO is shown as ADAPTIVE)
        "initialize": initialize,
        "warehouse": (_option(options, "warehouse") or "").strip('"').upper() or None,
        "initialization_warehouse": (_option(options, "initialization_warehouse") or "").strip('"').upper() or None,
        "scheduler": None if scheduler is None else (_text(scheduler) if scheduler.startswith("'") else scheduler).upper(),
        "immutable_where": _immutable_where(options),
    }


def _dynamic_text(name: str, transient: bool, options: dict, cluster: str | None, query: str) -> str:  # noqa: FBT001
    """The definition as SHOW DYNAMIC TABLES gives it (measured): Snowflake writes the options again in its own order."""
    parts = [f"create or replace {'transient ' if transient else ''}dynamic table {name}"]
    if (options.get("scheduler") or "").upper() == "DISABLE":
        parts.append("SCHEDULER = DISABLE")  # (measured: right after the name, in capitals)
    if cluster:
        parts.append(f"cluster by ({cluster})")
    if options.get("target_lag"):
        parts.append(f"lag = '{options['target_lag']}'")
    parts.append(f"refresh_mode = '{options['configured_refresh_mode']}'")
    parts.append(f"initialize = '{options['initialize']}'")
    if options.get("warehouse"):
        parts.append(f"warehouse = {options['warehouse']}")
    if options.get("initialization_warehouse"):
        parts.append(f"initialization_warehouse = {options['initialization_warehouse']}")
    if options.get("immutable_where"):
        parts.append(f"immutable where ({options['immutable_where']})")
    return " ".join(parts) + " as " + query


def _create(cursor, original, m: re.Match, text: str):  # noqa: ANN001, ANN202
    fq = qualified(cursor, m.group("name"))
    duck = cursor._duck_conn
    exists = duck.execute("select count(*) from information_schema.tables where table_catalog = ? and table_schema = ? and table_name = ?",
                          list(fq)).fetchone()[0]
    kind = m.group("kind").upper()
    label = "Dynamic table" if kind == "DYNAMIC" else "Table"
    if exists and m.group("exists"):
        return _status(original, cursor, f"{fq[2]} already exists, statement succeeded.")
    if exists and not m.group("replace"):
        raise _error(f"SQL compilation error:\nObject '{fq[2]}' already exists.", 2002, "42710")
    options, query = _head(m.group("rest"))
    if query is None:
        raise _error("SQL compilation error:\nsyntax error: a dynamic table needs AS <query>", 1003, "42000")
    cluster = _clustering(options)
    transient = bool(m.group("transient"))
    dynamic = _dynamic_options(options) if kind == "DYNAMIC" or _option(options, "target_lag") else None
    initialize_now = dynamic is None or dynamic["initialize"] == "ON_CREATE"
    body = query[1:_close(query, 0) - 1] if query.startswith("(") and _close(query, 0) == len(query) else query
    target = _sql_name(fq)
    cursor.execute(f"create or replace {'transient ' if transient else ''}table {target} as select * from ({body}) "
                   + ("" if initialize_now else "limit 0"))
    at = now()
    values = dict(kind="DYNAMIC TABLE" if kind == "DYNAMIC" else "INTERACTIVE TABLE", transient=transient, created_on=at, last_altered=at,
                  last_ddl=at, owner=_role(duck), cluster_by=cluster, comment=_text(_option(options, "comment")), change_tracking=kind == "DYNAMIC",
                  retention=1, secure=False, text=None, dynamic=None)
    if dynamic is not None:
        dynamic.update(query=body, written=query, initialized=initialize_now, scheduling_state="ACTIVE", last_suspended_on=None,
                       data_timestamp=at.isoformat() if initialize_now else None)
        values["text"] = _dynamic_text(m.group("name"), transient, dynamic, cluster, query)
        values["dynamic"] = json.dumps(dynamic)
    values["row_count"] = _count(duck, fq)
    _upsert(duck, fq, **values)
    (WAITING.discard if initialize_now else WAITING.add)(fq)
    _schedule(fq, dynamic)
    _track_sources(cursor, body)
    return _status(original, cursor, f"{label} {fq[2]} successfully created.")


def _track_sources(cursor, query: str) -> None:  # noqa: ANN001
    """Snowflake turns change tracking on for the tables a dynamic table reads (measured: SHOW TABLES says ON)."""
    from sqlglot import exp, parse_one

    try:
        tables = list(parse_one(query, read="snowflake").find_all(exp.Table))
    except Exception:  # noqa: BLE001
        return
    for t in tables:
        if not t.name:
            continue
        fq = qualified(cursor, ".".join(p for p in (t.catalog, t.db, t.name) if p))
        try:
            _upsert(cursor._duck_conn, fq, change_tracking=True)
        except Exception:  # noqa: BLE001, S110
            pass


def refresh(cursor, fq) -> tuple[int, int]:  # noqa: ANN001
    """Compute a dynamic table again. Returns (rows inserted, rows deleted)."""
    duck = cursor._duck_conn
    row = _row(duck, fq)
    dynamic = json.loads(row["dynamic"]) if row and row.get("dynamic") else None
    if dynamic is None:
        raise _error(f"SQL compilation error:\nDynamic table '{'.'.join(fq)}' does not exist or not authorized.", 2003, "42S02")
    target = _sql_name(fq)
    at = now()
    for upstream, plan in list(SCHEDULED.items()):  # what it reads, refreshed first when it waits for it or is due
        if upstream != tuple(fq) and _named(upstream, dynamic["query"]) and plan["state"] == "ACTIVE" and (
                plan["lag"] == "DOWNSTREAM" or plan["at"] is None
                or (isinstance(plan["lag"], int) and (at - plan["at"]).total_seconds() >= plan["lag"])):
            refresh(cursor, upstream)
    (before,) = duck.execute(f"select count(*) from {target}").fetchone()
    cur = cursor._conn.cursor()
    cursor._conn._fx_refreshing = True  # (the refresh itself writes to it)
    try:
        cur.execute(f"delete from {target}")
        cur.execute(f"insert into {target} select * from ({dynamic['query']})")
    finally:
        cursor._conn._fx_refreshing = False
        cur.close()
    (after,) = duck.execute(f"select count(*) from {target}").fetchone()
    at = now()
    dynamic.update(initialized=True, data_timestamp=at.isoformat())
    WAITING.discard(tuple(fq))
    _schedule(fq, dynamic)
    _upsert(duck, fq, dynamic=json.dumps(dynamic), last_altered=at, row_count=after)
    return after, before


def _alter(cursor, original, m: re.Match):  # noqa: ANN001, ANN202
    fq = qualified(cursor, m.group("name"))
    duck = cursor._duck_conn
    row = _row(duck, fq)
    wanted = "DYNAMIC TABLE" if m.group("kind").lower() == "dynamic" else "INTERACTIVE TABLE"
    if row is None or row.get("kind") != wanted:
        if m.group("exists"):
            return _status(original, cursor, "Statement executed successfully.")
        raise _error(f"SQL compilation error:\n{wanted.title()} '{'.'.join(fq)}' does not exist or not authorized.", 2003, "42S02")
    rest = m.group("rest").strip().rstrip(";")
    dynamic = json.loads(row["dynamic"]) if row.get("dynamic") else {}
    at = now()
    if re.match(r"(?is)refresh\b", rest):
        before = dynamic.get("data_timestamp")
        inserted, deleted = refresh(cursor, fq)
        stats = json.dumps({"insertedRows": inserted, "copiedRows": 0, "deletedRows": deleted}, separators=(",", ":")) \
            if inserted or deleted or before is None else "No new data"
        stamp = at.strftime("%Y-%m-%d %H:%M:%S.%f")
        return original(cursor, f"select '{'.'.join(fq)}' as dt_name, '{stats}' as statistics, {1 if stats != 'No new data' else 0} as "
                                f"refreshed_dt_count, '{stamp} +00:00'::timestamp_ltz as data_timestamp")
    if re.match(r"(?is)suspend\b", rest):
        dynamic.update(scheduling_state="SUSPENDED", last_suspended_on=at.isoformat())
    elif re.match(r"(?is)resume\b", rest):
        dynamic.update(scheduling_state="ACTIVE")
    elif re.match(r"(?is)rename\s+to\s+", rest):
        to = qualified(cursor, re.match(r"(?is)rename\s+to\s+(" + _NAME + r")", rest).group(1))
        duck.execute(f"alter table {_sql_name(fq)} rename to {_sql_name(to)[_sql_name(to).rfind('.') + 1:]}")
        duck.execute(f"update {TABLE} set database_name = ?, schema_name = ?, name = ? where database_name = ? and schema_name = ? and name = ?",
                     [*to, *fq])
        if tuple(fq) in SCHEDULED:
            SCHEDULED[tuple(to)] = SCHEDULED.pop(tuple(fq))
        return _status(original, cursor, "Statement executed successfully.")
    elif re.match(r"(?is)swap\s+with\s+", rest):
        raise _error("SQL compilation error:\nSWAP WITH is not supported for dynamic tables.", 2032, "42601")
    elif re.match(r"(?is)set\s+immutable\s+where\b", rest):
        dynamic["immutable_where"] = _immutable_where(rest)
    elif re.match(r"(?is)unset\s+immutable\s+where\b", rest):
        dynamic["immutable_where"] = None
    elif re.match(r"(?is)(set|unset)\b", rest):
        setting = re.match(r"(?is)set\b", rest) is not None
        enabling = re.search(r"(?is)\bscheduler\s*=\s*'?enable", rest) is not None
        if setting and re.search(r"(?is)\btarget_lag\s*=", rest) and (dynamic.get("scheduler") or "").upper() == "DISABLE" and not enabling:
            lag = _option(rest, "target_lag") or ""
            raise _error(f"SQL compilation error:\ninvalid property combination 'SCHEDULER'='DISABLE' and 'TARGET_LAG'="
                         f"'{_text(lag) if lag.startswith(chr(39)) else lag}'", 1425, "22023")
        for key in ("target_lag", "warehouse", "initialization_warehouse", "refresh_mode", "scheduler", "comment", "data_retention_time_in_days"):
            if not re.search(r"(?is)\b" + key + r"\b", rest):
                continue
            if key == "comment":
                _upsert(duck, fq, comment=_text(_option(rest, "comment")) if setting else None)
                continue
            if key == "data_retention_time_in_days":
                continue
            value = _option(rest, key) if setting else None
            if value is not None:
                value = _text(value) if value.startswith("'") else value.strip('"')
                value = value if key == "target_lag" and value.upper() != "DOWNSTREAM" else value.upper()
            if key == "scheduler" and value == "DISABLE":
                dynamic["target_lag"] = None  # (no lag without the scheduler: SHOW gives '')
            if key == "refresh_mode":
                dynamic["configured_refresh_mode"] = value or "AUTO"
                dynamic["refresh_mode"] = "ADAPTIVE" if (value or "AUTO") == "AUTO" else value
            else:
                dynamic[key] = value
    elif re.match(r"(?is)cluster\s+by\b", rest):
        _upsert(duck, fq, cluster_by=_clustering(rest))
    elif re.match(r"(?is)drop\s+clustering\s+key\b", rest):
        _upsert(duck, fq, cluster_by=None)
    if dynamic:
        cluster = (_row(duck, fq) or {}).get("cluster_by")
        name = row["text"].split(" dynamic table ", 1)[1].split(" ", 1)[0] if row.get("text") else fq[2]
        text = _dynamic_text(name, bool(row.get("transient")), dynamic, cluster, dynamic.get("written") or dynamic.get("query", ""))
        _upsert(duck, fq, dynamic=json.dumps(dynamic), text=text, last_ddl=at)
        _schedule(fq, dynamic)
    return _status(original, cursor, "Statement executed successfully.")


def _scope(cursor, rest: str) -> tuple[list[str], list]:  # noqa: ANN001
    """LIKE / IN ACCOUNT|DATABASE|SCHEMA / STARTS WITH / LIMIT .. FROM of a SHOW, as a WHERE and its values."""
    where, values = ["1 = 1"], []
    like = re.search(r"(?is)\blike\s+('(?:[^']|'')*')", rest)
    if like:
        where.append("r.name ilike ?")
        values.append(_text(like.group(1)))
    scope = re.search(r"(?is)\bin\s+(account|database|schema)?\s*(" + _NAME + r")?", rest)
    conn = cursor._conn
    if scope and (scope.group(1) or "").lower() == "account":
        pass
    elif scope and (scope.group(1) or "").lower() == "database":
        where.append("r.database_name = ?")
        values.append(_parts(scope.group(2))[-1] if scope.group(2) else (conn.database or "").upper())
    elif scope and scope.group(2):
        parts = _parts(scope.group(2))
        where.append("r.database_name = ? and r.schema_name = ?")
        values += [parts[-2] if len(parts) > 1 else (conn.database or "").upper(), parts[-1]]
    else:
        where.append("r.database_name = ? and r.schema_name = ?")
        values += [(conn.database or "").upper(), (conn.schema or "").upper()]
    starts = re.search(r"(?is)\bstarts\s+with\s+('(?:[^']|'')*')", rest)
    if starts:
        where.append("r.name >= ? and starts_with(r.name, ?)")
        values += [_text(starts.group(1))] * 2
    after = re.search(r"(?is)\blimit\s+\d+\s+from\s+('(?:[^']|'')*')", rest)
    if after:
        where.append("r.name > ?")
        values.append(_text(after.group(1)))
    return where, values


def _show(cursor, original, m: re.Match):  # noqa: ANN001, ANN202
    rest = m.group("rest")
    where, values = _scope(cursor, rest)
    limit = re.search(r"(?is)\blimit\s+(\d+)", rest)
    kind = "DYNAMIC TABLE" if m.group("kind").lower() == "dynamic" else "INTERACTIVE TABLE"
    select = DYNAMIC_COLUMNS if kind == "DYNAMIC TABLE" else INTERACTIVE_COLUMNS
    if m.group("terse"):
        select = "r.created_on, r.name, 'TABLE' as kind, r.database_name, r.schema_name"
    sql = (f"select {select} from ({RELATIONS}) r where r.kind = '{kind}' and " + " and ".join(where) + " order by r.name"
           + (f" limit {limit.group(1)}" if limit else ""))
    rows = cursor._duck_conn.execute(sql, values).fetchall()
    names = [d[0] for d in cursor._duck_conn.description]
    from . import scripting

    if not rows:
        return original(cursor, "select " + ", ".join(f'null::{_KINDS.get(n, "varchar")} as "{n}"' for n in names) + " where false")
    selects = ["select " + ", ".join(f'{_literal(v, n, scripting)} as "{n}"' for n, v in zip(names, row)) for row in rows]
    return original(cursor, " union all ".join(selects))


_KINDS = {"created_on": "timestamp_ltz", "rows": "bigint", "bytes": "bigint", "last_suspended_on": "timestamp_ltz", "data_timestamp": "timestamp_ltz",
          "latest_data_timestamp": "timestamp_ltz"}  # (Snowflake types: these statements run as Snowflake SQL)


def _literal(value, name: str, scripting) -> str:  # noqa: ANN001
    if value is None:
        return f"null::{_KINDS.get(name, 'varchar')}"
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'::timestamp_ltz"
    return scripting.literal(value)


# ---------------------------------------------------------------- what SHOW and INFORMATION_SCHEMA read

# every table and view DuckDB holds, with what the registry knows of it (a table it never saw made, such as a generated
# source, was made when the emulator started)
RELATIONS = f"""
select t.database_name, t.schema_name, t.table_name as name,
    coalesce(r.kind, case when t.temporary then 'TEMPORARY TABLE' else 'TABLE' end) as kind,
    coalesce(r.transient, false) as transient, coalesce(r.created_on, _fs_global.main._fx_started()) as created_on,
    coalesce(r.last_altered, r.created_on, _fs_global.main._fx_started()) as last_altered,
    coalesce(r.last_ddl, r.created_on, _fs_global.main._fx_started()) as last_ddl,
    coalesce(r.owner, _fs_global.main._sf_current_role()) as owner, r.cluster_by, r.comment, coalesce(r.change_tracking, false) as change_tracking,
    coalesce(r.retention, case when t.temporary then 0 else 1 end) as retention, coalesce(r.secure, false) as secure, r.text, r.dynamic,
    coalesce(r.row_count, t.estimated_size) as rows,
    case when coalesce(r.row_count, t.estimated_size) = 0 then 0
         else greatest(1024, 512 * ceil(coalesce(r.row_count, t.estimated_size) * t.column_count * 1.5 / 512))::bigint end as bytes
from duckdb_tables() t
left join {TABLE} r on r.database_name = t.database_name and r.schema_name = t.schema_name and r.name = t.table_name
where t.schema_name not in ('_fs_information_schema') and t.database_name not in ('_fs_global', 'system', 'temp')
  and (t.database_name || '.' || t.schema_name || '.' || t.table_name) not in (select name from _fs_global.main._fx_standins)
union all
select v.database_name, v.schema_name, v.view_name as name, coalesce(r.kind, 'VIEW') as kind, false as transient,
    coalesce(r.created_on, _fs_global.main._fx_started()) as created_on, coalesce(r.last_altered, r.created_on, _fs_global.main._fx_started()),
    coalesce(r.last_ddl, r.created_on, _fs_global.main._fx_started()), coalesce(r.owner, _fs_global.main._sf_current_role()), null, r.comment,
    coalesce(r.change_tracking, false), null, coalesce(r.secure, false), coalesce(r.text, v.sql), null, null::bigint, null::bigint
from duckdb_views() v
left join {TABLE} r on r.database_name = v.database_name and r.schema_name = v.schema_name and r.name = v.view_name
where not v.internal and v.schema_name not in ('_fs_information_schema', 'main') and v.database_name not in ('_fs_global', 'system', 'temp')
  and (v.database_name || '.' || v.schema_name || '.' || v.view_name) not in (select name from _fs_global.main._fx_standins)
"""


def _linear(column: str) -> str:
    return f"case when {column} is null then null else 'LINEAR(' || {column} || ')' end"


DYNAMIC_COLUMNS = f"""r.created_on, r.name, r.database_name, r.schema_name, coalesce({_linear('r.cluster_by')}, '') as cluster_by, r.rows, r.bytes,
    r.owner, coalesce(json_extract_string(r.dynamic, '$.target_lag'),
                      case when json_extract_string(r.dynamic, '$.scheduler') = 'DISABLE' then '' end) as target_lag,
    json_extract_string(r.dynamic, '$.refresh_mode') as refresh_mode,
    null::varchar as refresh_mode_reason, json_extract_string(r.dynamic, '$.warehouse') as warehouse, coalesce(r.comment, '') as comment,
    r.text, case when r.cluster_by is null then 'OFF' else 'ON' end as automatic_clustering,
    json_extract_string(r.dynamic, '$.scheduling_state') as scheduling_state,
    json_extract_string(r.dynamic, '$.last_suspended_on')::timestamptz as last_suspended_on, 'false' as is_clone, 'false' as is_replica,
    'false' as is_iceberg, json_extract_string(r.dynamic, '$.data_timestamp')::timestamptz as data_timestamp, 'ROLE' as owner_role_type,
    null::varchar as execute_as_user, null::varchar as secondary_role_names, 'false' as insert_only_inputs,
    json_extract_string(r.dynamic, '$.immutable_where') as immutable_where,
    coalesce(json_extract_string(r.dynamic, '$.initialization_warehouse'), '') as initialization_warehouse, null::varchar as backfill_from,
    json_extract_string(r.dynamic, '$.scheduler') as scheduler, null::varchar as frozen_where,
    json_extract_string(r.dynamic, '$.configured_refresh_mode') as configured_refresh_mode"""

INTERACTIVE_COLUMNS = """r.created_on, r.name, r.database_name, r.schema_name, coalesce('(' || r.cluster_by || ')', '') as cluster_by, r.rows, r.bytes,
    r.owner, json_extract_string(r.dynamic, '$.target_lag') as target_lag, json_extract_string(r.dynamic, '$.warehouse') as refresh_warehouse,
    coalesce(r.comment, '') as comment, r.text, json_extract_string(r.dynamic, '$.scheduling_state') as scheduling_state,
    'ROLE' as owner_role_type, json_extract_string(r.dynamic, '$.initialization_warehouse') as initialization_warehouse,
    null::varchar as last_completed_refresh_state, json_extract_string(r.dynamic, '$.data_timestamp')::timestamptz as latest_data_timestamp"""

SHOW_TABLES = f"""
create or replace view _fs_global._fs_information_schema._fs_show_tables as
select r.created_on, r.name, r.database_name, r.schema_name,
    case when r.kind = 'TEMPORARY TABLE' then 'TEMPORARY' when r.transient then 'TRANSIENT' else 'TABLE' end as kind,
    coalesce(r.comment, '') as comment,
    coalesce(case when r.kind = 'INTERACTIVE TABLE' then '(' || r.cluster_by || ')' else {_linear('r.cluster_by')} end, '') as cluster_by,
    r.rows, r.bytes, r.owner, r.retention::varchar as retention_time, case when r.cluster_by is null then 'OFF' else 'ON' end as automatic_clustering,
    case when r.change_tracking then 'ON' else 'OFF' end as change_tracking, 'OFF' as search_optimization,
    null::bigint as search_optimization_progress, null::bigint as search_optimization_bytes, 'N' as is_external, 'N' as enable_schema_evolution,
    'ROLE' as owner_role_type, 'N' as is_event, 'N' as is_hybrid, 'N' as is_iceberg,
    case when r.kind = 'DYNAMIC TABLE' then 'Y' else 'N' end as is_dynamic, 'N' as is_immutable,
    case when r.kind = 'INTERACTIVE TABLE' then 'Y' else 'N' end as is_interactive, 'OFF' as row_timestamp, 'OFF' as error_logging
from ({RELATIONS}) r where r.kind not in ('VIEW', 'MATERIALIZED VIEW')
"""

SHOW_OBJECTS = f"""
create or replace view _fs_global._fs_information_schema._fs_show_objects as
select r.created_on, r.name, r.database_name, r.schema_name,
    case when r.kind in ('VIEW', 'MATERIALIZED VIEW') then r.kind when r.kind = 'TEMPORARY TABLE' then 'TEMPORARY' else 'TABLE' end as kind,
    coalesce(r.comment, '') as comment,
    coalesce(case when r.kind = 'INTERACTIVE TABLE' then '(' || r.cluster_by || ')' else {_linear('r.cluster_by')} end, '') as cluster_by,
    coalesce(r.rows, 0) as rows, coalesce(r.bytes, 0) as bytes, r.owner, coalesce(r.retention, 1)::varchar as retention_time,
    'ROLE' as owner_role_type, 'N' as is_hybrid, case when r.kind = 'DYNAMIC TABLE' then 'Y' else 'N' end as is_dynamic, 'N' as is_iceberg,
    case when r.kind = 'INTERACTIVE TABLE' then 'Y' else 'N' end as is_interactive
from ({RELATIONS}) r where true
"""

SHOW_VIEWS = f"""
create or replace view _fs_global._fs_information_schema._fs_show_views as
select r.created_on, r.name, '' as reserved, r.database_name, r.schema_name, r.owner, coalesce(r.comment, '') as comment, r.text,
    case when r.secure then 'true' else 'false' end as is_secure, case when r.kind = 'MATERIALIZED VIEW' then 'true' else 'false' end as is_materialized,
    'ROLE' as owner_role_type, case when r.change_tracking then 'ON' else 'OFF' end as change_tracking
from ({RELATIONS}) r where r.kind in ('VIEW', 'MATERIALIZED VIEW')
"""

# INFORMATION_SCHEMA.TABLES of one database, as Snowflake has it (measured: a view's transient, row count, size and retention
# are NULL; a temporary table is LOCAL TEMPORARY, with no transient flag and retention 0; an interactive table is INTERACTIVE TABLE)
INFORMATION_SCHEMA_TABLES = f"""
create view if not exists ${{catalog}}._fs_information_schema._fs_tables AS
select r.database_name as "TABLE_CATALOG", r.schema_name as "TABLE_SCHEMA", r.name as "TABLE_NAME", r.owner as "TABLE_OWNER",
    case when r.kind in ('VIEW', 'MATERIALIZED VIEW') then r.kind when r.kind = 'TEMPORARY TABLE' then 'LOCAL TEMPORARY'
         when r.kind = 'INTERACTIVE TABLE' then 'INTERACTIVE TABLE' else 'BASE TABLE' end as "TABLE_TYPE",
    case when r.kind in ('TABLE', 'DYNAMIC TABLE') then case when r.transient then 'YES' else 'NO' end end as "IS_TRANSIENT",
    {_linear('r.cluster_by')} as "CLUSTERING_KEY", r.rows as "ROW_COUNT", r.bytes as "BYTES", r.retention as "RETENTION_TIME",
    NULL::VARCHAR AS "SELF_REFERENCING_COLUMN_NAME", NULL::VARCHAR AS "REFERENCE_GENERATION", NULL::VARCHAR AS "USER_DEFINED_TYPE_CATALOG",
    NULL::VARCHAR AS "USER_DEFINED_TYPE_SCHEMA", NULL::VARCHAR AS "USER_DEFINED_TYPE_NAME",
    case when r.kind in ('VIEW', 'MATERIALIZED VIEW', 'DYNAMIC TABLE') then 'NO' else 'YES' end as "IS_INSERTABLE_INTO", 'YES' AS "IS_TYPED",
    case when r.kind = 'TEMPORARY TABLE' then 'PRESERVE ROWS' end AS "COMMIT_ACTION",
    r.created_on as "CREATED", r.last_altered as "LAST_ALTERED", r.last_ddl as "LAST_DDL", 'CI' as "LAST_DDL_BY",
    case when r.cluster_by is null then 'NO' else 'YES' end as "AUTO_CLUSTERING_ON", r.comment as "COMMENT",
    case when r.kind = 'TEMPORARY TABLE' then 'YES' else 'NO' end as "IS_TEMPORARY", 'NO' as "IS_ICEBERG",
    case when r.kind = 'DYNAMIC TABLE' then 'YES' else 'NO' end as "IS_DYNAMIC", 'NO' as "IS_IMMUTABLE", 'NO' as "IS_HYBRID",
    'NO' AS "ERROR_LOGGING", case when r.kind = 'INTERACTIVE TABLE' then 'YES' else 'NO' end AS "IS_INTERACTIVE", 'NO' AS "ROW_TIMESTAMP_ON"
from ({RELATIONS}) r
where r.database_name = '${{catalog}}'
"""


# ---------------------------------------------------------------- GET_DDL

_GET_DDL = re.compile(_LEAD + r"select\s+get_ddl\s*\(\s*'(?P<kind>[^']*)'\s*,\s*'(?P<name>(?:[^']|'')*)'\s*(?:,\s*(?P<full>true|false))?\s*\)"
                      r"\s*(?:as\s+(?P<alias>\"[^\"]+\"|\w+)\s*)?;?\s*$")


def _ident(name: str) -> str:
    return name if re.fullmatch(r"[A-Z_][A-Z0-9_$]*", name) else '"' + name.replace('"', '""') + '"'


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def get_ddl(cursor, kind: str, name: str, full: bool = False) -> str:  # noqa: ANN001, FBT001, FBT002, C901
    """GET_DDL of a table, view or dynamic table, written as Snowflake writes it (measured 2026-11-25): Snowflake's
    types, NOT NULL, DEFAULT, COLLATE and COMMENT in that order, unique keys before the primary key, foreign keys last,
    the clustering key ahead of the columns and the table's comment after them; a view or dynamic table lists its
    column names and keeps its query as written."""
    fq = qualified(cursor, name)
    duck = cursor._duck_conn
    shown = ".".join(_ident(p) for p in fq) if full else _ident(fq[2])
    is_view = duck.execute("select count(*) from duckdb_views() where database_name = ? and schema_name = ? and view_name = ?",
                           list(fq)).fetchone()[0] > 0
    exists = is_view or duck.execute("select count(*) from duckdb_tables() where database_name = ? and schema_name = ? and table_name = ?",
                                     list(fq)).fetchone()[0] > 0
    wanted = kind.strip().lower().replace(" ", "_")
    if not exists or (wanted == "view" and not is_view) or (wanted == "dynamic_table" and is_view):
        raise _error(f"SQL compilation error:\n{'View' if wanted == 'view' else 'Table'} '{'.'.join(fq)}' does not exist or not authorized.",
                     2003, "02000")
    reg = _row(duck, fq) or {}
    cur = cursor._conn.cursor()
    try:
        described = cur.execute(f"describe table {_sql_name(fq)}").fetchall()
    finally:
        cur.close()
    columns = [_ident(row[0]) for row in described]
    if is_view or reg.get("kind") == "DYNAMIC TABLE":
        listed = "(\n\t" + ",\n\t".join(columns) + "\n)"
        if is_view:
            text = reg.get("text") or duck.execute("select sql from duckdb_views() where database_name = ? and schema_name = ? "
                                                   "and view_name = ?", list(fq)).fetchone()[0]
            _, body = _head(re.sub(_LEAD + r"create\s+(?:or\s+replace\s+)?(?:\w+\s+)*?view\s+(?:if\s+not\s+exists\s+)?" + _NAME, "", text, count=1))
            secure = "secure " if reg.get("secure") else ""
            comment = f" COMMENT={_quoted(reg['comment'])}\n" if reg.get("comment") else ""
            return f"create or replace {secure}view {shown}{listed}{comment} as {body};"
        dynamic = json.loads(reg["dynamic"])
        options = []
        if dynamic.get("target_lag"):
            options.append(f"target_lag = '{dynamic['target_lag']}'")
        options += [f"refresh_mode = {dynamic.get('configured_refresh_mode', 'AUTO')}", f"initialize = {dynamic.get('initialize', 'ON_CREATE')}"]
        if dynamic.get("warehouse"):
            options.append(f"warehouse = {dynamic['warehouse']}")
        cluster = f" cluster by ({reg['cluster_by']})" if reg.get("cluster_by") else ""
        query = dynamic.get("written") or dynamic.get("query", "")
        transient = "transient " if reg.get("transient") else ""
        return f"create or replace {transient}dynamic table {shown}{listed} {' '.join(options)}\n{cluster} as {query};"
    lines = []
    for row in described:
        name_, type_, _kind, nullable, default = row[:5]
        comment = row[9] if len(row) > 9 else None
        line = f"{_ident(name_)} {type_}"
        if nullable == "N":
            line += " NOT NULL"
        if default is not None and str(default) != "":
            line += f" DEFAULT {default}"
        if comment:
            line += f" COMMENT {_quoted(comment)}"
        lines.append(line)
    try:
        keys = duck.execute("select c.name, c.kind, c.columns, c.ref_database, c.ref_schema, c.ref_table, c.ref_columns "
                            "from _fs_global.main._fx_live_constraints c where c.database_name = ? and c.schema_name = ? and c.table_name = ?",
                            list(fq)).fetchall()
    except Exception:  # noqa: BLE001
        keys = []
    order = {"UNIQUE": 0, "PRIMARY KEY": 1, "FOREIGN KEY": 2}
    for key_name, key_kind, key_columns, ref_db, ref_schema, ref_table, ref_columns in sorted(keys, key=lambda k: order.get(k[1], 3)):
        named = "" if not key_name or key_name.upper().startswith("SYS_CONSTRAINT_") else f"constraint {_ident(key_name)} "
        cols = ", ".join(_ident(c) for c in key_columns or [])
        line = f"{named}{key_kind.lower()} ({cols})"
        if key_kind == "FOREIGN KEY":
            line += f" references {_ident(ref_db)}.{_ident(ref_schema)}.{_ident(ref_table)}({', '.join(_ident(c) for c in ref_columns or [])})"
        lines.append(line)
    kind_word = "TEMPORARY TABLE" if reg.get("kind") == "TEMPORARY TABLE" else "TRANSIENT TABLE" if reg.get("transient") else "TABLE"
    cluster = f" cluster by ({reg['cluster_by']})" if reg.get("cluster_by") else " "
    comment = f"COMMENT={_quoted(reg['comment'])}\n" if reg.get("comment") else ""
    return f"create or replace {kind_word} {shown}{cluster}(\n\t" + ",\n\t".join(lines) + f"\n){comment};"


def _get_ddl(cursor, original, m: re.Match):  # noqa: ANN001, ANN202
    ddl = get_ddl(cursor, m.group("kind"), m.group("name").replace("''", "'"), (m.group("full") or "").lower() == "true")
    alias = m.group("alias") or f"GET_DDL('{m.group('kind').upper()}', '{m.group('name')}'" + (f", {m.group('full').upper()}" if m.group("full") else "") + ")"
    alias = alias.strip('"') if alias.startswith('"') else alias.upper()
    return original(cursor, "select " + _quoted(ddl).replace("\\", "\\\\") + ' as "' + alias.replace('"', '""') + '"')


# ---------------------------------------------------------------- grants on tables and views

GRANTS = "_fs_global.main._fx_grants"
# what ALL grants (measured): on a view also VIEW EXPANDED QUERY PROFILE
_ALL_TABLE = ["APPLY COMMENT", "APPLYBUDGET", "DELETE", "DELETE ERROR TABLE", "EVOLVE SCHEMA", "INSERT", "REBUILD", "REFERENCES", "SELECT",
              "SELECT ERROR TABLE", "TRUNCATE", "UPDATE"]
_OBJECT = r"(?:(?P<kind>table|view|dynamic\s+table|materialized\s+view|external\s+table|iceberg\s+table|interactive\s+table)\s+)?"
_GRANT = re.compile(_LEAD + r"grant\s+(?P<privs>.+?)\s+on\s+" + _OBJECT + r"(?P<name>" + _NAME + r")\s+to\s+(?P<grantees>.+?)"
                    r"(?P<option>\s+with\s+grant\s+option)?\s*;?\s*$")
_REVOKE = re.compile(_LEAD + r"revoke\s+(?P<grant_option>grant\s+option\s+for\s+)?(?P<privs>.+?)\s+on\s+" + _OBJECT + r"(?P<name>" + _NAME
                     + r")\s+from\s+(?P<grantees>.+?)(?:\s+(?:cascade|restrict))?\s*;?\s*$")
_SHOW_GRANTS = re.compile(_LEAD + r"show\s+grants\s+on\s+" + _OBJECT + r"(?P<name>" + _NAME + r")\s*;?\s*$")
_NOT_RELATIONS = re.compile(r"(?i)^(?:all|future)\s+\w+\s+in\s|^(?:schema|database|warehouse|function|procedure|integration|stage|"
                            r"file\s+format|sequence|stream|task|pipe|role|user|account|share|tag|masking|row\s+access)\b")


def ensure_grants(duck) -> None:  # noqa: ANN001
    duck.execute(f"create table if not exists {GRANTS} (database_name varchar, schema_name varchar, name varchar, privilege varchar, "
                 "grantee varchar, grant_option boolean, granted_by varchar, created_on timestamptz)")


def _relation_kind(duck, fq) -> str | None:  # noqa: ANN001
    """TABLE, VIEW or DYNAMIC_TABLE as SHOW GRANTS names it, or None when nothing has that name."""
    reg = _row(duck, fq) or {}
    if reg.get("kind") == "DYNAMIC TABLE":
        return "DYNAMIC_TABLE"
    if duck.execute("select count(*) from duckdb_views() where database_name = ? and schema_name = ? and view_name = ?", list(fq)).fetchone()[0]:
        return "VIEW"
    if duck.execute("select count(*) from duckdb_tables() where database_name = ? and schema_name = ? and table_name = ?", list(fq)).fetchone()[0]:
        return "TABLE"
    return None


_KNOWN = {*_ALL_TABLE, "VIEW EXPANDED QUERY PROFILE", "OWNERSHIP", "ALL", "ALL PRIVILEGES"}


def _privileges(text: str, kind: str) -> list[str]:
    words = [re.sub(r"\s+", " ", p.strip()).upper() for p in text.split(",") if p.strip()]
    out = []
    for w in words:
        if w in ("ALL", "ALL PRIVILEGES"):
            out += _ALL_TABLE + (["VIEW EXPANDED QUERY PROFILE"] if kind == "VIEW" else [])
        else:
            out.append(w)
    return list(dict.fromkeys(out))


def _grantees(text: str) -> list[str] | None:
    """The roles a GRANT names; None when it names something else (a share, a database role, a user)."""
    out = []
    for part in text.split(","):
        m = re.match(r'(?is)^\s*(?:role\s+)?("(?:[^"]|"")+"|[\w$]+)\s*$', part)
        if not m or re.match(r"(?is)^\s*(share|database\s+role|application|user)\b", part):
            return None
        out.append(_unquote(m.group(1)))
    return out


def grants_intercept(cursor, command, original):  # noqa: ANN001, ANN201, C901
    """GRANT / REVOKE of privileges on a table or view, and SHOW GRANTS ON one: kept as Snowflake keeps them."""
    if not isinstance(command, str):
        return None
    text = _command(command)
    duck = cursor._duck_conn
    if (m := _SHOW_GRANTS.match(text)) is not None:
        fq = qualified(cursor, m.group("name"))
        kind = _relation_kind(duck, fq)
        if kind is None:
            return None
        reg = _row(duck, fq) or {}
        owner, made = reg.get("owner") or _role(duck), reg.get("created_on") or now()
        rows = [(made, "OWNERSHIP", kind, ".".join(fq), "ROLE", owner, "true", owner, "ROLE")]
        rows += [(r[0], r[1], kind, ".".join(fq), "ROLE", r[2], "true" if r[3] else "false", r[4], "ROLE") for r in duck.execute(
            f"select created_on, privilege, grantee, grant_option, granted_by from {GRANTS} where database_name = ? and schema_name = ? "
            "and name = ? order by grantee, privilege", list(fq)).fetchall()]
        names = ["created_on", "privilege", "granted_on", "name", "granted_to", "grantee_name", "grant_option", "granted_by", "granted_by_role_type"]
        from . import scripting

        selects = ["select " + ", ".join(f'{_literal(v, n, scripting)} as "{n}"' for n, v in zip(names, row)) for row in rows]
        return original(cursor, " union all ".join(selects))
    grant, revoke = _GRANT.match(text), _REVOKE.match(text)
    m = grant or revoke
    if m is None or _NOT_RELATIONS.match(text[m.start("name") - len(m.group("kind") or "") - 1:].lstrip()) \
            or _NOT_RELATIONS.match(m.group("name")):
        return None
    grantees = _grantees(m.group("grantees"))
    if grantees is None:
        return None
    fq = qualified(cursor, m.group("name"))
    kind = _relation_kind(duck, fq)
    if kind is None:
        written = m.group("name").strip()
        shown = written.upper() if not written.startswith('"') else written.strip('"')
        raise _error(f"SQL compilation error:\n{'View' if (m.group('kind') or '').lower() == 'view' else 'Table'} '{shown}' does not exist or "
                     "not authorized.", 2003, "42S02")
    from . import account

    if account.roles_known(duck):
        for role in grantees:
            if not account._role_exists(cursor, role):
                raise _error(f"SQL compilation error:\nRole '{role}' does not exist or not authorized.", 2003, "02000")
    privileges = _privileges(m.group("privs"), kind)
    unknown = [p for p in (re.sub(r"\s+", " ", w.strip()).upper() for w in m.group("privs").split(",")) if p and p not in _KNOWN]
    if unknown:
        # a word that is no privilege: Snowflake's parser stops at what follows it (measured: `grant fake_privilege on t ...`
        # is "unexpected 'on'" at the position of ON)
        at_on = re.search(r"(?i)\s(on)\s", text[:m.start("name")])
        position = at_on.start(1) - text.rfind("\n", 0, at_on.start(1)) - 1 if at_on else 0
        raise _error(f"SQL compilation error:\nsyntax error line 1 at position {position} unexpected 'on'.", 1003, "42000")
    at, granter = now(), _role(duck)
    affected = 0
    for role in grantees:
        for p in privileges:
            where = [*fq, p, role]
            held = duck.execute(f"select grant_option from {GRANTS} where database_name = ? and schema_name = ? and name = ? and privilege = ? "
                                "and grantee = ?", where).fetchone()
            if grant:
                if held is None:
                    duck.execute(f"insert into {GRANTS} values (?, ?, ?, ?, ?, ?, ?, ?)", [*where, bool(m.group("option")), granter, at])
                elif m.group("option") and not held[0]:
                    duck.execute(f"update {GRANTS} set grant_option = true where database_name = ? and schema_name = ? and name = ? "
                                 "and privilege = ? and grantee = ?", where)
            elif held is not None:
                affected += 1
                if m.group("grant_option"):
                    duck.execute(f"update {GRANTS} set grant_option = false where database_name = ? and schema_name = ? and name = ? "
                                 "and privilege = ? and grantee = ?", where)
                else:
                    duck.execute(f"delete from {GRANTS} where database_name = ? and schema_name = ? and name = ? and privilege = ? "
                                 "and grantee = ?", where)
    if revoke:
        return _status(original, cursor, f"Statement executed successfully. {affected} objects affected.")
    return _status(original, cursor, "Statement executed successfully.")


def _grants_follow(duck, fq, *, dropped: bool = False, to=None) -> None:  # noqa: ANN001
    """A relation made again without COPY GRANTS, or dropped, loses its grants; one renamed keeps them."""
    if to is not None:
        duck.execute(f"update {GRANTS} set database_name = ?, schema_name = ?, name = ? where database_name = ? and schema_name = ? and name = ?",
                     [*to, *fq])
    elif dropped:
        duck.execute(f"delete from {GRANTS} where database_name = ? and schema_name = ? and name = ?", list(fq))



# ---------------------------------------------------------------- CREATE TABLE t (columns) AS query

_CTAS_COLUMNS = re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?(?:(?:local|global)\s+)?(?:(?:transient|temporary|temp)\s+)?table\s+"
                           r"(?:if\s+not\s+exists\s+)?" + _NAME + r"\s*\(")


def _ctas_with_columns(cursor, original, text: str):  # noqa: ANN001, ANN202
    """A table made with its columns declared and filled from a query (dbt's contracts): as Snowflake does it, the table
    as declared (NOT NULL enforced: 100072 for a NULL), filled with the query's rows by position, and put in place only
    when that worked; a CREATE OR REPLACE that fails leaves the table there was."""
    from sqlglot import exp, parse_one

    try:
        tree = parse_one(text, read="snowflake")
    except Exception:  # noqa: BLE001
        return None
    if not (isinstance(tree, exp.Create) and str(tree.args.get("kind") or "").upper() == "TABLE" and isinstance(tree.this, exp.Schema)
            and tree.expression is not None and any(isinstance(c, exp.ColumnDef) for c in tree.this.expressions)):
        return None
    target = tree.this.this
    fq = qualified(cursor, ".".join(p for p in (target.catalog, target.db, target.name) if p))
    duck = cursor._duck_conn
    exists = duck.execute("select count(*) from duckdb_tables() where database_name = ? and schema_name = ? and table_name = ?",
                          list(fq)).fetchone()[0] > 0
    if exists and tree.args.get("exists"):
        return _status(original, cursor, f"{fq[2]} already exists, statement succeeded.")
    if exists and not tree.args.get("replace"):
        raise _error(f"SQL compilation error:\nObject '{fq[2]}' already exists.", 2002, "42710")
    scratch = (fq[0], fq[1], fq[2] + "__FX_CTAS")
    query = tree.expression.copy()
    declared = tree.copy()
    declared.set("expression", None)
    declared.set("replace", True)
    declared.set("exists", False)
    declared.this.set("this", exp.to_table(_sql_name(scratch), dialect="duckdb"))
    names = [c.this.copy() for c in tree.this.expressions if isinstance(c, exp.ColumnDef)]
    cur = cursor._conn.cursor()
    try:
        cur.execute(declared.sql(dialect="snowflake"))
        try:
            columns = exp.Tuple(expressions=[exp.column(n) for n in names])
            cur.execute(f"insert into {_sql_name(scratch)} {columns.sql(dialect='snowflake')} {query.sql(dialect='snowflake')}")
        except Exception as e:
            cur.execute(f"drop table if exists {_sql_name(scratch)}")
            if getattr(e, "errno", None) == 100072:  # (named as Snowflake names it: the table the statement makes, in full)
                raise _error(re.sub(r"to table \S+ failed", f"to table {'.'.join(fq)} failed", str(e.msg)), 100072, "22000") from e
            raise
        if exists:
            cur.execute(f"drop table {_sql_name(fq)}")
        duck.execute(f"alter table {_sql_name(scratch)} rename to {_sql_name((fq[2],))}")
    finally:
        cur.close()
    note(cursor, text)  # (what Snowflake keeps about it: made now, by this role)
    return _status(original, cursor, f"Table {fq[2]} successfully created.")



# ---------------------------------------------------------------- CREATE TABLE t CLONE s

_CLONE = re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?(?:(?P<kind>transient|temporary|temp)\s+)?table\s+(?P<exists>if\s+not\s+exists\s+)?"
                    r"(?P<name>" + _NAME + r")\s+clone\s+(?P<source>" + _NAME + r")(?P<rest>(?:\s+copy\s+(?:grants|tags))*)\s*;?\s*$")


def _clone(cursor, original, m: re.Match, text: str):  # noqa: ANN001, ANN202
    """A table's clone: its definition (columns, defaults, NOT NULL) and rows as they are now. (AT / BEFORE clones go through
    time travel: history.py.) COPY GRANTS keeps what was granted on the table it replaces."""
    fq, source = qualified(cursor, m.group("name")), qualified(cursor, m.group("source"))
    duck = cursor._duck_conn
    made = duck.execute("select sql from duckdb_tables() where database_name = ? and schema_name = ? and table_name = ?", list(source)).fetchone()
    if made is None:
        raise _error(f"SQL compilation error:\nObject '{'.'.join(source)}' does not exist or not authorized.", 2003, "02000")
    exists = duck.execute("select count(*) from duckdb_tables() where database_name = ? and schema_name = ? and table_name = ?",
                          list(fq)).fetchone()[0] > 0
    if exists and m.group("exists"):
        return _status(original, cursor, f"{fq[2]} already exists, statement succeeded.")
    if exists and not m.group("replace"):
        raise _error(f"SQL compilation error:\nObject '{fq[2]}' already exists.", 2002, "42710")
    kept_grants = duck.execute(f"select * from {GRANTS} where database_name = ? and schema_name = ? and name = ?", list(fq)).fetchall() \
        if exists and re.search(r"(?i)copy\s+grants", m.group("rest") or "") else []
    scratch = (fq[0], fq[1], fq[2] + "__FX_CLONE")
    # DuckDB's own statement for the source, made under the scratch name: CREATE TABLE <name>(<columns>);
    ddl = re.sub(r"(?is)^\s*create\s+table\s+(?:\S+?)\s*\(", f"create table {_sql_name(scratch)}(", made[0], count=1)
    duck.execute(f"drop table if exists {_sql_name(scratch)}")
    duck.execute(ddl)
    duck.execute(f"insert into {_sql_name(scratch)} select * from {_sql_name(source)}")
    if exists:
        duck.execute(f"drop table {_sql_name(fq)}")
    duck.execute(f"alter table {_sql_name(scratch)} rename to {_sql_name((fq[2],))}")
    note(cursor, text.replace(m.group("rest") or "", "") if m.group("rest") else text)  # (made now; what the source had: see _note)
    if kept_grants:
        duck.executemany(f"insert into {GRANTS} values (?, ?, ?, ?, ?, ?, ?, ?)", [list(g) for g in kept_grants])
    return _status(original, cursor, f"Table {fq[2]} successfully created.")
