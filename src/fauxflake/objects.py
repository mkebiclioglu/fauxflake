"""Snowflake objects that are not tables or views.

fakesnow knows tables, views, sequences, stages and file formats. Real projects also create
functions, procedures, streams, tasks and policies, usually from hooks and macros. This module
intercepts those statements before they reach the transpiler and gives each one a local meaning:

* functions      SQL -> DuckDB macro; Python -> the handler itself; JavaScript -> Duktape
* procedures     SQL (Snowflake Scripting, interpreted by `scripting.py`), JavaScript, Python
* streams        the rows that changed since the stream was last consumed by a DML statement
* tasks          stored; `EXECUTE TASK` runs the body, then the resumed tasks declared AFTER it
* policies       masking and row access policies, applied wherever the table is read
* pipes, alerts  stored; `ALTER PIPE ... REFRESH` and `EXECUTE ALERT` run them once

State lives with the emulator instance, so every connection sees the same objects.
"""

from __future__ import annotations

import json
import re
import sys
import threading
from dataclasses import dataclass, field

import sqlglot
from snowflake.connector.errors import ProgrammingError
from sqlglot import exp

from . import scripting, semantics

_LEAD = r"(?is)^(?:\s|--[^\n]*\n|//[^\n]*\n|/\*.*?\*/)*"
_NAME = r"[\w$.\"]+"
_KEY = "__fauxflake_objects__"


@dataclass
class State:
    functions: dict[str, dict[int, dict]] = field(default_factory=dict)  # name -> arity -> definition
    procedures: dict[str, dict] = field(default_factory=dict)
    streams: dict[tuple[str, str, str], dict] = field(default_factory=dict)
    tasks: dict[str, dict] = field(default_factory=dict)
    pipes: dict[str, str] = field(default_factory=dict)
    alerts: dict[str, dict] = field(default_factory=dict)
    masks: dict[tuple[str, str, str], dict[str, tuple[str, list[str]]]] = field(default_factory=dict)
    row_policies: dict[tuple[str, str, str], list[tuple[str, list[str]]]] = field(default_factory=dict)
    pinned: set[str] = field(default_factory=set)  # functions the spec defines: the project cannot replace them
    native: list[str] = field(default_factory=list)  # Python functions registered in DuckDB
    js: object = None
    lock: threading.RLock = field(default_factory=threading.RLock)


def state_of(conn) -> State:  # noqa: ANN001
    """The objects of the emulator this fakesnow connection belongs to."""
    return conn.results_cache.setdefault(_KEY, State())


def error(message: str) -> ProgrammingError:
    return ProgrammingError(msg=message, errno=2003, sqlstate="42S02")


# ---------------------------------------------------------------- small parsers

def _unquote(part: str) -> str:
    return part[1:-1] if part.startswith('"') and part.endswith('"') else part.upper()


def bare(name: str) -> str:
    """The last part of a possibly qualified name, the way Snowflake resolves it."""
    return _unquote(re.findall(r'"[^"]*"|[^.]+', name.strip())[-1])


def qualified(cursor, name: str) -> tuple[str, str, str]:  # noqa: ANN001
    parts = [_unquote(p) for p in re.findall(r'"[^"]*"|[^.]+', name.strip())]
    conn = cursor._conn
    defaults = [conn.database or "", conn.schema or ""]
    return tuple((defaults + parts)[-3:]) if len(parts) < 3 else tuple(parts[-3:])  # type: ignore[return-value]


def _sql_name(fq: tuple[str, str, str]) -> str:
    return ".".join('"' + p.replace('"', '""') + '"' for p in fq)


def find_keyword(text: str, keyword: str, start: int = 0) -> int:
    """Position of `keyword` as a whole word outside quotes and parentheses, or -1."""
    depth, i, n = 0, start, len(text)
    pattern = re.compile(r"\b" + keyword + r"\b", re.IGNORECASE)
    while i < n:
        ch = text[i]
        if ch == "'":
            i += 1
            while i < n and (text[i] != "'" or text[i + 1:i + 2] == "'"):
                i += 2 if text[i] in "'\\" else 1
        elif ch == '"':
            i = text.find('"', i + 1)
            if i < 0:
                return -1
        elif text.startswith("$$", i):
            i = text.find("$$", i + 2) + 1
            if i <= 0:
                return -1
        elif text.startswith("--", i):
            i = text.find("\n", i)
            if i < 0:
                return -1
        elif text.startswith("/*", i):
            i = text.find("*/", i) + 1
            if i <= 0:
                return -1
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and (ch.isalpha() or ch == "_") and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] in "_$")):
            m = pattern.match(text, i)
            if m:
                return i
            while i + 1 < n and (text[i + 1].isalnum() or text[i + 1] in "_$"):
                i += 1
        i += 1
    return -1


def split_top(text: str, sep: str = ",") -> list[str]:
    """Split on a separator that is outside quotes and brackets."""
    out, depth, start, i, n = [], 0, 0, 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            i += 1
            while i < n and (text[i] != "'" or text[i + 1:i + 2] == "'"):
                i += 2 if text[i] in "'\\" else 1
        elif ch == '"':
            i = text.find('"', i + 1)
            if i < 0:
                break
        elif text.startswith("$$", i):
            i = text.find("$$", i + 2) + 1
            if i <= 0:
                break
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == sep and depth == 0:
            out.append(text[start:i])
            start = i + 1
        i += 1
    out.append(text[start:])
    return [p.strip() for p in out if p.strip()]


def _close_paren(text: str, open_at: int) -> int:
    depth, i, n = 0, open_at, len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            i += 1
            while i < n and (text[i] != "'" or text[i + 1:i + 2] == "'"):
                i += 2 if text[i] in "'\\" else 1
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise error("unbalanced parentheses")


def duck_type(sf_type: str) -> str:
    """The DuckDB type a Python or JavaScript handler exchanges for a Snowflake type."""
    t = sf_type.upper().strip()
    base = re.match(r"[A-Z_0-9 ]*", t).group(0).strip()
    if base in ("NUMBER", "DECIMAL", "NUMERIC"):
        scale = re.search(r",\s*(\d+)\s*\)", t)
        return "DOUBLE" if scale and int(scale.group(1)) > 0 else "BIGINT"
    if base in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return "BIGINT"
    if base in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return "DOUBLE"
    if base in ("BOOLEAN", "DATE", "TIME"):
        return base
    if base.startswith(("TIMESTAMP_TZ", "TIMESTAMP_LTZ")):
        return "TIMESTAMPTZ"
    if base.startswith(("TIMESTAMP", "DATETIME")):
        return "TIMESTAMP"
    if base in ("VARIANT", "OBJECT", "ARRAY", "MAP"):
        return "JSON"
    if base in ("BINARY", "VARBINARY"):
        return "BLOB"
    return "VARCHAR"


def plain(value):  # noqa: ANN001, ANN201
    """A value JSON can carry: what a JavaScript handler or a Scripting variable receives."""
    import datetime
    import decimal

    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat(sep=" ") if isinstance(value, datetime.datetime) else value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


def _status(cursor, original, text: str):  # noqa: ANN001, ANN202
    return original(cursor, "select '" + text.replace("'", "''") + "' as \"status\"")


def _run(cursor, sql: str) -> tuple[list[str], list[tuple]]:  # noqa: ANN001
    """Run one Snowflake statement on a fresh cursor of the same session."""
    cur = cursor._conn.cursor()
    try:
        cur.execute(sql)
        columns = [d[0] for d in (cur.description or [])]
        return columns, list(cur.fetchall() or []) if columns else []
    finally:
        cur.close()


# ---------------------------------------------------------------- functions and procedures

_CREATE_CALLABLE = re.compile(
    _LEAD + r"create\s+(?:or\s+replace\s+)?(?P<mods>(?:(?:secure|temporary|temp|external|aggregate|transient)\s+)*)"
            r"(?P<kind>function|procedure)\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")\s*\(")
_BODY = re.compile(r"(?is)\bas\s*(?:\$\$(?P<dollar>.*)\$\$|'(?P<quoted>(?:[^']|''|\\')*)')\s*;?\s*$")


def _parse_callable(sql: str) -> dict | None:
    m = _CREATE_CALLABLE.match(sql)
    if not m:
        return None
    close = _close_paren(sql, m.end() - 1)
    args = []
    for part in split_top(sql[m.end():close]):
        words = part.split(None, 1)
        kind = re.split(r"(?i)\s+default\s+|\s*:=", words[1])[0].strip() if len(words) > 1 else "VARCHAR"
        args.append((_unquote(words[0]), kind))
    rest = sql[close + 1:]
    body = _BODY.search(rest)
    options = rest[:body.start()] if body else rest
    text = None
    if body:
        text = body.group("dollar") if body.group("dollar") is not None else body.group("quoted").replace("''", "'")
    returns = re.search(r"(?is)\breturns\s+(table\s*\(|[\w ]+?(?:\([^)]*\))?)(?=\s+(?:not\s+null|null|language|called|returns|strict|volatile|immutable|"
                        r"memoizable|comment|runtime_version|packages|imports|handler|execute|as|api_integration|external_access_integrations|secrets|"
                        r"copy|target_path|artifact_repository)\b|\s*$|\s*->)", options)
    language = re.search(r"(?i)\blanguage\s+(\w+)", options)
    handler = re.search(r"(?i)\bhandler\s*=\s*'([^']+)'", options)
    kind = returns.group(1).strip() if returns else "VARCHAR"
    return {
        "kind": m.group("kind").lower(), "name": bare(m.group("name")), "args": args, "body": text,
        "table": kind.lower().startswith("table"), "returns": "VARCHAR" if kind.lower().startswith("table") else kind,
        "language": (language.group(1).lower() if language else "sql"),
        "handler": handler.group(1) if handler else None,
        "external": "external" in m.group("mods").lower() or bool(re.search(r"(?i)\bapi_integration\b", options)),
        "options": options,
    }


def _transpile_expression(cursor, body: str) -> str:  # noqa: ANN001
    text = body.strip().rstrip(";").strip()
    query = re.match(r"(?is)(select|with)\b", text)
    tree = sqlglot.parse_one(f"select ({text})" if query else f"select {text}", read="snowflake")
    return cursor._transform(tree, None).expressions[0].unalias().sql(dialect="duckdb")


def _transpile_query(cursor, body: str) -> str:  # noqa: ANN001
    tree = sqlglot.parse_one(body.strip().rstrip(";"), read="snowflake")
    return cursor._transform(tree, None).sql(dialect="duckdb")


def _params(args: list[tuple[str, str]]) -> str:
    return ", ".join('"' + name + '"' for name, _ in args)


def _js(state: State):  # noqa: ANN202
    if state.js is None:
        state.js = scripting.JavaScript()
    return state.js


class _MissingPackage(ProgrammingError):
    def __init__(self, why: str) -> None:
        super().__init__(msg=why, errno=2003, sqlstate="42S02")
        self.why = why

    def __str__(self) -> str:
        return self.why


def _python_handler(defn: dict):  # noqa: ANN202
    namespace: dict = {"__name__": f"fauxflake_udf_{defn['name'].lower()}"}
    try:
        exec(compile(defn["body"], f"<{defn['kind']} {defn['name']}>", "exec"), namespace)  # noqa: S102 - the project's own code
    except ImportError as e:
        raise _MissingPackage(f"its Python code imports {e.name!r}, which is not installed locally") from e
    target = namespace
    for part in (defn["handler"] or "main").split("."):
        target = target[part] if isinstance(target, dict) else getattr(target, part)
    return target


def _convert_in(value, kind: str):  # noqa: ANN001, ANN202
    return json.loads(value) if kind == "JSON" and isinstance(value, str) else value


def _convert_out(value, kind: str):  # noqa: ANN001, ANN202
    if value is None:
        return None
    if kind == "JSON":
        return json.dumps(plain(value))
    if kind == "BIGINT":
        return int(value)
    if kind == "DOUBLE":
        return float(value)
    if kind == "VARCHAR" and not isinstance(value, str):
        return json.dumps(plain(value)) if isinstance(value, (dict, list)) else str(value)
    return value


def _register_native(cursor, state: State, defn: dict) -> str:  # noqa: ANN001
    """Register a Python callable for a Python or JavaScript function; returns its DuckDB name."""
    name = f"_udfx_{defn['name'].lower()}_{len(defn['args'])}"
    kinds = [duck_type(k) for _, k in defn["args"]]
    out_kind = duck_type(defn["returns"])
    if defn["language"] == "python":
        handler = _python_handler(defn)

        def call(*values):  # noqa: ANN002, ANN202
            return _convert_out(handler(*[_convert_in(v, k) for v, k in zip(values, kinds)]), out_kind)
    else:
        engine = _js(state)
        key = engine.define(name, [a for a, _ in defn["args"]], defn["body"])

        def call(*values):  # noqa: ANN002, ANN202
            return _convert_out(engine.call(key, [plain(_convert_in(v, k)) for v, k in zip(values, kinds)]), out_kind)
    duck = cursor._duck_conn
    try:
        duck.remove_function(name)
    except Exception:  # noqa: BLE001, S110 - not registered yet
        pass
    duck.create_function(name, call, kinds, out_kind, null_handling="special", side_effects=True)
    if name not in state.native:
        state.native.append(name)
    return name


def _define_function(cursor, state: State, defn: dict) -> None:  # noqa: ANN001
    name = defn["name"].lower()
    if name in state.pinned:
        return  # the spec gives this function a stand-in, which wins over the project's definition
    args = defn["args"]
    if defn["external"] or defn["language"] not in ("sql", "python", "javascript"):
        why = "external functions call a service" if defn["external"] else f"{defn['language']} handlers do not run locally"
        print(f"fauxflake: function {defn['name']} returns NULL here ({why}); give it a stand-in under `udfs:` in the spec.", file=sys.stderr)
        defn["duck"] = "null::json" if duck_type(defn["returns"]) == "JSON" else "null"
    elif defn["table"]:
        if defn["language"] != "sql":
            raise error(f"{defn['name']}: table functions written in {defn['language']} are not supported; "
                        "give it a stand-in under `udfs:` in the spec.")
        defn["duck"] = _transpile_query(cursor, defn["body"])
    elif defn["language"] == "sql":
        defn["duck"] = _transpile_expression(cursor, defn["body"])
    else:
        try:
            native = _register_native(cursor, state, defn)
        except _MissingPackage as e:
            if name in semantics.UDFS:
                print(f"fauxflake: function {defn['name']} keeps its stand-in from the spec ({e}).", file=sys.stderr)
                return
            print(f"fauxflake: function {defn['name']} returns NULL here ({e}). Install the package next to fauxflake "
                  "to run the real code, or give the function a stand-in under `udfs:` in the spec.", file=sys.stderr)
            native = None
        passed = ", ".join(f'to_json("{a}")' if duck_type(k) == "JSON" else f'cast("{a}" as {duck_type(k)})' for a, k in args)
        defn["duck"] = f"{native}({passed})" if native else ("null::json" if duck_type(defn["returns"]) == "JSON" else "null")
    overloads = state.functions.setdefault(name, {})
    if overloads and next(iter(overloads.values()))["table"] != defn["table"]:
        overloads.clear()
    overloads[len(args)] = defn
    macro = ("_udtf_" if defn["table"] else "_udf_") + name
    bodies = ", ".join(f"({_params(d['args'])}) as " + (f"table ({d['duck']})" if d["table"] else f"({d['duck']})") for d in overloads.values())
    cursor._duck_conn.execute(f"create or replace macro _fs_global.main.{macro}{bodies}")
    (semantics.UDTFS if defn["table"] else semantics.UDFS)[name] = macro
    (semantics.UDFS if defn["table"] else semantics.UDTFS).pop(name, None)


def pin(emu, names) -> None:  # noqa: ANN001
    """Functions the spec defines keep their stand-in whatever the project creates later."""
    emu._fs.results_cache.setdefault(_KEY, State()).pinned = {n.lower() for n in names}


def _session(cursor):  # noqa: ANN001, ANN202
    """What a Python procedure or model gets as `session`: a real Snowpark session on this
    connection when Snowpark is installed, the small built-in one otherwise."""
    return scripting.snowpark_session(cursor._conn) or scripting.Session(lambda sql: _run(cursor, sql))


def call_procedure(cursor, proc: dict, values: list):  # noqa: ANN001, ANN201
    """Run a stored procedure; returns its value, or a scripting.Table for `RETURN TABLE(...)`."""
    names = [a for a, _ in proc["args"]]
    if len(values) != len(names):
        raise error(f"procedure {proc['name']} takes {len(names)} argument(s), got {len(values)}")
    run = lambda sql: _run(cursor, sql)  # noqa: E731
    if proc["language"] == "sql":
        return scripting.Script(run).run(proc["body"], dict(zip(names, map(plain, values))))
    if proc["language"] == "javascript":
        return scripting.JavaScript().procedure(names, proc["body"], [plain(v) for v in values], run)
    if proc["language"] == "python":
        session = _session(cursor)
        try:
            return _python_handler(proc)(session, *values)
        finally:
            scripting.release(session)
    raise error(f"procedure {proc['name']}: {proc['language']} handlers do not run locally")


# `WITH name AS PROCEDURE (...) ... AS $$...$$ CALL name(...)`: an anonymous procedure, which is how dbt runs a Python model
_ANONYMOUS = re.compile(_LEAD + r"with\s+(?P<name>" + _NAME + r")\s+as\s+procedure\s*(?P<rest>\(.*)\bcall\s+(?P=name)\s*\((?P<args>[^)]*)\)\s*;?\s*$")
_CALL = re.compile(_LEAD + r"call\s+(?P<name>" + _NAME + r")\s*\((?P<args>.*)\)\s*(?:into\s+:\w+\s*)?;?\s*$")


def _call(cursor, original, state: State, m: re.Match):  # noqa: ANN001, ANN202
    name = bare(m.group("name"))
    proc = state.procedures.get(name)
    if proc is None:
        if name.startswith("SYSTEM$"):
            return _status(cursor, original, "Statement executed successfully.")
        raise error(f"Unknown procedure {name}. It is not created by the project's hooks; "
                    "create it in an on-run-start hook or call it only on Snowflake.")
    args = m.group("args").strip()
    values = list(_run(cursor, f"select {args}")[1][0]) if args else []
    result = call_procedure(cursor, proc, values)
    return _result(cursor, original, result, name)


def _result(cursor, original, result, name: str):  # noqa: ANN001, ANN202
    if isinstance(result, scripting.Table):
        return original(cursor, result.query)
    if hasattr(result, "queries") and getattr(result, "queries", None):  # a Snowpark DataFrame
        return original(cursor, result.queries["queries"][-1])
    if hasattr(result, "to_dict") and hasattr(result, "columns"):  # a pandas DataFrame
        return original(cursor, scripting.Session(lambda sql: _run(cursor, sql)).create_dataframe(result).query)
    return original(cursor, f'select {scripting.literal(plain(result))} as "{name}"')


# ---------------------------------------------------------------- streams

_STREAM = re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?stream\s+(?P<exists>if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")\s+"
                             r"(?:copy\s+grants\s+)?on\s+(?:external\s+table|dynamic\s+table|event\s+table|table|view)\s+(?P<source>" + _NAME + r")(?P<options>.*)$")
_DML = re.compile(_LEAD + r"(insert|merge|update|delete|copy\s+into|create\s+(or\s+replace\s+)?(transient\s+|temporary\s+|temp\s+)*table)\b")


def _create_stream(cursor, original, state: State, m: re.Match):  # noqa: ANN001, ANN202
    fq = qualified(cursor, m.group("name"))
    if fq in state.streams and m.group("exists"):
        return _status(cursor, original, f"{fq[2]} already exists, statement succeeded.")
    source = _sql_name(qualified(cursor, m.group("source")))
    options = m.group("options")
    append_only = bool(re.search(r"(?i)\b(append_only|insert_only)\s*=\s*true", options))
    initial = bool(re.search(r"(?i)\bshow_initial_rows\s*=\s*true", options))
    base = _sql_name((fq[0], fq[1], f"__FX_STREAM_{fq[2]}"))
    duck = cursor._duck_conn
    duck.execute(f"create or replace table {base} as select * from {source}" + (" where false" if initial else ""))
    meta = "false as \"METADATA$ISUPDATE\", md5(cast(t as varchar)) as \"METADATA$ROW_ID\""
    view = f"select t.*, 'INSERT' as \"METADATA$ACTION\", {meta} from (select * from {source} except all select * from {base}) t"
    if not append_only:
        view += f" union all select t.*, 'DELETE' as \"METADATA$ACTION\", {meta} from (select * from {base} except all select * from {source}) t"
    duck.execute(f"create or replace view {_sql_name(fq)} as {view}")
    state.streams[fq] = {"source": source, "base": base, "mode": "APPEND_ONLY" if append_only else "DEFAULT"}
    return _status(cursor, original, f"Stream {fq[2]} successfully created.")


def consume_streams(cursor, command: str) -> None:  # noqa: ANN001
    """A DML statement that reads a stream moves its offset to now, as committing does on Snowflake."""
    state = cursor._conn.results_cache.get(_KEY)
    if not state or not state.streams or not _DML.match(command):
        return
    for fq, stream in state.streams.items():
        if re.search(r'(?i)(?<![\w$])"?' + re.escape(fq[2]) + r'"?(?![\w$])', command):
            cursor._duck_conn.execute(f"create or replace table {stream['base']} as select * from {stream['source']}")


# ---------------------------------------------------------------- COPY INTO from cloud storage

LOCATIONS: dict[str, str] = {}  # the spec's `locations:`: URL prefix -> local directory holding the same files
_COPY = re.compile(_LEAD + r"copy\s+into\s+(?P<table>" + _NAME + r")\s*(?P<columns>\([^)]*\))?\s*from\s+'?(?P<url>(?:s3|s3a|gcs|gs|azure|https?)://[^\s']+)'?(?P<rest>.*)$")
_COPY_COLUMNS = ["file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen", "first_error", "first_error_line",
                 "first_error_character", "first_error_column_name"]


def _option(text: str, name: str) -> str | None:
    m = re.search(r"(?is)\b" + name + r"\s*=\s*('(?:[^']|'')*'|[^\s)(]+|\()", text)
    if not m:
        return None
    if m.group(1) == "(":  # a parenthesised value, possibly with nested parentheses
        return text[m.end() - 1:_close_paren(text, m.end() - 1) + 1]
    return m.group(1)


def _unquoted(value: str | None) -> str | None:
    return value[1:-1].replace("''", "'") if value and value.startswith("'") and value.endswith("'") else value


def strip_comments(text: str) -> str:
    """The statement without `--` and `/* */` comments, leaving string literals alone."""
    out, i, n = [], 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            j = i + 1
            while j < n and (text[j] != "'" or text[j + 1:j + 2] == "'"):
                j += 2 if text[j] in "'\\" else 1
            out.append(text[i:j + 1])
            i = j + 1
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i)
            i = n if j < 0 else j + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _copy_into(cursor, original, state: State, m: re.Match):  # noqa: ANN001, ANN202
    """`COPY INTO t FROM s3://bucket/prefix FILE_FORMAT = (...) PATTERN = '...'`: DuckDB reads the files.
    A public bucket needs nothing; a private one uses the AWS credentials in the environment. A prefix
    listed under `locations:` in the spec is read from a local directory instead."""
    duck, rest = cursor._duck_conn, strip_comments(m.group("rest"))
    url = m.group("url").rstrip("/")
    local = next((path for prefix, path in LOCATIONS.items() if url.startswith(prefix.rstrip("/"))), None)
    if local is not None:
        prefix = next(p for p in LOCATIONS if url.startswith(p.rstrip("/")))
        root = local.rstrip("/") + url[len(prefix.rstrip("/")):]
    else:
        root = url.replace("gcs://", "gs://").replace("s3a://", "s3://")
        duck.execute("install httpfs; load httpfs")
        if root.startswith("s3://"):
            import os

            if os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_PROFILE"):
                duck.execute("create or replace secret _fx_s3 (type s3, provider credential_chain)")
    listed = [r[0] for r in duck.execute("select file from glob(?)", [root + "/**"]).fetchall()]
    if not listed and re.search(r"\.\w+$", root):
        listed = [r[0] for r in duck.execute("select file from glob(?)", [root]).fetchall()]
    pattern = _unquoted(_option(rest, "pattern"))
    files = _option(rest, "files")
    if files:
        wanted = [_unquoted(f) for f in split_top(files.strip("()"))]
        listed = [f for f in listed if any(f.endswith("/" + w.lstrip("/")) for w in wanted)]
    if pattern:
        listed = [f for f in listed if re.fullmatch(pattern, f[len(root) + 1:]) or re.fullmatch(pattern, f)
                  or re.fullmatch(pattern, "/" + f.split("://", 1)[-1].split("/", 1)[-1])]
    fmt = _option(rest, "file_format") or "()"
    named = re.search(r"(?i)format_name\s*=\s*'?([\w$.\"]+)'?", fmt)
    if named or not fmt.startswith("("):  # a named file format: let the emulator's own COPY resolve it
        raise error("COPY INTO from a URL with a named file format: declare the options inline (FILE_FORMAT = (TYPE = CSV ...))")
    kind = (_unquoted(_option(fmt, "type")) or "CSV").upper()
    table = _sql_name(qualified(cursor, m.group("table")))
    columns = m.group("columns") or ""
    target = [r[0] for r in duck.execute(f"describe {table}").fetchall()]
    if columns:
        target = [bare(c) for c in split_top(columns.strip("()"))]
    rows = []
    for f in sorted(listed):
        if kind == "CSV":
            header = int(_option(fmt, "skip_header") or 0) > 0 or (_option(fmt, "parse_header") or "").lower() == "true"
            delim = _unquoted(_option(fmt, "field_delimiter")) or ","
            quote = _unquoted(_option(fmt, "field_optionally_enclosed_by"))
            nulls = [_unquoted(v) for v in split_top((_option(fmt, "null_if") or "()").strip("()"))]
            options = [f"header = {'true' if header else 'false'}", "delim = " + scripting.literal(delim.replace("\\t", "\t")),
                       "all_varchar = true", "auto_detect = false", "null_padding = true",
                       "columns = {" + ", ".join(f"'c{i}': 'VARCHAR'" for i in range(len(target))) + "}"]
            options.append("quote = " + scripting.literal(quote if quote and quote.upper() != "NONE" else ""))
            if nulls:
                options.append("nullstr = [" + ", ".join(scripting.literal(v) for v in ["", *nulls]) + "]")
            source = f"read_csv({scripting.literal(f)}, {', '.join(options)})"
        elif kind == "PARQUET":
            source = f"read_parquet({scripting.literal(f)})"
        elif kind == "JSON":
            source = f"(select to_json(j) from read_json_auto({scripting.literal(f)}) j)"
        else:
            raise error(f"COPY INTO: file type {kind} is not supported")
        names = ", ".join('"' + c + '"' for c in target)
        loaded = duck.execute(f"insert into {table} ({names}) select * from {source}").fetchone()[0]
        rows.append([f, "LOADED", loaded, loaded, 1, 0, None, None, None, None])
    if not rows:
        return original(cursor, "select 'Copy executed with 0 files processed.' as \"status\"")
    selects = ["select " + ", ".join(f'{scripting.literal(v)} as "{c}"' for c, v in zip(_COPY_COLUMNS, row)) for row in rows]
    return original(cursor, " union all ".join(selects))


# ---------------------------------------------------------------- tasks, pipes, alerts

_TASK = re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?task\s+(?P<exists>if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*)$")


def _create_task(cursor, original, state: State, m: re.Match):  # noqa: ANN001, ANN202
    name, rest = bare(m.group("name")), m.group("rest")
    if name in state.tasks and m.group("exists"):
        return _status(cursor, original, f"{name} already exists, statement succeeded.")
    at = find_keyword(rest, "as")
    if at < 0:
        raise error(f"task {name}: no AS <sql> found")
    options, body = rest[:at], rest[at + 2:].strip().rstrip(";")
    schedule = re.search(r"(?i)\bschedule\s*=\s*'([^']*)'", options)
    after = re.search(r"(?is)\bafter\s+(.+?)(?=\s+when\b|\s*$)", options)
    when = re.search(r"(?is)\bwhen\s+(.*)$", options)
    state.tasks[name] = {
        "name": name, "database": cursor._conn.database, "schema": cursor._conn.schema, "body": body,
        "schedule": schedule.group(1) if schedule else None, "state": "suspended",
        "after": [bare(p) for p in split_top(after.group(1))] if after else [],
        "when": when.group(1).strip() if when else None,
    }
    return _status(cursor, original, f"Task {name} successfully created.")


def run_task(cursor, state: State, name: str, root: bool = True) -> list[str]:  # noqa: ANN001
    """Run a task once, then every resumed task that declares it as a predecessor."""
    task = state.tasks.get(name)
    if task is None:
        raise error(f"Task {name} does not exist")
    ran: list[str] = []
    if not root and task["when"] and not _run(cursor, f"select {task['when']}")[1][0][0]:
        return ran
    _run(cursor, task["body"])
    ran.append(name)
    for child in list(state.tasks.values()):
        if name in child["after"] and child["state"] == "started":
            ran += run_task(cursor, state, child["name"], root=False)
    return ran


def run_due_tasks(emu, database: str, schema: str = "CI") -> list[str]:  # noqa: ANN001
    """Run every resumed task that has a schedule, as the scheduler would at least once."""
    conn = emu._fs.connect(database=database, schema=schema)
    try:
        state, cursor, ran = state_of(conn), conn.cursor(), []
        for task in list(state.tasks.values()):
            if task["state"] == "started" and task["schedule"] and not task["after"]:
                if task["when"] and not _run(cursor, f"select {task['when']}")[1][0][0]:
                    continue
                ran += run_task(cursor, state, task["name"])
        return ran
    finally:
        conn.close()


# ---------------------------------------------------------------- policies

_POLICY = re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?(?P<kind>masking|row\s+access)\s+policy\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")\s+as\s*\(")
_ALTER_POLICY = re.compile(_LEAD + r"alter\s+(?:table|view|materialized\s+view|dynamic\s+table)\s+(?:if\s+exists\s+)?(?P<table>" + _NAME + r")\s+(?P<rest>.*(?:masking\s+policy|row\s+access\s+polic).*)$")


def policy_function(name: str) -> str:
    return "_policy_" + bare(name).lower()


def _create_policy(cursor, original, state: State, m: re.Match, sql: str):  # noqa: ANN001, ANN202
    close = _close_paren(sql, m.end() - 1)
    arrow = sql.find("->", close)
    if arrow < 0:
        raise error("policy body not found (expected `-> <expression>`)")
    args = []
    for part in split_top(sql[m.end():close]):
        words = part.split(None, 1)
        args.append((_unquote(words[0]), words[1] if len(words) > 1 else "VARCHAR"))
    defn = {"kind": "function", "name": policy_function(m.group("name")), "args": args, "body": sql[arrow + 2:].strip().rstrip(";"),
            "table": False, "returns": "VARCHAR", "language": "sql", "handler": None, "external": False}
    _define_function(cursor, state, defn)
    return _status(cursor, original, f"Policy {bare(m.group('name'))} successfully created.")


def _alter_policy(cursor, original, state: State, m: re.Match):  # noqa: ANN001, ANN202
    table, rest = qualified(cursor, m.group("table")), m.group("rest")
    for col, policy, using in re.findall(r"(?is)(?:column\s+)?(" + _NAME + r")\s+set\s+masking\s+policy\s+(" + _NAME + r")(?:\s+using\s*\(([^)]*)\))?", rest):
        extra = [bare(c) for c in split_top(using)][1:] if using else []
        state.masks.setdefault(table, {})[bare(col)] = (policy_function(policy), extra)
    for col in re.findall(r"(?is)(?:column\s+)?(" + _NAME + r")\s+unset\s+masking\s+policy", rest):
        state.masks.get(table, {}).pop(bare(col), None)
    if re.search(r"(?i)drop\s+all\s+row\s+access\s+policies", rest):
        state.row_policies.pop(table, None)
    for policy in re.findall(r"(?is)drop\s+row\s+access\s+policy\s+(" + _NAME + r")", rest):
        state.row_policies[table] = [p for p in state.row_policies.get(table, []) if p[0] != policy_function(policy)]
    for policy, cols in re.findall(r"(?is)add\s+row\s+access\s+policy\s+(" + _NAME + r")\s+on\s*\(([^)]*)\)", rest):
        state.row_policies.setdefault(table, []).append((policy_function(policy), [bare(c) for c in split_top(cols)]))
    return _status(cursor, original, "Statement executed successfully.")


def apply_policies(tree: exp.Expression, cursor) -> exp.Expression:  # noqa: ANN001
    """Wherever a protected table is read, read it through its masking and row access policies."""
    state = cursor._conn.results_cache.get(_KEY)
    if not state or not (state.masks or state.row_policies):
        return tree
    conn = cursor._conn
    for table in list(tree.find_all(exp.Table)):
        if not isinstance(table.this, exp.Identifier) or not (isinstance(table.parent, (exp.From, exp.Join)) or table.arg_key == "using"):
            continue
        fq = ((table.catalog or conn.database or "").upper(), (table.db or conn.schema or "").upper(),
              table.name if table.this.quoted else table.name.upper())
        masks, rows = state.masks.get(fq, {}), state.row_policies.get(fq, [])
        if not masks and not rows:
            continue
        quote = lambda c: '"' + c + '"'  # noqa: E731
        replaced = ", ".join(f"{fn}({', '.join(quote(c) for c in [col, *extra])}) as {quote(col)}" for col, (fn, extra) in masks.items())
        where = " and ".join(f"{fn}({', '.join(quote(c) for c in cols)})" for fn, cols in rows)
        select = sqlglot.parse_one(
            f"select *{' replace (' + replaced + ')' if replaced else ''} from {_sql_name(fq)}{' where ' + where if where else ''}",
            read="snowflake")
        alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(table.name, quoted=table.this.quoted))
        table.replace(exp.Subquery(this=select, alias=alias))
    return tree


# ---------------------------------------------------------------- dispatch

_SIMPLE = [  # (pattern, handler name)
    (re.compile(_LEAD + r"execute\s+task\s+(?P<name>" + _NAME + r")"), "execute_task"),
    (re.compile(_LEAD + r"alter\s+task\s+(?:if\s+exists\s+)?(?P<name>" + _NAME + r")\s+(?P<rest>.*)$"), "alter_task"),
    (re.compile(_LEAD + r"drop\s+(?P<kind>task|stream|pipe|alert|function|procedure|masking\s+policy|row\s+access\s+policy)\s+(?:if\s+exists\s+)?(?P<name>" + _NAME + r")"), "drop"),
    (re.compile(_LEAD + r"show\s+(?:terse\s+)?(?P<kind>tasks|streams|pipes|alerts|user\s+functions|procedures|masking\s+policies|row\s+access\s+policies)\b"), "show"),
    (re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?pipe\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*)$"), "create_pipe"),
    (re.compile(_LEAD + r"alter\s+pipe\s+(?:if\s+exists\s+)?(?P<name>" + _NAME + r")\s+refresh\b"), "refresh_pipe"),
    (re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?alert\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*)$"), "create_alert"),
    (re.compile(_LEAD + r"execute\s+alert\s+(?P<name>" + _NAME + r")"), "execute_alert"),
    (re.compile(_LEAD + r"alter\s+(?:stream|alert|pipe|function|procedure|masking\s+policy|row\s+access\s+policy)\s"), "nothing"),
    (re.compile(_LEAD + r"alter\s+table\s+(?:if\s+exists\s+)?(?P<a>" + _NAME + r")\s+swap\s+with\s+(?P<b>" + _NAME + r")"), "swap"),
    (re.compile(_LEAD + r"use\s+role\s+(?P<name>" + _NAME + r")"), "use_role"),
    (re.compile(_LEAD + r"create\s+(?:or\s+replace\s+)?external\s+table\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")\s*(?P<rest>.*)$"), "external_table"),
    (re.compile(_LEAD + r"alter\s+external\s+table\s"), "nothing"),
    (re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?(?:transient\s+)?schema\s+(?:if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")\s+clone\s+(?P<source>" + _NAME + r")"), "clone_schema"),
    (re.compile(_LEAD + r"show\s+(?:terse\s+)?(?P<kind>warehouses|roles|users|shares|integrations|(?:api|security|storage|notification|external\s+access|catalog)\s+integrations|"
                        r"network\s+policies|network\s+rules|resource\s+monitors|tags|secrets|dynamic\s+tables|materialized\s+views|external\s+tables|"
                        r"event\s+tables|iceberg\s+tables|managed\s+accounts|replication\s+\w+|failover\s+groups|password\s+policies|session\s+policies|"
                        r"packages\s+policies|external\s+functions|compute\s+pools|services|image\s+repositories|streamlits|notebooks|models|datasets|"
                        r"git\s+repositories|cortex\s+search\s+services|semantic\s+views|grants)\b"), "show_nothing"),
    (re.compile(_LEAD + r"execute\s+immediate\s+(?P<rest>.*)$"), "execute_immediate"),
    (re.compile(_LEAD + r"(?P<rest>(?:declare\b|begin\s+(?!transaction\b|work\b|name\b)\S).*\bend\b[\s;]*)$"), "block"),
]


_TRAILING_COMMENT = re.compile(r"(?s)(?:\s*;)?\s*/\*(?:(?!\*/).)*\*/\s*;?\s*$")
_LAST_QUERY = re.compile(r"(?i)\blast_query_id\(\s*(?:-1)?\s*\)")


_HAS_DATA = re.compile(r"(?i)\bsystem\$stream_has_data\(\s*'([^']+)'\s*\)")


def session_functions(cursor, command: str) -> str:  # noqa: ANN001
    """LAST_QUERY_ID() is the statement this session ran before the current one;
    SYSTEM$STREAM_HAS_DATA('s') asks whether the stream has rows."""
    lowered = command.lower()
    if "last_query_id" in lowered:
        command = _LAST_QUERY.sub("'" + str(getattr(cursor._conn, "_fauxflake_last_query", "") or "") + "'", command)
    if "system$stream_has_data" in lowered:
        command = _HAS_DATA.sub(r"(select count(*) > 0 from \1)", command)
    return command


def result_scans(tree: exp.Expression, cursor) -> exp.Expression:  # noqa: ANN001
    """`TABLE(RESULT_SCAN('<query id>'))` is an ordinary table: the rows that statement returned."""
    for node in list(tree.find_all(exp.TableFromRows)):
        call = node.this
        if not (isinstance(call, exp.Anonymous) and call.name.upper() == "RESULT_SCAN" and call.expressions
                and isinstance(call.expressions[0], exp.Literal)):
            continue
        cached = cursor._conn.results_cache.get(call.expressions[0].name)
        if cached is None:
            raise ProgrammingError(msg=f"Statement {call.expressions[0].name} not found", errno=709, sqlstate="02000")
        name = "__fx_result_" + re.sub(r"\W", "", call.expressions[0].name)
        cursor._duck_conn.register(name, cached[0])
        node.replace(exp.Table(this=exp.to_identifier(name, quoted=True), alias=node.args.get("alias")))
    return tree


def remember(cursor) -> None:  # noqa: ANN001
    cursor._conn._fauxflake_last_query = cursor.sfqid


def intercept(cursor, command: str, original):  # noqa: ANN001, ANN201
    """Handle a statement about one of the objects above. Returns None for everything else."""
    head = re.match(_LEAD + r"(\w+)", command)
    command = _TRAILING_COMMENT.sub("", command)  # dbt appends its query comment after the statement
    if not head or head.group(1).lower() not in ("create", "alter", "drop", "show", "call", "execute", "declare", "begin", "use", "with", "copy"):
        return None
    state = state_of(cursor._conn)
    with state.lock:
        defn = _parse_callable(command)
        if defn:
            if defn["body"] is None and not defn["external"]:
                raise error(f"{defn['kind']} {defn['name']}: no body found")
            if defn["kind"] == "procedure":
                state.procedures[defn["name"]] = defn
            else:
                _define_function(cursor, state, defn)
            return _status(cursor, original, f"{defn['kind'].capitalize()} {defn['name']} successfully created.")
        m = _ANONYMOUS.match(command)
        if m:
            proc = _parse_callable(f"create procedure {m.group('name')}{m.group('rest').rstrip().rstrip(';')}")
            if proc is None or proc["body"] is None:
                raise error("anonymous procedure: no body found")
            args = m.group("args").strip()
            values = list(_run(cursor, f"select {args}")[1][0]) if args else []
            return _result(cursor, original, call_procedure(cursor, proc, values), proc["name"])
        m = _CALL.match(command)
        if m:
            return _call(cursor, original, state, m)
        m = _COPY.match(command)
        if m:
            return _copy_into(cursor, original, state, m)
        m = _STREAM.match(command)
        if m:
            return _create_stream(cursor, original, state, m)
        m = _TASK.match(command)
        if m:
            return _create_task(cursor, original, state, m)
        m = _POLICY.match(command)
        if m:
            return _create_policy(cursor, original, state, m, command)
        m = _ALTER_POLICY.match(command)
        if m:
            return _alter_policy(cursor, original, state, m)
        for pattern, handler in _SIMPLE:
            m = pattern.match(command)
            if m:
                return globals()["_" + handler](cursor, original, state, m)
    return None


def set_role(duck, role: str) -> None:  # noqa: ANN001
    duck.execute("update _fs_global.main._fx_session set role = ?", [role.upper()])


def _use_role(cursor, original, state, m):  # noqa: ANN001, ANN202
    set_role(cursor._duck_conn, bare(m.group("name")))
    return _status(cursor, original, "Statement executed successfully.")


def _show_nothing(cursor, original, state, m):  # noqa: ANN001, ANN202
    """Account-level objects (warehouses, roles, integrations...) have nothing behind them locally: none exist."""
    columns = ["created_on", "name", "database_name", "schema_name", "kind", "owner", "comment"]
    return original(cursor, "select " + ", ".join(f'null::varchar as "{c}"' for c in columns) + " where false")


def _external_table(cursor, original, state, m):  # noqa: ANN001, ANN202
    """An external table that the spec already generates keeps its rows. Otherwise it starts empty,
    with VALUE, METADATA$FILENAME and the columns the statement declares."""
    fq = qualified(cursor, m.group("name"))
    duck = cursor._duck_conn
    exists = duck.execute("select 1 from information_schema.tables where table_catalog = ? and table_schema = ? and table_name = ?",
                          list(fq)).fetchone()
    if not exists:
        rest = m.group("rest")
        declared = []
        if rest.startswith("("):
            for part in split_top(rest[1:_close_paren(rest, 0)]):
                words = part.split(None, 2)
                if len(words) >= 2:
                    declared.append(f'"{_unquote(words[0])}" {duck_type(re.split(r"(?i)\s+as\b", " ".join(words[1:]))[0])}')
        cols = ['"VALUE" JSON', '"METADATA$FILENAME" VARCHAR', *declared]
        duck.execute(f"create table {_sql_name(fq)} ({', '.join(cols)})")
    return _status(cursor, original, f"Table {fq[2]} successfully created.")


def _clone_schema(cursor, original, state, m):  # noqa: ANN001, ANN202
    """`CREATE SCHEMA b CLONE a`: every table copied, every view recreated."""
    source, target = qualified(cursor, m.group("source") + ".X")[:2], qualified(cursor, m.group("name") + ".X")[:2]
    duck = cursor._duck_conn
    quote = lambda *parts: ".".join('"' + p + '"' for p in parts)  # noqa: E731
    if m.group("replace"):
        duck.execute(f"drop schema if exists {quote(*target)} cascade")
    duck.execute(f"create schema if not exists {quote(*target)}")
    for (name,) in duck.execute("select table_name from duckdb_tables() where database_name = ? and schema_name = ?", list(source)).fetchall():
        duck.execute(f"create table {quote(*target, name)} as select * from {quote(*source, name)}")
    views = duck.execute("select view_name, sql from duckdb_views() where database_name = ? and schema_name = ? and not internal", list(source)).fetchall()
    for name, sql in views:
        body = re.split(r"(?is)\bas\b", sql, maxsplit=1)
        if len(body) == 2:
            try:
                duck.execute(f"create view {quote(*target, name)} as {body[1]}")
            except Exception:  # noqa: BLE001, S110 - a view over something outside the schema
                pass
    return _status(cursor, original, f"Schema {target[1]} successfully created.")


def _nothing(cursor, original, state, m):  # noqa: ANN001, ANN202
    return _status(cursor, original, "Statement executed successfully.")


def _execute_task(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    ran = run_task(cursor, state, bare(m.group("name")))
    return _status(cursor, original, f"Task executed: {', '.join(ran)}.")


def _alter_task(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    task, rest = state.tasks.get(bare(m.group("name"))), m.group("rest").strip()
    if task is None:
        raise error(f"Task {bare(m.group('name'))} does not exist")
    word = rest.split(None, 1)[0].lower().rstrip(";")
    if word in ("resume", "suspend"):
        task["state"] = "started" if word == "resume" else "suspended"
    elif re.match(r"(?i)modify\s+as\b", rest):
        task["body"] = re.sub(r"(?is)^modify\s+as\s+", "", rest).rstrip(";")
    elif re.match(r"(?i)modify\s+when\b", rest):
        task["when"] = re.sub(r"(?is)^modify\s+when\s+", "", rest).rstrip(";")
    elif re.match(r"(?i)add\s+after\b", rest):
        task["after"] += [bare(p) for p in split_top(re.sub(r"(?is)^add\s+after\s+", "", rest).rstrip(";"))]
    elif re.match(r"(?i)remove\s+after\b", rest):
        gone = {bare(p) for p in split_top(re.sub(r"(?is)^remove\s+after\s+", "", rest).rstrip(";"))}
        task["after"] = [a for a in task["after"] if a not in gone]
    else:
        schedule = re.search(r"(?i)\bschedule\s*=\s*'([^']*)'", rest)
        if schedule:
            task["schedule"] = schedule.group(1)
    return _status(cursor, original, "Statement executed successfully.")


def _drop(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    kind, name = re.sub(r"\s+", " ", m.group("kind").lower()), bare(m.group("name").split("(")[0])
    if kind == "stream":
        fq = qualified(cursor, m.group("name"))
        stream = state.streams.pop(fq, None)
        if stream:
            cursor._duck_conn.execute(f"drop view if exists {_sql_name(fq)}")
            cursor._duck_conn.execute(f"drop table if exists {stream['base']}")
    elif kind == "function":
        state.functions.pop(name.lower(), None)
        semantics.UDFS.pop(name.lower(), None)
        semantics.UDTFS.pop(name.lower(), None)
    else:
        {"task": state.tasks, "pipe": state.pipes, "alert": state.alerts, "procedure": state.procedures}.get(kind, {}).pop(name, None)
    return _status(cursor, original, f"{name} successfully dropped.")


def _show(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    if m is not None:
        kind = re.sub(r"\s+", " ", m.group("kind").lower())
        if kind == "tasks":
            columns = ["created_on", "name", "id", "database_name", "schema_name", "owner", "comment", "warehouse", "schedule",
                       "predecessors", "state", "definition", "condition"]
            rows = [[None, t["name"], t["name"], t["database"], t["schema"], "SYSADMIN", "", "LOCAL", t["schedule"],
                     json.dumps(t["after"]), t["state"], t["body"], t["when"]] for t in state.tasks.values()]
        elif kind == "streams":
            columns = ["created_on", "name", "database_name", "schema_name", "owner", "comment", "table_name", "source_type",
                       "base_tables", "type", "stale", "mode"]
            rows = [[None, fq[2], fq[0], fq[1], "SYSADMIN", "", s["source"].replace('"', ""), "Table", s["source"].replace('"', ""),
                     "DELTA", "false", s["mode"]] for fq, s in state.streams.items()]
        elif kind == "user functions":
            columns = ["created_on", "name", "schema_name", "is_builtin", "is_aggregate", "min_num_arguments", "max_num_arguments",
                       "arguments", "description", "catalog_name", "is_table_function", "language"]
            rows = [[None, d["name"], cursor._conn.schema, "N", "N", n, n, f"{d['name']}({', '.join(k for _, k in d['args'])})",
                     "user-defined function", cursor._conn.database, "Y" if d["table"] else "N", d["language"].upper()]
                    for overloads in state.functions.values() for n, d in overloads.items() if not d["name"].startswith("_policy_")]
        elif kind == "procedures":
            columns = ["created_on", "name", "schema_name", "is_builtin", "arguments", "description", "catalog_name"]
            rows = [[None, p["name"], cursor._conn.schema, "N", f"{p['name']}({', '.join(k for _, k in p['args'])})",
                     "user-defined procedure", cursor._conn.database] for p in state.procedures.values()]
        else:
            columns = ["created_on", "name", "database_name", "schema_name", "kind", "owner"]
            source = state.pipes if kind == "pipes" else state.alerts if kind == "alerts" else \
                {d["name"][len("_policy_"):].upper(): 1 for o in state.functions.values() for d in o.values() if d["name"].startswith("_policy_")}
            rows = [[None, name, cursor._conn.database, cursor._conn.schema, kind.rstrip("s").upper(), "SYSADMIN"] for name in source]
    if not rows:
        return original(cursor, "select " + ", ".join(f'null::varchar as "{c}"' for c in columns) + " where false")
    selects = ["select " + ", ".join(f'{scripting.literal(v)} as "{c}"' for c, v in zip(columns, row)) for row in rows]
    return original(cursor, " union all ".join(selects))


def _create_pipe(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    rest = m.group("rest")
    at = find_keyword(rest, "as")
    state.pipes[bare(m.group("name"))] = rest[at + 2:].strip().rstrip(";") if at >= 0 else ""
    return _status(cursor, original, f"Pipe {bare(m.group('name'))} successfully created.")


def _refresh_pipe(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    body = state.pipes.get(bare(m.group("name")))
    if body is None:
        raise error(f"Pipe {bare(m.group('name'))} does not exist")
    return original(cursor, body)


def _create_alert(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    rest = m.group("rest")
    start = re.search(r"(?is)\bif\s*\(\s*exists\s*\(", rest)
    if not start:
        raise error("alert: expected IF (EXISTS (<query>)) THEN <action>")
    open_at = rest.index("(", start.start())
    close = _close_paren(rest, open_at)
    condition = rest[open_at + 1:close].strip()
    action = re.sub(r"(?is)^\s*then\s+", "", rest[close + 1:]).strip().rstrip(";")
    state.alerts[bare(m.group("name"))] = {"condition": condition, "action": action}
    return _status(cursor, original, f"Alert {bare(m.group('name'))} successfully created.")


def _execute_alert(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    alert = state.alerts.get(bare(m.group("name")))
    if alert is None:
        raise error(f"Alert {bare(m.group('name'))} does not exist")
    fired = bool(_run(cursor, f"select {alert['condition']}")[1][0][0])
    if fired:
        _run(cursor, alert["action"])
    return _status(cursor, original, "Alert executed: condition was " + ("true, action ran." if fired else "false."))


def _swap(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    a, b = _sql_name(qualified(cursor, m.group("a"))), qualified(cursor, m.group("b"))
    holder = '"__FX_SWAP"'
    duck = cursor._duck_conn
    duck.execute(f"alter table {a} rename to {holder}")
    duck.execute(f"alter table {_sql_name(b)} rename to \"{qualified(cursor, m.group('a'))[2]}\"")
    duck.execute(f"alter table {_sql_name((b[0], b[1], '__FX_SWAP'))} rename to \"{b[2]}\"")
    return _status(cursor, original, "Statement executed successfully.")


def _execute_immediate(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    rest = m.group("rest").strip().rstrip(";").strip()
    using = find_keyword(rest, "using")
    if using > 0:
        rest = rest[:using].strip()
    if rest.startswith("$$") and rest.endswith("$$"):
        text = rest[2:-2]
    else:
        text = _run(cursor, f"select {rest}")[1][0][0]
    return _text(cursor, original, text)


def _block(cursor, original, state: State, m):  # noqa: ANN001, ANN202
    return _result(cursor, original, scripting.Script(lambda sql: _run(cursor, sql)).run(m.group("rest"), {}), "anonymous block")


def _text(cursor, original, text: str):  # noqa: ANN001, ANN202
    if re.match(_LEAD + r"(declare\b|begin\s+(?!transaction\b|work\b|name\b)\S)", text):
        return _result(cursor, original, scripting.Script(lambda sql: _run(cursor, sql)).run(text, {}), "anonymous block")
    cursor.execute(text)
    return cursor


def close(emu) -> None:  # noqa: ANN001
    """Unregister Python functions so DuckDB shuts down cleanly."""
    state = emu._fs.results_cache.get(_KEY)
    for name in (state.native if state else []):
        try:
            emu.duck.remove_function(name)
        except Exception:  # noqa: BLE001, S110
            pass
