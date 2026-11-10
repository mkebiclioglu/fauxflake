"""Procedure bodies: Snowflake Scripting, JavaScript and Python.

`Script` interprets Snowflake Scripting: DECLARE, BEGIN/END, LET, assignment, IF, CASE, FOR
(over a range, a cursor or a RESULTSET), WHILE, REPEAT, LOOP, BREAK/CONTINUE, RETURN,
EXECUTE IMMEDIATE, cursors with OPEN/FETCH/CLOSE, SELECT ... INTO and EXCEPTION handlers.
Every SQL statement inside goes back through the emulator, so it means what it means anywhere else.

`JavaScript` runs JavaScript functions and procedures in Duktape (through dukpy), including the
`snowflake.execute` / `snowflake.createStatement` API. `Session` is the small part of a Snowpark
session Python procedures and models use most: `sql`, `table`, `call`, `collect`, `to_pandas`,
`write.save_as_table`.
"""

from __future__ import annotations

import datetime
import decimal
import json
import re
import threading
from typing import Callable

Run = Callable[[str], tuple[list[str], list[tuple]]]
_LEAD = r"(?is)^(?:\s|--[^\n]*\n|//[^\n]*\n|/\*.*?\*/)*"


class ScriptError(Exception):
    pass


class Table:
    """What `RETURN TABLE(resultset)` hands back: the query to run for the caller."""

    def __init__(self, query: str) -> None:
        self.query = query


def literal(value) -> str:  # noqa: ANN001
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return f"'{value}'::timestamp"
    if isinstance(value, datetime.date):
        return f"'{value}'::date"
    if isinstance(value, (dict, list, tuple)):
        return "parse_json('" + json.dumps(value, default=str).replace("\\", "\\\\").replace("'", "''") + "')"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def split_statements(text: str) -> list[str]:
    """Split at semicolons outside quotes, `$$` bodies, comments and brackets. Comments are dropped."""
    out, buf, depth, i, n = [], [], 0, 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            j = i + 1
            while j < n and (text[j] != "'" or text[j + 1:j + 2] == "'"):
                j += 2 if text[j] in "'\\" else 1
            buf.append(text[i:j + 1])
            i = j + 1
            continue
        if ch == '"':
            j = text.find('"', i + 1)
            j = n - 1 if j < 0 else j
            buf.append(text[i:j + 1])
            i = j + 1
            continue
        if text.startswith("$$", i):
            j = text.find("$$", i + 2)
            j = n - 2 if j < 0 else j
            buf.append(text[i:j + 2])
            i = j + 2
            continue
        if text.startswith("--", i) or (text.startswith("//", i) and (i == 0 or text[i - 1] != ":")):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if text.startswith("/*", i):
            j = text.find("*/", i)
            i = n if j < 0 else j + 2
            buf.append(" ")
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == ";" and depth <= 0:
            out.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


class _Return(Exception):
    def __init__(self, value) -> None:  # noqa: ANN001
        self.value = value


class _Break(Exception):
    pass


class _Continue(Exception):
    pass


def _keyword(text: str, keyword: str) -> int:
    from .objects import find_keyword

    return find_keyword(text, keyword)


class _Parser:
    """Turns the statement list into nested tuples the interpreter walks."""

    def __init__(self, text: str) -> None:
        self.chunks = split_statements(text)
        self.i = 0

    def peek(self) -> str | None:
        return self.chunks[self.i] if self.i < len(self.chunks) else None

    def push(self, rest: str) -> None:
        if rest.strip():
            self.chunks.insert(self.i, rest.strip())

    def at(self, *heads: str) -> re.Match | None:
        chunk = self.peek()
        if chunk is None:
            return None
        for head in heads:
            m = re.match(r"(?is)" + head, chunk)
            if m:
                return m
        return None

    def block(self, *until: str) -> list:
        body = []
        while self.peek() is not None and not self.at(*until):
            body.append(self.statement())
        if self.peek() is None:
            raise ScriptError(f"expected {' or '.join(until)} before the end of the block")
        return body

    def expect(self, head: str) -> None:
        if not self.at(head):
            raise ScriptError(f"expected {head!r}, found {self.peek()!r}")
        self.i += 1

    def program(self) -> list:
        body = []
        while self.peek() is not None:
            body.append(self.statement())
        return body

    def statement(self):  # noqa: ANN201, C901, PLR0911, PLR0912, PLR0915
        chunk = self.chunks[self.i]
        self.i += 1
        m = re.match(r"(?is)declare\b(.*)", chunk)
        if m:
            self.push(m.group(1))
            decls = []
            while self.peek() is not None and not self.at(r"begin\b"):
                decls.append(self.declaration(self.chunks[self.i]))
                self.i += 1
            if self.peek() is None:
                raise ScriptError("DECLARE without BEGIN")
            block = self.statement()
            return ("begin", decls + block[1], block[2])
        m = re.match(r"(?is)begin\b(?!\s*(?:transaction|work|name)\b)(.*)", chunk)
        if m:
            self.push(m.group(1))
            body = self.block(r"end\s*(\w+\s*)?$", r"exception\b")
            handlers = []
            m = self.at(r"exception\b(.*)")
            if m:
                self.i += 1
                self.push(m.group(1))
                while True:
                    m = self.at(r"when\b(.*)")
                    if not m:
                        break
                    self.i += 1
                    then = _keyword(m.group(1), "then")
                    self.push(m.group(1)[then + 4:])
                    handlers.append((m.group(1)[:then].strip().upper(), self.block(r"when\b", r"end\s*(\w+\s*)?$")))
            self.expect(r"end\s*(\w+\s*)?$")
            return ("begin", body, handlers)
        m = re.match(r"(?is)if\b(.*)", chunk)
        if m:
            branches, otherwise = [], []
            rest = m.group(1)
            while True:
                then = _keyword(rest, "then")
                if then < 0:
                    raise ScriptError("IF without THEN")
                self.push(rest[then + 4:])
                branches.append((rest[:then].strip(), self.block(r"elseif\b", r"else\b", r"end\s+if\b")))
                m = self.at(r"elseif\b(.*)")
                if m:
                    self.i += 1
                    rest = m.group(1)
                    continue
                m = self.at(r"else\b(.*)")
                if m:
                    self.i += 1
                    self.push(m.group(1))
                    otherwise = self.block(r"end\s+if\b")
                break
            self.expect(r"end\s+if\b")
            return ("if", branches, otherwise)
        m = re.match(r"(?is)case\b(.*)", chunk)
        if m:
            when = _keyword(m.group(1), "when")
            operand = m.group(1)[:when].strip()
            self.push(m.group(1)[when:])
            branches, otherwise = [], []
            while True:
                m = self.at(r"when\b(.*)")
                if not m:
                    break
                self.i += 1
                then = _keyword(m.group(1), "then")
                cond = m.group(1)[:then].strip()
                self.push(m.group(1)[then + 4:])
                branches.append((f"({operand}) = ({cond})" if operand else cond, self.block(r"when\b", r"else\b", r"end(\s+case)?\s*$")))
            m = self.at(r"else\b(.*)")
            if m:
                self.i += 1
                self.push(m.group(1))
                otherwise = self.block(r"end(\s+case)?\s*$")
            self.expect(r"end(\s+case)?\s*$")
            return ("if", branches, otherwise)
        m = re.match(r"(?is)for\s+(\w+)\s+in\s+(.*)", chunk)
        if m:
            do = _keyword(m.group(2), "do")
            if do < 0:
                do = _keyword(m.group(2), "loop")
                skip = 4
            else:
                skip = 2
            source = m.group(2)[:do].strip()
            self.push(m.group(2)[do + skip:])
            body = self.block(r"end\s+(for|loop)\b")
            self.i += 1
            reverse = re.match(r"(?is)reverse\s+(.*)", source)
            if reverse:
                source = reverse.group(1)
            to = _keyword(source, "to")
            if to >= 0:
                return ("for_range", m.group(1).upper(), source[:to].strip(), source[to + 2:].strip(), bool(reverse), body)
            return ("for_each", m.group(1).upper(), source.upper(), body)
        m = re.match(r"(?is)while\b(.*)", chunk)
        if m:
            do = _keyword(m.group(1), "do")
            skip = 2
            if do < 0:
                do, skip = _keyword(m.group(1), "loop"), 4
            self.push(m.group(1)[do + skip:])
            body = self.block(r"end\s+(while|loop)\b")
            self.i += 1
            return ("while", m.group(1)[:do].strip(), body)
        m = re.match(r"(?is)repeat\b(.*)", chunk)
        if m:
            self.push(m.group(1))
            body = self.block(r"until\b")
            cond = re.match(r"(?is)until\b(.*?)(\bend\s+repeat\b\s*)?$", self.chunks[self.i]).group(1)
            self.i += 1
            if self.at(r"end\s+repeat\b"):
                self.i += 1
            return ("repeat", body, cond.strip())
        m = re.match(r"(?is)loop\b(.*)", chunk)
        if m:
            self.push(m.group(1))
            body = self.block(r"end\s+loop\b")
            self.i += 1
            return ("while", "true", body)
        if re.match(r"(?is)(break|exit)\b", chunk):
            return ("break",)
        if re.match(r"(?is)(continue|iterate)\b", chunk):
            return ("continue",)
        m = re.match(r"(?is)let\s+(.*)", chunk)
        if m:
            return self.declaration(m.group(1))
        m = re.match(r"(?is)(\w+)\s*:=\s*(.*)", chunk)
        if m:
            return ("let", m.group(1).upper(), m.group(2))
        m = re.match(r"(?is)return\b\s*(.*)", chunk)
        if m:
            return ("return", m.group(1))
        m = re.match(r"(?is)execute\s+immediate\s+(.*)", chunk)
        if m:
            using = _keyword(m.group(1), "using")
            return ("execute", m.group(1)[:using].strip() if using >= 0 else m.group(1), m.group(1)[using + 5:].strip() if using >= 0 else "")
        m = re.match(r"(?is)raise\b\s*(\w*)", chunk)
        if m:
            return ("raise", m.group(1))
        if re.match(r"(?is)null\s*$", chunk):
            return ("null",)
        m = re.match(r"(?is)(open|close)\s+(\w+)", chunk)
        if m:
            return (m.group(1).lower(), m.group(2).upper())
        m = re.match(r"(?is)fetch\s+(\w+)\s+into\s+(.*)", chunk)
        if m:
            return ("fetch", m.group(1).upper(), [v.strip().lstrip(":").upper() for v in m.group(2).split(",")])
        return ("sql", chunk)

    def declaration(self, text: str):  # noqa: ANN201
        m = re.match(r"(?is)(\w+)\s+cursor\s+for\s+(.*)", text)
        if m:
            return ("cursor", m.group(1).upper(), m.group(2))
        m = re.match(r"(?is)(\w+)\s+resultset(?:\s*(?::=|\bdefault\b)\s*(.*))?$", text)
        if m:
            return ("resultset", m.group(1).upper(), m.group(2))
        if re.match(r"(?is)\w+\s+exception\b", text):
            return ("null",)
        m = re.match(r"(?is)(\w+)\s*(?:[\w ]+?(?:\([^)]*\))?)??\s*(?::=|\bdefault\b)\s*(.*)$", text)
        if m:
            return ("let", m.group(1).upper(), m.group(2))
        m = re.match(r"(?is)(\w+)\b", text)
        return ("let", m.group(1).upper(), None)


_QUOTED = re.compile(r"""('(?:[^'\\]|''|\\.)*'|"(?:[^"]|"")*"|\$\$.*?\$\$)""", re.DOTALL)
_BUILTIN = re.compile(r"(?i)(?<![\w$.:])(sqlerrm|sqlcode|sqlstate|sqlrowcount|sqlfound|sqlnotfound)(?![\w$(])")
_INTO = re.compile(r"(?is)\binto\s+(:?\w+(?:\s*,\s*:?\w+)*)\s+(?=from\b)")


class Script:
    def __init__(self, run: Run) -> None:
        self.run_sql = run
        self.vars: dict[str, object] = {"SQLROWCOUNT": None, "SQLFOUND": None, "SQLNOTFOUND": None}
        self.cursors: dict[str, dict] = {}

    def run(self, text: str, variables: dict):  # noqa: ANN201
        self.vars.update({k.upper(): v for k, v in variables.items()})
        try:
            self.execute(_Parser(text).program())
        except _Return as r:
            return r.value
        return None

    # --- values

    def substitute(self, text: str, bare: bool) -> str:
        """Put variable values into SQL: `:name` everywhere, bare names too inside expressions."""
        names = [n for n in self.vars if self.vars[n] is not None or n not in ("SQLROWCOUNT", "SQLFOUND", "SQLNOTFOUND")]
        if not names and ":" not in text:
            return text

        def value(name: str, attr: str | None) -> str:
            v = self.vars[name]
            if attr is not None and isinstance(v, dict):
                return literal(v.get(attr.upper()))
            return literal(v) + (f".{attr}" if attr else "")

        def bound(m: re.Match) -> str:
            name = m.group(1).upper()
            return value(name, m.group(2)) if name in self.vars else m.group(0)

        alternation = "|".join(sorted((re.escape(n) for n in names), key=len, reverse=True))
        word = re.compile(r"(?i)(?<![\w$.:])(" + alternation + r")(?![\w$(])(?:\.(\w+))?") if bare and names else None
        rows = "|".join(re.escape(n) for n in names if isinstance(self.vars[n], dict))
        records = re.compile(r"(?i)(?<![\w$.:])(" + rows + r")\.(\w+)") if rows else None
        parts = _QUOTED.split(text)
        for k in range(0, len(parts), 2):
            part = re.sub(r"(?<![:\w]):(\w+)(?:\.(\w+))?", bound, parts[k])
            if records is not None:  # rec.col from a FOR loop over a cursor
                part = records.sub(lambda m: value(m.group(1).upper(), m.group(2)), part)
            part = _BUILTIN.sub(lambda m: value(m.group(1).upper(), None) if m.group(1).upper() in self.vars else m.group(0), part)
            if word is not None:
                part = word.sub(lambda m: value(m.group(1).upper(), m.group(2)), part)
            parts[k] = part
        out = "".join(parts)
        return re.sub(r"(?i)\bidentifier\(\s*'([^']+)'\s*\)", r"\1", out)

    def value(self, expr: str):  # noqa: ANN201
        from .objects import plain

        expr = expr.strip()
        if re.fullmatch(r"-?\d+", expr):
            return int(expr)
        m = re.fullmatch(r"\(\s*(select\b.*|with\b.*)\)", expr, re.IGNORECASE | re.DOTALL)
        sql = self.substitute(m.group(1), False) if m else f"select {self.substitute(expr, True)}"
        rows = self.run_sql(sql)[1]
        return plain(rows[0][0]) if rows else None

    def truth(self, cond: str) -> bool:
        return bool(self.value(cond))

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        from .objects import plain

        columns, rows = self.run_sql(self.substitute(sql, False))
        self.vars["SQLROWCOUNT"] = len(rows)
        self.vars["SQLFOUND"], self.vars["SQLNOTFOUND"] = bool(rows), not rows
        return [c.upper() for c in columns], [tuple(plain(v) for v in row) for row in rows]

    # --- statements

    def execute(self, body: list) -> None:
        for node in body:
            self.step(node)

    def loop_body(self, body: list) -> bool:
        """Run one iteration; False when the loop should stop."""
        try:
            self.execute(body)
        except _Break:
            return False
        except _Continue:
            pass
        return True

    def step(self, node) -> None:  # noqa: ANN001, C901, PLR0912, PLR0915
        kind = node[0]
        if kind == "sql":
            sql = node[1]
            into = _INTO.search(sql)
            if into and re.match(r"(?is)\s*(select|with)\b", sql):
                columns, rows = self.query(sql[:into.start()] + sql[into.end():])
                for name, v in zip(into.group(1).split(","), rows[0] if rows else [None] * 99):
                    self.vars[name.strip().lstrip(":").upper()] = v
                return
            m = re.match(_LEAD + r"call\s+(.*?)\s+into\s+:(\w+)\s*$", sql)
            if m:
                rows = self.query("call " + m.group(1))[1]
                self.vars[m.group(2).upper()] = rows[0][0] if rows else None
                return
            self.query(sql)
        elif kind == "let":
            if node[1] in self.cursors and node[2] is not None:  # rs := (select ...)
                query = node[2].strip()
                query = query[1:-1] if query.startswith("(") and query.endswith(")") else query
                self.cursors[node[1]].update(query=self.substitute(query, False), rows=None, at=0)
                return
            self.vars[node[1]] = self.value(node[2]) if node[2] is not None else None
        elif kind in ("cursor", "resultset"):
            query = node[2]
            if query is not None:
                query = query.strip()
                if query.startswith("(") and query.endswith(")"):
                    query = query[1:-1]
                if kind == "resultset":
                    query = self.substitute(query, False)
            self.cursors[node[1]] = {"query": query, "rows": None, "at": 0}
        elif kind == "return":
            expr = node[1].strip()
            m = re.fullmatch(r"(?is)table\s*\(\s*(\w+)\s*\)", expr)
            if m and m.group(1).upper() in self.cursors:
                raise _Return(Table(self.substitute(self.cursors[m.group(1).upper()]["query"], False)))
            raise _Return(self.value(expr) if expr else None)
        elif kind == "if":
            for cond, body in node[1]:
                if self.truth(cond):
                    self.execute(body)
                    return
            self.execute(node[2])
        elif kind == "for_range":
            lo, hi = int(self.value(node[2])), int(self.value(node[3]))
            for i in (range(hi, lo - 1, -1) if node[4] else range(lo, hi + 1)):
                self.vars[node[1]] = i
                if not self.loop_body(node[5]):
                    break
        elif kind == "for_each":
            cursor = self.cursors.get(node[2])
            if cursor is None:
                raise ScriptError(f"FOR ... IN {node[2]}: no such cursor or RESULTSET")
            columns, rows = self.query(cursor["query"])
            for row in rows:
                self.vars[node[1]] = dict(zip(columns, row))
                if not self.loop_body(node[3]):
                    break
        elif kind == "while":
            for _ in range(1_000_000):
                if not self.truth(node[1]) or not self.loop_body(node[2]):
                    break
        elif kind == "repeat":
            for _ in range(1_000_000):
                if not self.loop_body(node[1]) or self.truth(node[2]):
                    break
        elif kind == "break":
            raise _Break
        elif kind == "continue":
            raise _Continue
        elif kind == "begin":
            try:
                self.execute(node[1])
            except (_Return, _Break, _Continue):
                raise
            except Exception as e:  # noqa: BLE001
                handlers = node[2]
                name = type(e).__name__.upper() if isinstance(e, ScriptError) and str(e).startswith("raised ") else "OTHER"
                chosen = next((body for when, body in handlers if when == "OTHER" or name in when or (isinstance(e, ScriptError) and str(e)[7:].upper() in when)), None)
                if chosen is None:
                    raise
                self.vars.update({"SQLERRM": str(e), "SQLCODE": getattr(e, "errno", -1), "SQLSTATE": getattr(e, "sqlstate", "P0000")})
                self.execute(chosen)
        elif kind == "execute":
            text = node[1].strip()
            sql = text[2:-2] if text.startswith("$$") and text.endswith("$$") else str(self.value(text))
            if node[2]:
                binds = [self.value(b) for b in node[2].strip("()").split(",")]
                sql = re.sub(r"\?", lambda _m: literal(binds.pop(0)) if binds else "?", sql)
                sql = re.sub(r":(\d+)", lambda m: literal(binds[int(m.group(1)) - 1]) if int(m.group(1)) <= len(binds) else m.group(0), sql)
            columns, rows = self.run_sql(sql)
            self.vars["SQLROWCOUNT"] = len(rows)
        elif kind == "raise":
            raise ScriptError(f"raised {node[1]}")
        elif kind == "open":
            cursor = self.cursors[node[1]]
            cursor["rows"], cursor["at"] = self.query(cursor["query"])[1], 0
        elif kind == "fetch":
            cursor = self.cursors[node[1]]
            if cursor["rows"] is None:
                cursor["rows"], cursor["at"] = self.query(cursor["query"])[1], 0
            row = cursor["rows"][cursor["at"]] if cursor["at"] < len(cursor["rows"]) else None
            cursor["at"] += 1
            for name, v in zip(node[2], row if row is not None else [None] * len(node[2])):
                self.vars[name] = v
            self.vars["SQLFOUND"], self.vars["SQLNOTFOUND"] = row is not None, row is None


# ---------------------------------------------------------------- JavaScript

_PRELUDE = """
var __rows = function(r) {
  var at = -1, cols = r.columns, rows = r.rows;
  var idx = function(c) { return typeof c === 'number' ? c - 1 : cols.indexOf(String(c).toUpperCase()) >= 0 ? cols.indexOf(String(c).toUpperCase()) : cols.indexOf(String(c)); };
  var rs = {
    next: function() { at += 1; if (at < rows.length) { for (var i = 0; i < cols.length; i++) { rs[cols[i]] = rows[at][i]; } return true; } return false; },
    getColumnValue: function(c) { return rows[at][idx(c)]; },
    getColumnValueAsString: function(c) { var v = rows[at][idx(c)]; return v === null ? null : String(v); },
    getColumnCount: function() { return cols.length; },
    getColumnName: function(i) { return cols[i - 1]; },
    getRowCount: function() { return rows.length; },
    getNumRowsAffected: function() { return rows.length; },
    getQueryId: function() { return 'fauxflake'; },
    getColumnSqlType: function(c) { return 'TEXT'; },
    getColumnType: function(c) { return 'string'; }
  };
  return rs;
};
var __statement = function(o) {
  var result = null;
  var st = {
    execute: function() { result = __rows(call_python('__sf_execute', o.sqlText, o.binds || [])); return result; },
    getSqlText: function() { return o.sqlText; },
    getQueryId: function() { return 'fauxflake'; },
    getColumnCount: function() { return result ? result.getColumnCount() : 0; },
    getColumnName: function(i) { return result.getColumnName(i); },
    getRowCount: function() { return result ? result.getRowCount() : 0; },
    getNumRowsAffected: function() { return result ? result.getRowCount() : 0; }
  };
  return st;
};
var snowflake = {
  execute: function(o) { return __statement(o).execute(); },
  createStatement: function(o) { return __statement(o); },
  log: function() {},
  setSpanAttribute: function() {},
  addEvent: function() {}
};
"""


class JavaScript:
    """One Duktape interpreter. Functions are defined once and called by key."""

    def __init__(self) -> None:
        import dukpy

        self._dukpy = dukpy
        self.interp = dukpy.JSInterpreter()
        self.lock = threading.Lock()

    def _load(self, source: str) -> None:
        try:
            self.interp.evaljs(source)
        except self._dukpy.JSRuntimeError as first:
            try:  # newer syntax than Duktape reads (arrow functions, let, classes): compile it down
                compiled = self._dukpy.babel_compile(source, presets=["es2015"])["code"]
                self.interp.evaljs(compiled)
            except Exception:  # noqa: BLE001
                raise ScriptError(f"JavaScript error: {first}") from first

    def define(self, key: str, params: list[str], body: str) -> str:
        with self.lock:
            self._load(f"var {key} = function({', '.join(params)}) {{\n{body}\n}};")
        return key

    def call(self, key: str, args: list):  # noqa: ANN201
        with self.lock:
            try:
                return self.interp.evaljs(
                    "(function(){ var a = dukpy.args.map(function(v){ return v === null ? undefined : v; });"
                    f" var r = {key}.apply(null, a); return r === undefined ? null : r; }})()", args=args)
            except self._dukpy.JSRuntimeError as e:
                raise ScriptError(f"JavaScript error: {e}") from e

    def procedure(self, params: list[str], body: str, args: list, run: Run):  # noqa: ANN201
        from .objects import plain

        def execute(sql: str, binds: list) -> dict:
            binds = list(binds or [])
            sql = re.sub(r"\?", lambda _m: literal(binds.pop(0)) if binds else "?", sql)
            sql = re.sub(r":(\d+)", lambda m: literal(binds[int(m.group(1)) - 1]) if 0 < int(m.group(1)) <= len(binds) else m.group(0), sql)
            columns, rows = run(sql)
            return {"columns": columns, "rows": [[plain(v) for v in row] for row in rows]}

        with self.lock:
            self.interp.export_function("__sf_execute", execute)
            self._load(_PRELUDE)
            self._load(f"var __proc = function({', '.join(params)}) {{\n{body}\n}};")
            try:
                return self.interp.evaljs(
                    "(function(){ var a = dukpy.args.map(function(v){ return v === null ? undefined : v; });"
                    " var r = __proc.apply(null, a); return r === undefined ? null : r; })()", args=args)
            except self._dukpy.JSRuntimeError as e:
                raise ScriptError(f"JavaScript error: {e}") from e


# ---------------------------------------------------------------- Python

class Row(tuple):
    """A result row: a tuple that also answers to column names, like a Snowpark Row."""

    def __new__(cls, values, columns):  # noqa: ANN001, ANN204
        self = super().__new__(cls, values)
        self._columns = [c.upper() for c in columns]
        return self

    def __getattr__(self, name: str):  # noqa: ANN204
        try:
            return self[self._columns.index(name.upper())]
        except ValueError:
            raise AttributeError(name) from None

    def __getitem__(self, key):  # noqa: ANN001, ANN204
        return super().__getitem__(self._columns.index(key.upper()) if isinstance(key, str) else key)

    def as_dict(self) -> dict:
        return dict(zip(self._columns, self))

    asDict = as_dict  # noqa: N815


class _Writer:
    def __init__(self, frame: "Frame") -> None:
        self.frame, self._mode = frame, "errorifexists"

    def mode(self, mode: str) -> "_Writer":
        self._mode = mode.lower()
        return self

    def save_as_table(self, name, mode: str | None = None, **_kw) -> None:  # noqa: ANN001, ANN003
        name = ".".join(name) if isinstance(name, (list, tuple)) else name
        mode = (mode or self._mode).lower()
        if mode == "append":
            self.frame.session._run(f"insert into {name} {self.frame.query}")
        else:
            self.frame.session._run(f"create or replace table {name} as {self.frame.query}")

    saveAsTable = save_as_table  # noqa: N815


class Frame:
    """A query that has not run yet: the part of a Snowpark DataFrame that needs no Snowpark."""

    def __init__(self, session: "Session", query: str) -> None:
        self.session, self.query = session, query

    @property
    def queries(self) -> dict:
        return {"queries": [self.query], "post_actions": []}

    @property
    def write(self) -> _Writer:
        return _Writer(self)

    @property
    def columns(self) -> list[str]:
        return self.session._run(f"select * from ({self.query}) limit 0")[0]

    def collect(self) -> list[Row]:
        columns, rows = self.session._run(self.query)
        return [Row(r, columns) for r in rows]

    def count(self) -> int:
        return self.session._run(f"select count(*) from ({self.query})")[1][0][0]

    def first(self):  # noqa: ANN201
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def limit(self, n: int) -> "Frame":
        return Frame(self.session, f"select * from ({self.query}) limit {int(n)}")

    def filter(self, condition: str) -> "Frame":
        return Frame(self.session, f"select * from ({self.query}) where {condition}")

    where = filter

    def select(self, *columns) -> "Frame":  # noqa: ANN002
        flat = [c for group in columns for c in (group if isinstance(group, (list, tuple)) else [group])]
        return Frame(self.session, f"select {', '.join(map(str, flat))} from ({self.query})")

    def to_pandas(self):  # noqa: ANN201
        try:
            import pandas as pd
        except ImportError as e:
            raise ScriptError("to_pandas() needs pandas: install it next to fauxflake (`pip install pandas`).") from e
        columns, rows = self.session._run(self.query)
        return pd.DataFrame([list(r) for r in rows], columns=columns)

    toPandas = to_pandas  # noqa: N815

    def show(self, n: int = 10) -> None:
        for row in self.limit(n).collect():
            print(row)


class Session:
    """What a Python procedure or model receives as `session` when Snowpark is not installed."""

    def __init__(self, run: Run) -> None:
        self._run = run

    def sql(self, query: str, params=None) -> Frame:  # noqa: ANN001
        binds = list(params or [])
        return Frame(self, re.sub(r"\?", lambda _m: literal(binds.pop(0)) if binds else "?", query.strip().rstrip(";")))

    def table(self, name) -> Frame:  # noqa: ANN001
        return Frame(self, "select * from " + (".".join(name) if isinstance(name, (list, tuple)) else name))

    def call(self, name: str, *args):  # noqa: ANN002, ANN201
        rows = self._run(f"call {name}({', '.join(literal(a) for a in args)})")[1]
        return rows[0][0] if rows else None

    def create_dataframe(self, data, schema=None) -> Frame:  # noqa: ANN001
        if hasattr(data, "to_dict") and hasattr(data, "columns"):  # a pandas DataFrame
            schema, data = [str(c) for c in data.columns], data.values.tolist()
        rows = [list(r) if isinstance(r, (list, tuple)) else [r] for r in data]
        names = list(schema) if schema else [f"_{i + 1}" for i in range(len(rows[0]) if rows else 1)]
        if not rows:
            return Frame(self, "select " + ", ".join(f'null as "{n}"' for n in names) + " where false")
        from .objects import plain

        selects = ["select " + ", ".join(f'{literal(plain(v))} as "{n}"' for v, n in zip(r, names)) for r in rows]
        return Frame(self, " union all ".join(selects))

    createDataFrame = create_dataframe  # noqa: N815

    def write_pandas(self, df, table_name: str, **_kw) -> Frame:  # noqa: ANN001, ANN003
        self.create_dataframe(df).write.mode("overwrite" if _kw.get("overwrite") else "append").save_as_table(table_name)
        return self.table(table_name)

    def use_database(self, name: str) -> None:
        self._run(f"use database {name}")

    def use_schema(self, name: str) -> None:
        self._run(f"use schema {name}")

    def get_current_database(self):  # noqa: ANN201
        return self._run("select current_database()")[1][0][0]

    def get_current_schema(self):  # noqa: ANN201
        return self._run("select current_schema()")[1][0][0]
