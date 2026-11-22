"""JSON as Snowflake reads it (PARSE_JSON, TRY_PARSE_JSON, CHECK_JSON), measured against it (conformance/cases.yml,
check_json_* and lenient_json_*).

Snowflake's reader is lenient: a key needs no quotes, a trailing comma is allowed (in an array it adds an undefined
element), NaN and Infinity are numbers, 01 is 1. It refuses a key given twice. Text that it refuses comes with its
own words for what is wrong and where (`missing colon, pos 6`). Empty or blank text is NULL.
"""
from __future__ import annotations

import json

UNDEFINED = "﷐undefined"  # (semantics.UNDEFINED: an undefined element of an array)


class JSONError(ValueError):
    pass


class _Reader:
    def __init__(self, text: str) -> None:
        self.s, self.i, self.n = text, 0, len(text)

    def fail(self, message: str, at: int | None = None) -> JSONError:
        return JSONError(message if at is None else f"{message}, pos {at}")

    def space(self) -> None:
        while self.i < self.n and self.s[self.i] in " \t\r\n\f\v":
            self.i += 1

    def value(self):  # noqa: ANN201
        self.space()
        if self.i >= self.n:
            raise self.fail("unexpected end of input", self.i + 1)
        c = self.s[self.i]
        if c == "{":
            return self.obj()
        if c == "[":
            return self.arr()
        if c == '"':
            return self.string()
        if c in "}]":
            raise self.fail(f"misplaced {c}", self.i + 1)
        if c == "-" or c.isdigit() or c == ".":
            return self.number()
        if c.isalpha() or c == "_":
            return self.keyword()
        raise self.fail(f"misplaced {c}", self.i + 1)

    def keyword(self):  # noqa: ANN201
        start = self.i
        while self.i < self.n and (self.s[self.i].isalnum() or self.s[self.i] == "_"):
            self.i += 1
        word = self.s[start:self.i]
        known = {"true": True, "false": False, "null": None, "NaN": float("nan"), "Infinity": float("inf")}
        if word in known:
            return known[word]
        raise self.fail(f'unknown keyword "{word}"', self.i + 1)

    def number(self):  # noqa: ANN201
        start = self.i
        if self.s[self.i] == "-":
            self.i += 1
            if self.s.startswith("Infinity", self.i):
                self.i += 8
                return float("-inf")
            if self.i >= self.n or not (self.s[self.i].isdigit() or self.s[self.i] == "."):
                raise self.fail("no number after a sign", self.i + 1)
        while self.i < self.n and (self.s[self.i].isdigit() or self.s[self.i] in ".eE+-"):
            self.i += 1
        text = self.s[start:self.i]
        try:
            value = float(text)
        except ValueError:
            raise self.fail(f"garbage in the numeric literal: {text} ", self.i + 1) from None
        if any(ch in text for ch in ".eE"):
            return _Number(text, value)
        return int(text)

    def string(self) -> str:
        self.i += 1
        out = []
        while True:
            if self.i >= self.n:
                raise self.fail("unfinished string", self.i + 1)
            c = self.s[self.i]
            if c == '"':
                self.i += 1
                return "".join(out)
            if c == "\\":
                self.i += 1
                if self.i >= self.n:
                    raise self.fail("unfinished string", self.i + 1)
                e = self.s[self.i]
                if e == "u":
                    out.append(chr(int(self.s[self.i + 1:self.i + 5], 16)))
                    self.i += 5
                    continue
                out.append({"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f"}.get(e, e))
                self.i += 1
                continue
            out.append(c)
            self.i += 1

    def key(self) -> str:
        if self.s[self.i] == '"':
            return self.string()
        start = self.i  # (a key without quotes, as Snowflake takes one)
        while self.i < self.n and (self.s[self.i].isalnum() or self.s[self.i] in "_$"):
            self.i += 1
        if self.i == start:
            raise self.fail(f"misplaced {self.s[self.i]}", self.i + 1)
        return self.s[start:self.i]

    def obj(self) -> dict:
        self.i += 1
        out: dict = {}
        while True:
            self.space()
            if self.i >= self.n:
                raise self.fail("incomplete object value", self.i + 1)
            if self.s[self.i] == "}":
                self.i += 1
                return out
            name = self.key()
            if name in out:
                raise self.fail(f'duplicate object attribute "{name}"', self.i)
            self.space()
            if self.i >= self.n or self.s[self.i] != ":":
                raise self.fail("missing colon", self.i + 1)
            self.i += 1
            self.space()
            if self.i < self.n and self.s[self.i] == "}":
                raise self.fail("misplaced }", self.i + 1)
            out[name] = self.value()
            self.space()
            if self.i >= self.n:
                raise self.fail("incomplete object value", self.i + 1)
            if self.s[self.i] == ",":
                self.i += 1
                continue
            if self.s[self.i] == "}":
                self.i += 1
                return out
            raise self.fail(f"misplaced {self.s[self.i]}", self.i + 1)

    def arr(self) -> list:
        self.i += 1
        out: list = []
        after_comma = False
        while True:
            self.space()
            if self.i >= self.n:
                raise self.fail("incomplete array value", self.i + 1)
            if self.s[self.i] == "]":
                self.i += 1
                if after_comma:
                    out.append(UNDEFINED)  # (`[1,]` is [1, undefined])
                return out
            out.append(self.value())
            self.space()
            if self.i >= self.n:
                raise self.fail("incomplete array value", self.i + 1)
            if self.s[self.i] == ",":
                self.i += 1
                after_comma = True
                continue
            if self.s[self.i] == "]":
                self.i += 1
                return out
            raise self.fail(f"misplaced {self.s[self.i]}", self.i + 1)


class _Number(float):
    """A number written with a point or an exponent: kept as it was written."""

    def __new__(cls, text: str, value: float):  # noqa: ANN204
        made = super().__new__(cls, value)
        made.text = text
        return made


def read(text: str):  # noqa: ANN201
    """The document `text` holds (None when it is blank), or JSONError with Snowflake's words."""
    reader = _Reader(text)
    reader.space()
    if reader.i >= reader.n:
        return None
    value = reader.value()
    reader.space()
    if reader.i < reader.n:
        if reader.s[reader.i] in "}]":
            raise reader.fail(f"misplaced {reader.s[reader.i]}", reader.i + 1)
        try:
            reader.value()
        except JSONError:
            raise reader.fail("garbage after valid input document") from None
        raise reader.fail("more than one document in the input")
    return value


def _dump(v) -> str:  # noqa: ANN001
    if isinstance(v, _Number) and ("e" in v.text or "E" in v.text):
        return float_text(float(v))  # a number written with an exponent is a FLOAT
    if isinstance(v, _Number):
        text = v.text
        if text.startswith("."):
            text = "0" + text
        if text.startswith("-."):
            text = "-0" + text[1:]
        return text
    if isinstance(v, float):
        return "NaN" if v != v else ("Infinity" if v > 0 else "-Infinity")
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _dump(x) for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "[" + ",".join(_dump(x) for x in v) + "]"
    return json.dumps(v, ensure_ascii=False)


# A FLOAT held in a VARIANT is written with its exponent and all 17 digits (1.5000000000000000e+00; Snowflake's TO_JSON
# shows 16 of them), a NUMBER without an exponent: the text alone tells them apart. DuckDB's own JSON functions write every number again (1.5), so where a
# document holds one, the emulator reads and writes it here instead (semantics: _sf_get, _sf_list and the builders).
import re as _re

FLOAT = _re.compile(r"[0-9]\.[0-9]{16}e[+-][0-9]")
FLOAT_SQL = "[0-9][.][0-9]{16}e[-+][0-9]"  # (the same, for regexp_matches)


class _Kept(str):
    """A number, or a whole value, as its text."""


def _kept(text: str):  # noqa: ANN202
    return json.loads(text, parse_float=_Kept, parse_int=_Kept, parse_constant=_Kept)


def _write(v) -> str:  # noqa: ANN001
    if isinstance(v, _Kept):
        return str(v)
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _write(x) for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "[" + ",".join(_write(x) for x in v) + "]"
    return json.dumps(v, ensure_ascii=False)


def float_text(v: float) -> str:
    """A FLOAT as it goes into a VARIANT."""
    if v != v:
        return "NaN"
    if v in (float("inf"), float("-inf")):
        return "Infinity" if v > 0 else "-Infinity"
    return "%.16e" % v


def shown(text: str) -> str:
    """A VARIANT's text as Snowflake shows it: each FLOAT with 16 digits."""
    return _re.sub(r"-?[0-9]\.[0-9]{16}e[+-][0-9]+", lambda m: "%.15e" % float(m.group()), text)


def elements(text: str | None) -> list | None:
    """The elements of a JSON array (or the values of an object), each as its own text."""
    if text is None:
        return None
    v = _kept(text)
    if isinstance(v, dict):
        v = list(v.values())
    return [_write(x) for x in v] if isinstance(v, list) else None


_STEP = _re.compile(r'\.(?:"((?:[^"\\]|\\.)*)"|([^.\[]+))|\[\s*(-?\d+)\s*\]|\[\s*"((?:[^"\\]|\\.)*)"\s*\]')


def get(text: str | None, path: str | None) -> str | None:
    """`text` -> `path` (a JSON path DuckDB reads: $.a, $."a b", $[0], $.a[1].b; or a key, or an index), keeping
    each number's text."""
    if text is None or path is None:
        return None
    v = _kept(text)
    if not path.startswith("$"):  # (v -> 'name' is a key; v -> 0 an index)
        if isinstance(v, list) and path.lstrip("-").isdigit():
            return _write(v[int(path)]) if -len(v) <= int(path) < len(v) else None
        return _write(v[path]) if isinstance(v, dict) and path in v else None
    at = 1
    while at < len(path):
        m = _STEP.match(path, at)
        if m is None:
            return None
        quoted, bare, index, keyed = m.groups()
        at = m.end()
        if index is not None:
            i = int(index)
            if not isinstance(v, list) or not -len(v) <= i < len(v):
                return None
            v = v[i]
            continue
        name = bare if bare is not None else json.loads('"' + (quoted if quoted is not None else keyed) + '"')
        if not isinstance(v, dict) or name not in v:
            return None
        v = v[name]
    return _write(v)


def merged(text: str | None, name: str | None, value: str | None) -> str | None:
    """json_merge_patch(text, {name: value}) for one key, keeping each number's text: a null value removes the key."""
    if text is None:
        return None
    v = _kept(text)
    if not isinstance(v, dict):
        return text
    if value is None or value == "null":
        v.pop(name, None)
    else:
        v[name] = _Kept(value)
    return _write(v)


def _canonical(v) -> str:  # noqa: ANN001
    if isinstance(v, _Kept):
        if v in ("NaN", "Infinity", "-Infinity"):
            return str(v)
        if "e" in v or "E" in v:
            return float_text(float(v))
        if "." in v:
            shortest = repr(float(v))
            return shortest if "e" not in shortest else str(v)
        return str(v)
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _canonical(x) for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "[" + ",".join(_canonical(x) for x in v) + "]"
    return json.dumps(v, ensure_ascii=False)


def _no_twice(pairs: list) -> dict:
    seen: dict = {}
    for k, v in pairs:
        if k in seen:
            raise KeyError(k)
        seen[k] = v
    return seen


def normalise(text: str | None) -> str | None:
    """PARSE_JSON: the document as strict JSON for DuckDB (text that already is comes back as it is), None when blank."""
    if text is None:
        return None
    try:
        json.loads(text, object_pairs_hook=_no_twice)
        if not _re.search(r"[0-9][eE]", text):
            return text if text.strip() else None
        # a FLOAT in it: written as the emulator keeps one, the other numbers as DuckDB writes them (1.50 is 1.5)
        return _canonical(_kept(text))
    except (ValueError, KeyError):
        pass
    if not text.strip():
        return None
    value = read(text)
    return None if value is None and not text.strip() else _dump(value)


def check(text: str | None) -> str | None:
    """CHECK_JSON: NULL when the text is JSON Snowflake reads, else what is wrong with it."""
    if text is None:
        return None
    try:
        read(text)
    except JSONError as e:
        return str(e)
    return None
