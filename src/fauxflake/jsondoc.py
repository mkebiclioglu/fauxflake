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
        return text if text.strip() else None
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
