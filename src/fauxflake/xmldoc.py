"""XML as Snowflake has it, measured against it (conformance/cases.yml, xml_*).

PARSE_XML gives a value Snowflake reads as an OBJECT: "@" the tag, "$" what the element holds, "@name" each
attribute. What an element holds is converted where it reads as a value (`1` an INTEGER, `1.50` a DECIMAL, `true`
a BOOLEAN, `007` stays text); one child element is that element, several (or text beside elements) a list. Written
as text, the value is XML again.

In the emulator a VARIANT is JSON, so an element is a JSON object with those keys and one more, MARK, whose value
lists the attributes in the order they were written: what tells an element from an OBJECT (TYPEOF says XML), and
how it is written back.
"""
from __future__ import annotations

import json
import re
from decimal import Decimal

from . import jsondoc

MARK = "﷐xml"


class XMLError(ValueError):
    def __init__(self, message: str, pos: int | None = None) -> None:
        super().__init__(message if pos is None else f"{message}, pos {pos}")


_NAME = re.compile(r"[A-Za-z_:][\w.\-:]*")
_ENTITIES = {"lt": "<", "gt": ">", "amp": "&", "quot": '"', "apos": "'"}


def _unescape(text: str) -> str:
    def one(m: re.Match) -> str:
        name = m.group(1)
        if name.startswith("#x") or name.startswith("#X"):
            return chr(int(name[2:], 16))
        if name.startswith("#"):
            return chr(int(name[1:]))
        return _ENTITIES.get(name, m.group(0))

    return re.sub(r"&(#[0-9]+|#[xX][0-9a-fA-F]+|[A-Za-z]+);", one, text)


def _value(text: str):  # noqa: ANN202
    """Text as Snowflake converts it: a number or a boolean when it reads as one, else the text."""
    bare = text.strip()
    if re.fullmatch(r"-?(0|[1-9]\d*)", bare):
        return int(bare)
    if re.fullmatch(r"-?(0|[1-9]\d*)\.\d+", bare):
        return Decimal(bare)
    if re.fullmatch(r"-?(0|[1-9]\d*)(\.\d+)?[eE][+-]?\d+", bare):
        return float(bare)
    if bare in ("true", "false"):
        return bare == "true"
    return text


def parse(text: str) -> dict | None:
    """The element `text` holds, or XMLError with Snowflake's words for what is wrong."""
    i, n = 0, len(text)
    stack: list[tuple[str, dict, list]] = []  # (tag, attributes, children)
    root = None

    def skip_space(j: int) -> int:
        while j < n and text[j].isspace():
            j += 1
        return j

    while True:
        if not stack:
            i = skip_space(i)
            while text.startswith(("<?", "<!--", "<!DOCTYPE", "<!doctype"), i):  # a prolog, a comment
                end = text.find("?>" if text.startswith("<?", i) else "-->" if text.startswith("<!--", i) else ">", i)
                if end < 0:
                    raise XMLError("not an XML element", i + 1)
                i = skip_space(end + (2 if text.startswith("<?", i) else 3 if text.startswith("<!--", i) else 1))
            if i >= n:
                return root
            if root is not None:
                raise XMLError("more than one document in the input")
            if text[i] != "<":
                raise XMLError("not an XML element", i + 1)
        if i >= n:
            raise XMLError("missing closing tags: " + "".join(f"</{t}>" for t, _, _ in reversed(stack)), n)
        if text.startswith("<![CDATA[", i):
            end = text.find("]]>", i)
            if end < 0:
                raise XMLError("missing closing tags: " + "".join(f"</{t}>" for t, _, _ in reversed(stack)), n)
            stack[-1][2].append(text[i + 9:end])
            i = end + 3
            continue
        if text.startswith("<!--", i):
            end = text.find("-->", i)
            i = n if end < 0 else end + 3
            continue
        if text.startswith("</", i):
            m = _NAME.match(text, i + 2)
            end = text.find(">", i)
            if not m or end < 0:
                raise XMLError("not an XML element", i + 1)
            tag = m.group(0)
            open_tags = [t for t, _, _ in stack]
            if tag not in open_tags:
                raise XMLError(f"no opening tag for </{tag}>", end + 1)
            if open_tags[-1] != tag:
                deeper = open_tags[open_tags.index(tag) + 1:]
                raise XMLError("missing closing tags: " + "".join(f"</{t}>" for t in reversed(deeper)), end + 1)
            element = _element(*stack.pop())
            i = end + 1
            if stack:
                stack[-1][2].append(element)
            else:
                root = element
            continue
        if text.startswith("<", i):
            m = _NAME.match(text, i + 1)
            if not m:
                raise XMLError("not an XML element", i + 1)
            tag, j, attrs = m.group(0), m.end(), {}
            while True:
                j = skip_space(j)
                if j >= n:
                    raise XMLError("missing closing tags: " + "".join(f"</{t}>" for t, _, _ in reversed(stack + [(tag, {}, [])])), n)
                if text.startswith("/>", j) or text[j] == ">":
                    break
                a = _NAME.match(text, j)
                if not a:
                    raise XMLError("not an XML element", j + 1)
                j = skip_space(a.end())
                value = ""
                if j < n and text[j] == "=":
                    j = skip_space(j + 1)
                    if j < n and text[j] in "\"'":
                        end = text.find(text[j], j + 1)
                        if end < 0:
                            raise XMLError("not an XML element", j + 1)
                        value, j = _unescape(text[j + 1:end]), end + 1
                    else:  # (unquoted, as Snowflake takes it)
                        k = j
                        while k < n and not text[k].isspace() and text[k] not in "/>":
                            k += 1
                        value, j = _unescape(text[j:k]), k
                attrs[a.group(0)] = value
            if text.startswith("/>", j):
                element = _element(tag, attrs, [])
                i = j + 2
                if stack:
                    stack[-1][2].append(element)
                else:
                    root = element
            else:
                stack.append((tag, attrs, []))
                i = j + 1
            continue
        end = text.find("<", i)
        end = n if end < 0 else end
        piece = _unescape(text[i:end])
        if stack and piece.strip():
            stack[-1][2].append(piece)
        i = end


def _element(tag: str, attrs: dict, children: list) -> dict:
    out: dict = {"@": tag}
    for k, v in attrs.items():
        out["@" + k] = _value(v)
    if not children:
        out["$"] = ""
    elif len(children) == 1:
        out["$"] = _value(children[0]) if isinstance(children[0], str) else children[0]
    else:
        out["$"] = children
    out[MARK] = list(attrs)
    return out


def to_json(text: str) -> str | None:
    """PARSE_XML: the element as the emulator's JSON."""
    element = parse(text)
    if element is None:
        return None

    def encode(v) -> str:  # noqa: ANN001
        if isinstance(v, dict):
            return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + encode(x) for k, x in v.items()) + "}"
        if isinstance(v, list):
            return "[" + ",".join(encode(x) for x in v) + "]"
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, float):
            return jsondoc.float_text(v)
        return json.dumps(v, ensure_ascii=False)

    return encode(element)


def is_element(v) -> bool:  # noqa: ANN001
    return isinstance(v, dict) and MARK in v and isinstance(v.get("@"), str)


def _text_of(v) -> str:  # noqa: ANN001
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return ("%.15g" % v)
    if isinstance(v, Decimal):
        s = str(v)
        return s.rstrip("0").rstrip(".") if "." in s else s
    return str(v)


def _escape(text: str, quote: bool = False) -> str:
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text.replace('"', "&quot;") if quote else text


def write(v) -> str:  # noqa: ANN001
    """An element as XML text."""
    if is_element(v):
        attrs = "".join(f' {k}="{_escape(_text_of(v.get("@" + k, "")), True)}"' for k in v[MARK])
        return f"<{v['@']}{attrs}>{write(v.get('$', ''))}</{v['@']}>"
    if isinstance(v, list):
        return "".join(write(x) for x in v)
    if v is None:
        return ""
    if isinstance(v, dict):
        return _escape(json.dumps(v, ensure_ascii=False))
    return _escape(_text_of(v))


def loads(text: str):  # noqa: ANN201
    return json.loads(text, parse_float=Decimal)


def check(text: str | None) -> str | None:
    """CHECK_XML: NULL when the text is XML, else what is wrong with it."""
    if text is None:
        return None
    try:
        parse(text)
    except XMLError as e:
        return str(e)
    return None


def xmlget(text: str | None, tag: str | None, instance: int | None = 0) -> str | None:
    """XMLGET: the `instance`-th child element named `tag`, or NULL."""
    if text is None or tag is None:
        return None
    try:
        v = loads(text)
    except ValueError:
        return None
    if not is_element(v):
        return None
    content = v.get("$")
    children = content if isinstance(content, list) else [content]
    hits = [c for c in children if is_element(c) and c["@"] == tag]
    k = int(instance or 0)
    if k < 0 or k >= len(hits):
        return None
    return to_json_value(hits[k])


def to_json_value(v) -> str:  # noqa: ANN001
    def encode(x) -> str:  # noqa: ANN001
        if isinstance(x, dict):
            return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + encode(y) for k, y in x.items()) + "}"
        if isinstance(x, list):
            return "[" + ",".join(encode(y) for y in x) + "]"
        if isinstance(x, Decimal):
            return str(x)
        return json.dumps(x, ensure_ascii=False)

    return encode(v)


def to_xml(text: str | None) -> str | None:
    """TO_XML: an element as its XML; any other value as Snowflake's <SnowflakeData type="...">."""
    if text is None:
        return None
    raw = json.loads(text, parse_float=lambda s: ("float" if "e" in s.lower() else "decimal", s),
                     parse_int=lambda s: ("int", s))
    plain = loads(text)
    if is_element(plain):
        return write(plain)

    def kind_of(v) -> str:  # noqa: ANN001
        if v is None:
            return "NULL_VALUE"
        if isinstance(v, bool):
            return "BOOLEAN"
        if isinstance(v, tuple):
            return {"int": "INTEGER", "decimal": "DECIMAL", "float": "DOUBLE"}[v[0]]
        if isinstance(v, str):
            return "VARCHAR"
        if isinstance(v, list):
            return "ARRAY"
        return "OBJECT"

    def body(v) -> str:  # noqa: ANN001
        if isinstance(v, tuple):
            kind, s = v
            if kind == "float":
                return "%.15e" % float(s)
            if kind == "decimal":
                return s.rstrip("0").rstrip(".") if "." in s else s
            return s
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, str):
            return _escape(v)
        if isinstance(v, list):
            return "".join(node("e", x) for x in v)
        if isinstance(v, dict):
            return "".join(node(k, v[k]) for k in sorted(v, key=lambda k: k.encode()))
        return ""

    def node(tag: str, v) -> str:  # noqa: ANN001
        kind = kind_of(v)
        if kind == "NULL_VALUE":
            return f'<{tag} type="NULL_VALUE" xsi:nil="true"/>'
        return f'<{tag} type="{kind}">{body(v)}</{tag}>'

    return node("SnowflakeData", raw)
