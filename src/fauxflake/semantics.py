"""Where Snowflake and DuckDB disagree on the *answer*, not the syntax.

The emulator transpiles SQL faithfully enough to run, but several functions then
return different values or types. Each fix here exists because a case in
`conformance/` failed against an answer recorded from real Snowflake.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from . import catalog, jsondoc, xmldoc

_NS = "_fs_global.main."
_SCALE = r"coalesce(try_cast(nullif(regexp_extract(typeof(a), ',(\d+)\)', 1), '') as integer), 0)"

def python_functions(duck) -> None:  # noqa: ANN001
    """What DuckDB has no function for and SQL cannot say: registered once, before the macros that call them."""
    import hashlib

    def sha2(text: str | None, bits: int | None) -> str | None:
        if text is None or bits not in (224, 256, 384, 512):
            return None
        return hashlib.new(f"sha{bits}", text.encode()).hexdigest()

    import pyarrow as pa

    def sorted_json(values):  # noqa: ANN001, ANN202 - a batch at a time
        return pa.array([json_sorted(v) for v in values.to_pylist()], pa.string())

    for name, fn, args, out, kind in (
            ("_fxpy_sha2", sha2, ["VARCHAR", "INTEGER"], "VARCHAR", None),
            ("_fxpy_json_sorted", sorted_json, ["VARCHAR"], "VARCHAR", "arrow"),
            ("_fxpy_encrypt_raw", encrypt_raw, ["BLOB", "BLOB", "BLOB", "BLOB", "VARCHAR"], "VARCHAR", None),
            ("_fxpy_randstr", randstr, ["BIGINT", "HUGEINT"], "VARCHAR", None),
            ("_fxpy_parse_xml", parse_xml, ["VARCHAR"], "VARCHAR", None),
            ("_fxpy_json_text", json_text, ["VARCHAR", "BOOLEAN"], "VARCHAR", None),
            ("_fxpy_check_json", jsondoc.check, ["VARCHAR"], "VARCHAR", None),
            ("_fxpy_parse_url", parse_url, ["VARCHAR", "INTEGER"], "VARCHAR", None),
            ("_fxpy_check_xml", xmldoc.check, ["VARCHAR"], "VARCHAR", None),
            ("_fxpy_to_xml", xmldoc.to_xml, ["VARCHAR"], "VARCHAR", None),
            ("_fxpy_xmlget", xmldoc.xmlget, ["VARCHAR", "VARCHAR", "BIGINT"], "VARCHAR", None),
            ("_fxpy_seeded_unit", seeded_unit, ["HUGEINT"], "DOUBLE", None),
            ("_fxpy_decrypt_raw", decrypt_raw, ["BLOB", "BLOB", "BLOB", "BLOB", "VARCHAR", "BLOB", "BOOLEAN"], "BLOB", None)):
        try:
            duck.create_function(name, fn, args, out, null_handling="special", side_effects=name == "_fxpy_encrypt_raw",
                                 **({"type": kind} if kind else {}))
        except Exception:  # noqa: BLE001, S110 - registered already on this connection
            pass
    duck.execute(f"create or replace macro {_NS}_fx_sha2(x, n) as _fxpy_sha2(x, n)")
    duck.execute(f"create or replace macro {_NS}_sf_randstr(n, seed) as _fxpy_randstr(n, seed)")
    duck.execute(f"create or replace macro {_NS}_sf_parse_xml (s) as _fxpy_parse_xml(s::varchar)::json, (s, b) as _fxpy_parse_xml(s::varchar)::json")
    duck.execute(f"create or replace macro {_NS}_sf_check_xml (s) as _fxpy_check_xml(s::varchar), (s, b) as _fxpy_check_xml(s::varchar)")
    duck.execute(f"create or replace macro {_NS}_sf_to_xml(v) as _fxpy_to_xml(v::varchar)")
    duck.execute(f"create or replace macro {_NS}_sf_check_json(v) as _fxpy_check_json(v::varchar)")
    duck.execute(f"create or replace macro {_NS}_sf_parse_url (u) as _fxpy_parse_url(u::varchar, 0)::json, "
                 "(u, permissive) as _fxpy_parse_url(u::varchar, permissive)::json")
    duck.execute(f"create or replace macro {_NS}_sf_xmlget (x, t) as _fxpy_xmlget(x::varchar, t, 0)::json, "
                 "(x, t, n) as _fxpy_xmlget(x::varchar, t, n)::json")
    duck.execute(f"create or replace macro {_NS}_fx_json_sorted(x) as _fxpy_json_sorted(x)")
    duck.execute(f"create or replace macro {_NS}_sf_encrypt_raw (v, k) as _fxpy_encrypt_raw(v, k, null, null, null)::json, "
                 "(v, k, iv) as _fxpy_encrypt_raw(v, k, iv, null, null)::json, "
                 "(v, k, iv, aad) as _fxpy_encrypt_raw(v, k, iv, aad, null)::json, "
                 "(v, k, iv, aad, m) as _fxpy_encrypt_raw(v, k, iv, aad, m)::json")
    for name, lenient in (("decrypt_raw", "false"), ("try_decrypt_raw", "true")):
        duck.execute(f"create or replace macro {_NS}_sf_{name} (v, k, iv) as _fxpy_decrypt_raw(v, k, iv, null, null, null, {lenient}), "
                     f"(v, k, iv, aad) as _fxpy_decrypt_raw(v, k, iv, aad, null, null, {lenient}), "
                     f"(v, k, iv, aad, m) as _fxpy_decrypt_raw(v, k, iv, aad, m, null, {lenient}), "
                     f"(v, k, iv, aad, m, tag) as _fxpy_decrypt_raw(v, k, iv, aad, m, tag, {lenient})")


def json_text(text, lenient):  # noqa: ANN001, ANN201
    """What PARSE_JSON reads in `text`, as JSON DuckDB reads (see jsondoc); TRY_PARSE_JSON (`lenient`) gives NULL for
    what Snowflake refuses."""
    try:
        return jsondoc.normalise(text)
    except jsondoc.JSONError as e:
        if lenient:
            return None
        raise _fx_error(100069, "22P02", f"Error parsing JSON: {e}") from None


def parse_url(url, permissive):  # noqa: ANN001, ANN201
    """PARSE_URL as Snowflake has it, measured (conformance parse_url_*): the scheme is what comes before the first ':'
    (none is an error), the host what follows '//' up to the path (a user name stays in it), the path without its
    first '/' when there is a host, the query's parameters as written (the last of a name repeated; a name without
    '=' is null). `permissive`: an error is {"error": ...} instead."""
    import json

    if url is None:
        return None
    fields = dict.fromkeys(("fragment", "host", "parameters", "path", "port", "query", "scheme"))

    def failed(message: str):  # noqa: ANN202
        if permissive:
            return json.dumps({"error": message})
        raise _fx_error(100139, "22000", f"Error parsing URL: {message}")

    if url == "":
        return json.dumps(fields)
    rest = url
    if "#" in rest:
        rest, fields["fragment"] = rest.split("#", 1)
    if "?" in rest:
        rest, query = rest.split("?", 1)
        fields["query"] = query
        params: dict = {}
        for part in query.split("&") if query else []:
            key, eq, value = part.partition("=")
            params[key] = value if eq else None
        fields["parameters"] = params
    colon, slash = rest.find(":"), rest.find("/")
    if colon <= 0 or (0 <= slash < colon):
        return failed("scheme not specified")
    fields["scheme"], rest = rest[:colon], rest[colon + 1:]
    if rest.startswith("//"):
        rest = rest[2:]
        cut = rest.find("/")
        authority, path = (rest, None) if cut < 0 else (rest[:cut], rest[cut + 1:])
        at = authority.rfind("@")
        hostport = authority[at + 1:]
        if ":" in hostport:
            host, port = hostport.rsplit(":", 1)
            if not port.isdigit():
                return failed("port must be a number")
            fields["port"], authority = port, authority[:at + 1] + host
        fields["host"] = authority or None
        fields["path"] = path
    else:
        fields["path"] = rest or None
    return json.dumps(fields)


def parse_xml(text):  # noqa: ANN001, ANN201
    if text is None:
        return None
    try:
        return xmldoc.to_json(text)
    except xmldoc.XMLError as e:
        raise _fx_error(100100, "22P02", f"Error parsing XML: {e}") from None


# Snowflake's generator for a seed, measured: its state starts at seed × 0x9E3779B97F4A7C15 (mod 2**64) and steps as the
# 64-bit LCG of PCG's constants. UNIFORM(lo, hi, seed) is the first state as a fraction of 2**64 (the same on every row:
# uniform(0, 1000000, 1) is 618034); RANDSTR(n, seed) takes a character of 0-9a-zA-Z from each state in turn.
_PHI, _LCG_MUL, _LCG_INC, _ALPHABET = 0x9E3779B97F4A7C15, 6364136223846793005, 1442695040888963407, \
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def seeded_unit(seed):  # noqa: ANN001, ANN201
    return None if seed is None else ((int(seed) * _PHI) % 2**64) / 2**64


def randstr(n, seed):  # noqa: ANN001, ANN201
    if n is None or seed is None:
        return None
    state, out = (int(seed) * _PHI) % 2**64, []
    for _ in range(max(0, int(n))):
        out.append(_ALPHABET[(state * 62) >> 64])
        state = (state * _LCG_MUL + _LCG_INC) % 2**64
    return "".join(out)


# AES as Snowflake's ENCRYPT_RAW / DECRYPT_RAW does it: a method `AES[-mode[-padding]]` (GCM when none is given),
# the IV a mode needs (random when NULL), and Snowflake's errors for a key, an IV or a method it does not take
_IV_BITS = {"GCM": 96, "ECB": 0, "CBC": 128, "CTR": 128, "CFB": 128, "OFB": 128}


def _fx_error(errno: int, sqlstate: str, message: str) -> Exception:
    """An error a Python function raises for the emulator to give as Snowflake's (see emulator.snowflake_error)."""
    return ValueError(f"\x1ffxerr:{errno}:{sqlstate}:{message}\x1f")


def _aes(key: bytes, iv: bytes | None, method: str | None):  # noqa: ANN202
    parts = (method or "AES-GCM").upper().split("-")
    mode = parts[1] if len(parts) > 1 else "GCM"
    pad = parts[2] if len(parts) > 2 else None
    if parts[0] != "AES" or len(parts) > 3 or mode not in _IV_BITS or (pad is not None and (pad != "PKCS" or mode not in ("ECB", "CBC"))):
        raise _fx_error(100302, "22023", f"Malformed encryption method parameter: {method}")
    if len(key) * 8 not in (128, 192, 256):
        raise _fx_error(100304, "22023", f"Key size of {len(key) * 8} bits not found for encryption algorithm AES")
    if iv is not None and len(iv) * 8 != _IV_BITS[mode]:
        raise _fx_error(100303, "22023", f"IV/Nonce of size {len(iv) * 8} bits needs to be of size of {_IV_BITS[mode]} "
                                         f"bits for encryption mode {mode}")
    return mode


def encrypt_raw(value, key, iv, aad, method):  # noqa: ANN001, ANN201
    import json
    import os

    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if value is None or key is None:
        return None
    value, key = bytes(value), bytes(key)
    mode = _aes(key, None if iv is None else bytes(iv), method)
    iv = bytes(iv) if iv is not None else (os.urandom(_IV_BITS[mode] // 8) if mode != "ECB" else None)
    if mode == "GCM":
        out = AESGCM(key).encrypt(iv, value, None if aad is None else bytes(aad))
        return json.dumps({"ciphertext": out[:-16].hex().upper(), "iv": iv.hex().upper(), "tag": out[-16:].hex().upper()})
    if mode in ("ECB", "CBC"):
        padder = padding.PKCS7(128).padder()
        value = padder.update(value) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.ECB() if mode == "ECB" else getattr(modes, mode)(iv)).encryptor()
    out = cipher.update(value) + cipher.finalize()
    return json.dumps({"ciphertext": out.hex().upper(), "iv": None if iv is None else iv.hex().upper()})


def decrypt_raw(value, key, iv, aad, method, tag, lenient):  # noqa: ANN001, ANN201
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if value is None or key is None:
        return None
    try:
        value, key = bytes(value), bytes(key)
        mode = _aes(key, None if iv is None else bytes(iv), method)
        if iv is None and mode != "ECB":
            raise _fx_error(100303, "22023", f"IV/Nonce of size 0 bits needs to be of size of {_IV_BITS[mode]} bits for encryption mode {mode}")
        if mode == "GCM":
            if tag is None:
                raise _fx_error(100308, "22023", "Decryption mode requires an AEAD tag as parameter")
            tag = bytes(tag)
            if len(tag) != 16:
                raise _fx_error(100309, "22023", f"Wrong AEAD tag size. Expected 16, but got {len(tag)}")
            try:
                return AESGCM(key).decrypt(bytes(iv), value + tag, None if aad is None else bytes(aad))
            except InvalidTag:
                raise _fx_error(100311, "22023", "Decryption failed. Check encrypted data, key, AAD, or AEAD tag.") from None
        cipher = Cipher(algorithms.AES(key), modes.ECB() if mode == "ECB" else getattr(modes, mode)(bytes(iv))).decryptor()
        out = cipher.update(value) + cipher.finalize()
        if mode in ("ECB", "CBC"):
            try:
                unpadder = padding.PKCS7(128).unpadder()
                out = unpadder.update(out) + unpadder.finalize()
            except ValueError:
                raise _fx_error(100311, "22023", "Decryption failed. Check encrypted data, key, AAD, or AEAD tag.") from None
        return out
    except ValueError:
        if lenient:
            return None
        raise


# A SQL NULL put in an ARRAY is `undefined` in Snowflake: counted by ARRAY_SIZE, written `undefined`, skipped by FLATTEN
# and ARRAY_COMPACT, read back as SQL NULL, and not the JSON null. In the emulator's JSON it is this string (U+FDD0 is a
# noncharacter no data holds)
UNDEFINED = "\ufdd0undefined"
_UNDEFINED_JSON = r"""json('"\ufdd0undefined"')"""  # (written in ASCII: DuckDB counts error positions in bytes)


def json_sorted(text: str | None) -> str | None:
    """JSON text the way Snowflake writes it: the keys of every object in byte order (Snowflake keeps an OBJECT's
    keys sorted, however it was built), numbers as they were written. Anything that is not an object or an array
    comes back as it is."""
    import json

    if text is None or not text.lstrip().startswith(("{", "[")):
        return text
    if xmldoc.MARK in text or "\\ufdd0xml" in text:  # an XML element (its mark as written, or as DuckDB escapes it): as XML
        try:
            value = xmldoc.loads(text)
        except ValueError:
            value = None
        if xmldoc.is_element(value):
            return xmldoc.write(value)

    class Raw(str):  # a number, kept as written (131.0 stays 131.0, a long decimal keeps its digits)
        pass

    def dump(v) -> str:  # noqa: ANN001
        if isinstance(v, Raw):  # 1e3 is a FLOAT (1.000000000000000e+03), 131.0 a NUMBER written without its zeros (131)
            if "e" in v or "E" in v:
                return "%.15e" % float(v) if v.lower() not in ("nan", "infinity", "-infinity") else str(v)
            return v.rstrip("0").rstrip(".") if "." in v else str(v)
        if v == UNDEFINED:
            return "undefined"
        if isinstance(v, list):
            return "[" + ",".join(dump(x) for x in v) + "]"
        if isinstance(v, dict):
            return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + dump(v[k])
                                  for k in sorted(v, key=lambda k: k.encode())) + "}"
        return json.dumps(v, ensure_ascii=False)

    try:
        return dump(json.loads(text, parse_int=Raw, parse_float=Raw, parse_constant=Raw))
    except ValueError:
        return text


MACROS = [
    # what Snowflake will not convert to a number or a boolean when it compiles a statement: a date, a time, a
    # timestamp, a binary. For those types the macro calls a function that is gone (it is dropped once the macro is
    # made), so DuckDB refuses the call when it binds it; any other type is the untyped form. (The emulator's
    # connections cast anything to text implicitly, so overloads that accept types would accept these too.)
    f"create or replace macro {_NS}_fx_refused_conversion(x) as true",
    *[f"create or replace macro {_NS}_fx_{name} " + ", ".join(
        f"(x {kind}) as {_NS}_fx_refused_conversion(x)"
        for kind in ("timestamp", "timestamp_ns", "timestamp_ms", "timestamp_s", "timestamptz", "date", "time", "timetz", "blob")
        if kind != allowed) + (f", (x {allowed}) as true" if allowed else "") + ", (x) as true"
      for name, allowed in (("convertible", None), ("try_date", "date"), ("try_binary", "blob"))],
    # TO_TIMESTAMP and TO_DATE take no FLOAT (a number of seconds is a NUMBER): the same, for DOUBLE and FLOAT only
    f"create or replace macro {_NS}_fx_no_float (x double) as {_NS}_fx_refused_conversion(x), (x float) as "
    f"{_NS}_fx_refused_conversion(x), (x decimal) as true, (x bigint) as true, (x hugeint) as true, (x integer) as true, "
    "(x varchar) as true, (x) as true",
    f"drop macro if exists {_NS}_fx_refused_conversion",
    # NUMBER / NUMBER keeps max(s, min(s + 6, 12)) decimals and raises on zero.
    # DuckDB returns a full-precision double, and NULL on zero. The failing cast raises without
    # making the expression volatile, so `a / b as r, r * 2` can still reuse the alias.
    f"""create or replace macro {_NS}_sf_div(a, b) as case
          when b = 0 and a is not null then cast('Division by zero' as double)
          when typeof(a) in ('DOUBLE', 'FLOAT', 'VARCHAR') or typeof(b) in ('DOUBLE', 'FLOAT', 'VARCHAR') then a::double / b::double
          else round(a::double / b::double, greatest({_SCALE}, least({_SCALE} + 6, 12))) end""",
    # DATE_TRUNC keeps the input type. DuckDB turns a DATE into a TIMESTAMP.
    f"""create or replace macro {_NS}_sf_date_trunc
          (p varchar, x date) as date_trunc(p, x)::date,
          (p, x) as date_trunc(p, x::timestamp)""",
    f"create table if not exists {_NS}_fx_session as select 'SYSADMIN' as role",
    f"create table if not exists {_NS}_fx_secure_views (database_name varchar, schema_name varchar, view_name varchar)",
    f"create or replace macro {_NS}_sf_current_role() as (select role from {_NS}_fx_session)",
    # GET_DDL: the statement DuckDB keeps for the object (DuckDB types, not Snowflake's).
    f"""create or replace macro {_NS}_sf_get_ddl(kind, name) as (
          select sql from (select table_name as n, sql from duckdb_tables() where database_name = current_database()
                           union all select view_name, sql from duckdb_views() where database_name = current_database())
          where upper(n) = upper(split_part(name, '.', -1)) limit 1)""",
    # TYPEOF looks inside a VARIANT. DuckDB only says JSON.
    # (an XML element is an object with the emulator's mark among its keys; a number written with an exponent is a
    # FLOAT, as Snowflake reads one)
    f"""create or replace macro {_NS}_sf_typeof(v) as case when typeof(v) <> 'JSON' then typeof(v) else
          case json_type(v::varchar)
            when 'UBIGINT' then 'INTEGER' when 'BIGINT' then 'INTEGER'
            when 'DOUBLE' then case when regexp_matches(v::varchar, '[eE]|NaN|Infinity') then 'DOUBLE' else 'DECIMAL' end
            when 'OBJECT' then case when list_contains(json_keys(v::json), chr(64976) || 'xml') then 'XML' else 'OBJECT' end
            when 'NULL' then 'NULL_VALUE' else json_type(v::varchar) end end""",
    # A windowed AVG keeps three extra decimals, not six.
    f"""create or replace macro {_NS}_sf_div3(a, b) as case
          when typeof(a) in ('DOUBLE', 'FLOAT', 'VARCHAR') then a::double / b::double
          else round(a::double / b::double, {_SCALE} + 3) end""",
    # VARIANCE of fixed-point input is fixed-point; STDDEV of integers is the root of that.
    f"""create or replace macro {_NS}_sf_var(v, a) as case
          when typeof(a) in ('DOUBLE', 'FLOAT') then v else round(v, least(2 * {_SCALE} + 6, 12)) end""",
    f"""create or replace macro {_NS}_sf_std(v, a) as case
          when typeof(a) in ('DOUBLE', 'FLOAT') or {_SCALE} > 0 then sqrt(v) else sqrt(round(v, 6)) end""",
    f"""create or replace macro {_NS}_sf_skew_fixed(g, m2, n, d, kv) as n * sqrt(n - 1) / (n - 2)
          * (g * pow(m2, 1.5) * (n - 2) / (n * sqrt(n - 1)) + 3 * d * m2 + n * d * d * d)
          / pow(trunc((m2 + n * d * d) / (n - 1) * pow(10, kv)) / pow(10, kv) * (n - 1), 1.5)""",
    f"create or replace macro {_NS}_fx_current_date() as today()",
    f"create or replace macro {_NS}_fx_current_timestamp() as get_current_timestamp()",
    f"create or replace macro {_NS}_fx_current_time() as get_current_time()",
    f"create or replace macro {_NS}_fx_localtime() as current_localtime()",
    f"create or replace macro {_NS}_fx_localtimestamp() as current_localtimestamp()",
    # SKEW of fixed-point input is taken around the mean as AVG gives it (rounded: 6 more decimals, 3 in a window)
    # with the variance cut to VARIANCE's scale (measured: 7, 2, 7 gives -1.732049872261, not -1.732050807569).
    # g, v and mu are DuckDB's skewness, var_samp and avg; d is how far the rounded mean is from the true one.
    f"""create or replace macro {_NS}_sf_skew(g, v, mu, n, a, w) as case
          when n < 3 then null
          when v = 0 then error(chr(31) || 'fxerr:100051:22012:Division by zero' || chr(31))::double
          when typeof(a) in ('DOUBLE', 'FLOAT') then g
          else {_NS}_sf_skew_fixed(g, v * (n - 1), n,
                 mu - round(mu, case when w then {_SCALE} + 3 else greatest({_SCALE}, least({_SCALE} + 6, 12)) end),
                 least(2 * {_SCALE} + 6, 12)) end""",
    f"""create or replace macro {_NS}_sf_kurtosis(k, v, n) as case
          when n < 4 then null when v = 0 then error(chr(31) || 'fxerr:100051:22012:Division by zero' || chr(31))::double
          else k end""",
    # Casting keeps the left operand's type: DATE + INTERVAL '1 month' is still a DATE.
    f"""create or replace macro {_NS}_sf_like
          (x date, r timestamp) as r::date,
          (x, r) as r""",
    # Text of a value: floats print the way C's %g does (100, 1e+20, 0.1), with 10 significant digits, up to 9 decimals
    # past the point, and 15 at most (measured: 0.3148508346, 15.022928717, 8478738.53312303); timestamps always
    # show milliseconds, and a VARIANT string loses its JSON quotes.
    f"""create or replace macro {_NS}_sf_text(x) as case
          when typeof(x) in ('DOUBLE', 'FLOAT') and x::double <> 0 and not isnan(x::double) and not isinf(x::double)
            then format('{{:.{{}}g}}', x::double, least(15, greatest(10, floor(log10(abs(x::double)))::integer + 10)))
          when typeof(x) in ('DOUBLE', 'FLOAT') then format('{{:.15g}}', x::double)
          when typeof(x) = 'JSON' and json_type(x::json) in ('OBJECT', 'ARRAY') then {_NS}_fx_json_sorted(x::varchar)
          when typeof(x) = 'JSON' then (x::json ->> '$')
          when typeof(x) in ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP_MS', 'TIMESTAMP_S')
            then strftime(try_cast(x::varchar as timestamp), '%Y-%m-%d %H:%M:%S.%g')
          when typeof(x) = 'TIMESTAMP WITH TIME ZONE'
            then strftime(timezone('UTC', try_cast(x::varchar as timestamptz)), '%Y-%m-%d %H:%M:%S.%g') || ' +0000'
          else x::varchar end""",
    # Text of a division: NUMBER / NUMBER prints every decimal of its scale ('1.500000').
    f"""create or replace macro {_NS}_sf_fixed(v, s) as
          rtrim(left(v::decimal(38, 12)::varchar, length(v::decimal(38, 12)::varchar) - (12 - s)), '.')""",
    f"""create or replace macro {_NS}_sf_div_text(a, b) as case
          when typeof(a) in ('DOUBLE', 'FLOAT', 'VARCHAR') or typeof(b) in ('DOUBLE', 'FLOAT', 'VARCHAR') then {_NS}_sf_text(a::double / b::double)
          else {_NS}_sf_fixed({_NS}_sf_div(a, b), greatest({_SCALE}, least({_SCALE} + 6, 12))) end""",
    f"""create or replace macro {_NS}_sf_round_div_text(a, b, n) as case
          when typeof(a) in ('DOUBLE', 'FLOAT', 'VARCHAR') or typeof(b) in ('DOUBLE', 'FLOAT', 'VARCHAR') then {_NS}_sf_text(round(a::double / b::double, n))
          else {_NS}_sf_fixed(round({_NS}_sf_div(a, b), n), least(n, greatest({_SCALE}, least({_SCALE} + 6, 12)))) end""",
    f"""create or replace macro {_NS}_sf_width_bucket(x, lo, hi, n) as case
          when x < lo then 0 when x >= hi then n + 1 else floor((x - lo) * n / (hi - lo))::bigint + 1 end""",
    f"""create or replace macro {_NS}_sf_array_size
          (x json) as case when json_type(x) = 'ARRAY' then json_array_length(x) end,
          (x) as len(x)""",
    f"""create or replace macro {_NS}_sf_arrays_to_object(k, v) as
          to_json(map(from_json(k::json, '["VARCHAR"]'), from_json(v::json, '["JSON"]')))""",
    f"""create or replace macro {_NS}_sf_arrays_zip
          (a, b) as to_json(list_transform(range(greatest(json_array_length(a::json), json_array_length(b::json))::bigint), lambda i:
             json_object('$1', a::json -> i::integer, '$2', b::json -> i::integer))),
          (a, b, c) as to_json(list_transform(range(greatest(json_array_length(a::json), json_array_length(b::json), json_array_length(c::json))::bigint), lambda i:
             json_object('$1', a::json -> i::integer, '$2', b::json -> i::integer, '$3', c::json -> i::integer)))""",
    f"create or replace macro {_NS}_sf_try_base64_binary(x) as try(from_base64(x::varchar))",
    f"create or replace macro {_NS}_sf_md5_lower64(x) as ('0x' || right(md5(x::varchar), 16))::ubigint::decimal(38, 0)",
    f"create or replace macro {_NS}_sf_md5_upper64(x) as ('0x' || left(md5(x::varchar), 16))::ubigint::decimal(38, 0)",
    f"create or replace macro {_NS}_sf_array_union_agg(x) as to_json(list_distinct(flatten(list(from_json(x::json, '[\"JSON\"]')))))",
    f"""create or replace macro {_NS}_sf_try_input(x, target) as case
          when typeof(x) in ('VARCHAR', '"NULL"') or (target = 'DATE' and typeof(x) = 'DATE')
            or (target = 'BOOLEAN' and typeof(x) = 'BOOLEAN') or (target = 'DOUBLE' and typeof(x) in ('DOUBLE', 'FLOAT'))
            or (target = 'BLOB' and typeof(x) = 'BLOB')
            or (target = 'WHOLE' and typeof(x) in ('BIGINT', 'HUGEINT', 'SMALLINT', 'DECIMAL(38,0)')) then x
          else error('Function TRY_CAST cannot be used with arguments of type ' || typeof(x)) end""",
    f"create or replace macro {_NS}_sf_negate(x) as -x",
    f"create or replace macro {_NS}_sf_bitcount(x) as case when typeof(x) = 'BLOB' then bit_count(x::bit) else bit_count(x::bigint) end",
    f"create or replace macro {_NS}_sf_getbit(x, p) as (x::bigint >> p::integer) & 1",
    f"create or replace macro {_NS}_sf_array_repeat(x, n) as to_json(list_transform(range(n::bigint), lambda i: x))",
    f"create or replace macro {_NS}_sf_binary_as_string(b) as decode(b::blob)",
    f"create or replace macro {_NS}_sf_string_as_binary(s) as encode(s::varchar)",
    f"create or replace macro {_NS}_sf_jaccard(a, b) as jaccard(a::varchar, b::varchar)",
    f"create or replace macro {_NS}_sf_approx_count(x) as approx_count_distinct(x)",
    f"""create or replace macro {_NS}_sf_try_hex_binary(s) as case
          when length(s::varchar) % 2 = 0 and regexp_full_match(s::varchar, '[0-9A-Fa-f]*') then unhex(s::varchar) end""",
    f"create or replace macro {_NS}_sf_is_binary(v) as case when v is not null then false end",
    f"create or replace macro {_NS}_sf_as_binary(v) as case when v is not null then null::blob end",  # (no VARIANT here holds bytes)
    f"create or replace macro {_NS}_sf_bitmap_position(bucket, bit) as (bucket - 1) * 32768 + bit + 1",
    f"create or replace macro {_NS}_sf_try_validate_utf8(s) as s",
    f"create or replace macro {_NS}_sf_current_account_name() as 'FAUXFLAKE'",
    # the text behind the base64, as UTF-8; a VARIANT string is read without its JSON quotes
    f"create or replace macro {_NS}_sf_try_base64(x) as try(decode(from_base64({_NS}_sf_text(x))))",
    f"create or replace macro {_NS}_sf_base64(x) as decode(from_base64({_NS}_sf_text(x)))",
    f"create or replace macro {_NS}_sf_array_distinct(l) as list_filter(l, lambda x, i: list_position(l, x) = i)",
    f"create or replace macro {_NS}_sf_jarowinkler(a, b) as round(jaro_winkler_similarity(upper(a), upper(b)) * 100)::bigint",
    f"""create or replace macro {_NS}_sf_object_insert
          (o, k, v) as json_merge_patch(o::json, json_object(k::varchar, v)),
          (o, k, v, f) as json_merge_patch(o::json, json_object(k::varchar, v))""",
    f"create or replace macro {_NS}_sf_object_delete(o, k) as json_merge_patch(o::json, json_object(k::varchar, null))",
    f"create or replace macro {_NS}_sf_object_pick(o, k) as json_object(k::varchar, json_extract(o::json, '$.' || k))",
    *[f"create or replace macro {_NS}_sf_is_{name}(v) as json_type(v::varchar) in ({kinds})" for name, kinds in {
        "object": "'OBJECT'", "array": "'ARRAY'", "integer": "'UBIGINT', 'BIGINT'", "varchar": "'VARCHAR'",
        "char": "'VARCHAR'", "boolean": "'BOOLEAN'", "decimal": "'UBIGINT', 'BIGINT', 'DOUBLE'",
        "double": "'UBIGINT', 'BIGINT', 'DOUBLE'", "real": "'UBIGINT', 'BIGINT', 'DOUBLE'"}.items()],
    f"create or replace macro {_NS}_sf_as_integer(v) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT') then v::bigint end",
    f"create or replace macro {_NS}_sf_as_varchar(v) as case when json_type(v::varchar) = 'VARCHAR' then v::json ->> '$' end",
    f"""create or replace macro {_NS}_sf_split_to_table(s, d) as table
          select 1 as seq, unnest(range(1, len(string_split(s, d)) + 1)) as index, unnest(string_split(s, d)) as value""",
    # SOUNDEX: first letter, then the codes of the rest; H and W do not separate equal codes.
    f"""create or replace macro {_NS}_sf_soundex_codes(s) as replace(translate(
          regexp_replace(upper(s), '[^A-Z]', '', 'g'),
          'BFPVCGJKQSXZDTLMNRAEIOUYHW', '111122222222334556000000--'), '-', '')""",
    f"""create or replace macro {_NS}_sf_soundex_squeeze(c) as regexp_replace(regexp_replace(regexp_replace(
          regexp_replace(regexp_replace(regexp_replace(regexp_replace(c, '0+', '0', 'g'), '1+', '1', 'g'),
          '2+', '2', 'g'), '3+', '3', 'g'), '4+', '4', 'g'), '5+', '5', 'g'), '6+', '6', 'g')""",
    f"""create or replace macro {_NS}_sf_soundex(s) as case
          when s is null then null
          when regexp_replace(upper(s), '[^A-Z]', '', 'g') = '' then ''
          else left(regexp_replace(s, '[^A-Za-z]', '', 'g'), 1) || left(replace(
            case when left(regexp_replace(upper(s), '[^A-Z]', '', 'g'), 1) in ('H', 'W')
                 then {_NS}_sf_soundex_squeeze({_NS}_sf_soundex_codes(s))
                 else substr({_NS}_sf_soundex_squeeze({_NS}_sf_soundex_codes(s)), 2) end, '0', '') || '000', 3) end""",
    # PARSE_IP for IPv4 addresses, with the range of the network when a prefix is given.
    f"""create or replace macro {_NS}_sf_ipv4(h) as (split_part(h, '.', 1)::bigint * 16777216
          + split_part(h, '.', 2)::bigint * 65536 + split_part(h, '.', 3)::bigint * 256 + split_part(h, '.', 4)::bigint)""",
    f"""create or replace macro {_NS}_sf_ip_bits(ip) as coalesce(try_cast(nullif(split_part(ip, '/', 2), '') as integer), 32)""",
    f"""create or replace macro {_NS}_sf_parse_ip
          (ip, kind) as case when ip is null then null else json_object(
          'family', 4,
          'host', split_part(ip, '/', 1),
          'ip_fields', [{_NS}_sf_ipv4(split_part(ip, '/', 1)), 0, 0, 0],
          'ip_type', lower(kind),
          'ipv4', {_NS}_sf_ipv4(split_part(ip, '/', 1)),
          'ipv4_range_end', {_NS}_sf_ipv4(split_part(ip, '/', 1)) | ((1::bigint << (32 - {_NS}_sf_ip_bits(ip))) - 1),
          'ipv4_range_start', {_NS}_sf_ipv4(split_part(ip, '/', 1)) & (4294967295 - ((1::bigint << (32 - {_NS}_sf_ip_bits(ip))) - 1)),
          'netmask_prefix_length', try_cast(nullif(split_part(ip, '/', 2), '') as integer),
          'snowflake$type', 'ip_address') end,
          (ip, kind, permissive) as case when ip is null then null
          when ip = '' then json_object('family', null, 'host', null, 'ip_fields', null, 'ip_type', lower(kind),
                                        'netmask_prefix_length', null, 'snowflake$type', 'ip_address')
          when not regexp_matches(ip, '^[0-9./]*$') then json_object('error', 'invalid decimal string with non-digital input')
          when len(string_split(split_part(ip, '/', 1), '.')) < 4 then json_object('error', 'Invalid IPv4 address. IP address includes too few fields.')
          when len(string_split(split_part(ip, '/', 1), '.')) > 4 then json_object('error', 'Invalid IPv4 address. IP address includes too many fields.')
          when list_bool_or(list_transform(string_split(split_part(ip, '/', 1), '.'), lambda f: f = '' or try_cast(f as integer) is null or try_cast(f as integer) > 255))
            then json_object('error', 'Invalid IPv4 address. One or more fields is higher than 255.')
          else json_object(
          'family', 4,
          'host', split_part(ip, '/', 1),
          'ip_fields', [{_NS}_sf_ipv4(split_part(ip, '/', 1)), 0, 0, 0],
          'ip_type', lower(kind),
          'ipv4', {_NS}_sf_ipv4(split_part(ip, '/', 1)),
          'ipv4_range_end', {_NS}_sf_ipv4(split_part(ip, '/', 1)) | ((1::bigint << (32 - {_NS}_sf_ip_bits(ip))) - 1),
          'ipv4_range_start', {_NS}_sf_ipv4(split_part(ip, '/', 1)) & (4294967295 - ((1::bigint << (32 - {_NS}_sf_ip_bits(ip))) - 1)),
          'netmask_prefix_length', try_cast(nullif(split_part(ip, '/', 2), '') as integer),
          'snowflake$type', 'ip_address') end""",
    # GET_IGNORE_CASE prefers the exact key, then any key that differs only in case.
    f"""create or replace macro {_NS}_sf_get_ignore_case(v, k) as json_extract(v::json, '$."' || coalesce(
          list_filter(json_keys(v::json), lambda x: x = k)[1],
          list_filter(json_keys(v::json), lambda x: lower(x) = lower(k))[1], k) || '"')""",
    # v[i] with an index worked out per row (arr[array_size(arr) - 1], obj[key_column]): a number is an element of an
    # array, text a key of an object (a VARIANT index is a key only); anything else, or a miss, is NULL
    f"""create or replace macro {_NS}_sf_element(v, i) as (case
          when typeof(i) = 'VARCHAR' then json_extract(v::json, '$."' || i::varchar || '"')
          when typeof(i) = 'JSON' then json_extract(v::json, '$."' || (try_cast(i::varchar as json) ->> '$') || '"')
          when try_cast(i::varchar as double) >= 0 then nullif(json_extract(v::json, '$[' || floor(try_cast(i::varchar as double))::bigint
            || ']'), json('"{UNDEFINED}"')) end)""",
    # Dates out of range roll over: month 14 is February of the next year, day 0 the day before.
    f"""create or replace macro {_NS}_sf_date_from_parts(y, m, d) as
          (make_date(y::bigint, 1, 1) + to_months((m - 1)::integer) + to_days((d - 1)::integer))::date""",
    f"""create or replace macro {_NS}_sf_timestamp_from_parts
          (y, m, d, h, mi, sec) as {_NS}_sf_date_from_parts(y, m, d)::timestamp + to_hours(h::bigint) + to_minutes(mi::bigint) + to_seconds(sec::double),
          (y, m, d, h, mi, sec, ns) as {_NS}_sf_date_from_parts(y, m, d)::timestamp + to_hours(h::bigint) + to_minutes(mi::bigint) + to_seconds(sec::double) + to_microseconds((ns / 1000)::bigint),
          (d, t) as (d::date + t::time)""",
    # Strings Snowflake reads as booleans beyond DuckDB's: on / off.
    f"""create or replace macro {_NS}_sf_bool(x) as case
          when typeof(x) = 'VARCHAR' and lower(trim(x::varchar)) = 'on' then true
          when typeof(x) = 'VARCHAR' and lower(trim(x::varchar)) = 'off' then false
          else x::boolean end""",
    f"""create or replace macro {_NS}_sf_try_bool(x) as case
          when lower(trim(x::varchar)) = 'on' then true
          when lower(trim(x::varchar)) = 'off' then false
          else try_cast(x::varchar as boolean) end""",
    # Replace only the nth match, counting from a start position.
    f"""create or replace macro {_NS}_sf_regexp_replace_nth(s, p, r, pos, occ) as case
          when occ = 0 then left(s, pos - 1) || regexp_replace(substr(s, pos), p, r, 'g')
          else left(s, pos - 1) || array_to_string(list_transform(
            range(1, len(regexp_split_to_array(substr(s, pos), p)) + 1),
            lambda _i: regexp_split_to_array(substr(s, pos), p)[_i] || coalesce(case
              when _i = occ then regexp_replace(regexp_extract_all(substr(s, pos), p)[_i], p, r)
              else regexp_extract_all(substr(s, pos), p)[_i] end, '')), '') end""",
    f"create or replace macro {_NS}_sf_try_hex(x) as try(decode(unhex(x)))",
    # Dates in text: ISO first, then the other formats Snowflake detects, then epoch numbers.
    f"""create or replace macro {_NS}_sf_epoch(x) as case
          when not regexp_full_match(x::varchar, '-?[0-9]{{1,18}}') then null
          when abs(x::varchar::bigint) < 31536000000 then to_timestamp(x::varchar::bigint)
          when abs(x::varchar::bigint) < 31536000000000 then to_timestamp(x::varchar::bigint / 1000.0)
          when abs(x::varchar::bigint) < 31536000000000000 then to_timestamp(x::varchar::bigint / 1000000.0)
          else to_timestamp(x::varchar::bigint / 1000000000.0) end""",
    # a cast (unlike TO_TIMESTAMP) always counts a number as seconds
    f"""create or replace macro {_NS}_sf_seconds(x) as coalesce(
          try_cast(x::varchar as timestamp), timezone('UTC', to_timestamp(try_cast(x::varchar as double))))""",
    f"""create or replace macro {_NS}_sf_try_timestamp(x) as coalesce(
          try_cast(x::varchar as timestamp),
          try_strptime(x::varchar, ['%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y', '%d-%b-%Y %H:%M:%S', '%d-%b-%Y']),
          timezone('UTC', {_NS}_sf_epoch(x)))""",
    f"""create or replace macro {_NS}_sf_try_date(x) as coalesce(try_cast(x as date), {_NS}_sf_try_timestamp(x)::date)""",
    f"""create or replace macro {_NS}_sf_to_date(x) as coalesce({_NS}_sf_try_date(x), cast(x::varchar as date))""",
    f"""create or replace macro {_NS}_sf_try_time(x) as coalesce(
          case when regexp_matches(x::varchar, '^[[:space:]]*[0-9]{{1,2}}:[0-9]{{2}}') then try_cast(x::varchar as time) end,
          timezone('UTC', {_NS}_sf_epoch(x))::time)""",
    f"""create or replace macro {_NS}_sf_object_pick(o, keys) as coalesce((
          select json_group_object(k, json_extract(o, '$."' || k || '"'))
          from (select unnest(cast(to_json(keys) as varchar[])) as k) where json_exists(o, '$."' || k || '"')), '{{}}'::json)""",
    # Object keys come back sorted: Snowflake stores objects ordered by key.
    # (of a VARIANT that holds no object: refused, as the cast to OBJECT is)
    f"""create or replace macro {_NS}_sf_object_keys(o) as to_json(list_sort(json_keys(case when o is null then null
          when typeof(o) <> 'JSON' or json_type(o::json) = 'OBJECT' then o::json when json_type(o::json) = 'NULL' then null
          else error(chr(31) || 'fxerr:100071:22000:Failed to cast variant value ' || o::varchar || ' to OBJECT' || chr(31))::json end)))""",
    f"create or replace macro {_NS}_sf_hash_agg(x) as bit_xor(hash(x))",
    # FLATTEN(..., RECURSIVE => TRUE) walks every level.
    # what FLATTEN is given, when it is a list or an object; a string or a number held in a VARIANT gives no rows,
    # as in Snowflake, instead of an error
    f"""create or replace macro {_NS}_sf_flattenable(x) as
          case when json_type(to_json(x)) in ('ARRAY', 'OBJECT') then to_json(x) end""",
    # (a value that is neither, a string or a number or JSON null, is one row: itself, at the path '')
    f"""create or replace macro {_NS}_sf_flatten_recursive(input) as table
          select SEQ, KEY, PATH, INDEX, VALUE, THIS from (
            select hash(to_json(input))::ubigint as SEQ,
                   case when fullkey like '%]' then null else "key" end as KEY,
                   regexp_replace(fullkey, '^\\$\\.?', '') as PATH,
                   case when fullkey like '%]' then "key"::bigint end as INDEX,
                   "value"::json as VALUE,
                   json_extract(to_json(input), "path") as THIS,
                   id as _fx_order
            from json_tree(to_json(input)::varchar) where parent is not null and "value" is distinct from {_UNDEFINED_JSON}
            union all
            select hash(to_json(input))::ubigint, null::varchar, '', null::bigint, to_json(input), to_json(input), 0
            where input is not null and json_type(to_json(input)) not in ('ARRAY', 'OBJECT')
          ) order by _fx_order""",
    *[f"create or replace macro {_NS}_sf_is_{name}(v) as json_type(v::varchar) = 'VARCHAR' and try_cast((v::json ->> '$') as {kind}) is not null"
      for name, kind in {"date": "date", "time": "time", "timestamp_ntz": "timestamp", "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp"}.items()],
    *[f"create or replace macro {_NS}_sf_as_{name}(v) as try_cast((v::json ->> '$') as {kind})"
      for name, kind in {"date": "date", "time": "time", "timestamp_ntz": "timestamp", "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp"}.items()],
    *[f"""create or replace macro {_NS}_sf_as_{name}
            (v) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT', 'DOUBLE') then v::double end,
            (v, p) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT', 'DOUBLE') then round(v::double, 0) end,
            (v, p, s) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT', 'DOUBLE') then round(v::double, s::integer) end"""
      for name in ("double", "real", "number", "decimal")],
    f"""create or replace macro {_NS}_sf_sha2
          (x) as sha256(x::varchar),
          (x, n) as case when n = 256 then sha256(x::varchar) else {_NS}_fx_sha2(x::varchar, n::integer) end""",
    f"create or replace macro {_NS}_sf_sha2_binary(x) as unhex(sha256(x::varchar))",
    f"create or replace macro {_NS}_sf_as_boolean(v) as case when json_type(v::varchar) = 'BOOLEAN' then v::boolean end",
    f"create or replace macro {_NS}_sf_as_object(v) as case when json_type(v::varchar) = 'OBJECT' then v::json end",
    f"create or replace macro {_NS}_sf_as_array(v) as case when json_type(v::varchar) = 'ARRAY' then v::json end",
    # Arrays: a stored ARRAY is JSON, an array built inside a query may be a DuckDB list.
    # Every array function reads either as a list of JSON values and answers with JSON.
    f"create or replace macro {_NS}_sf_list(x) as to_json(x)::json[]",
    f"""create or replace macro {_NS}_sf_sorted(a) as list_transform(list_sort(list_transform({_NS}_sf_list(a),
          lambda e: struct_pack(n := case when json_type(e) in ('UBIGINT', 'BIGINT', 'DOUBLE') then e::double end,
                                s := e::varchar, e := e))), lambda x: x.e)""",
    f"create or replace macro {_NS}_sf_array_contains(a, v) as case when v is null then null else list_contains({_NS}_sf_list(a), to_json(v)) end",
    f"create or replace macro {_NS}_sf_array_position(a, v) as nullif(list_position({_NS}_sf_list(a), to_json(v)), 0) - 1",
    f"""create or replace macro {_NS}_sf_array_to_string(a, s) as
          array_to_string(list_transform({_NS}_sf_list(a), lambda e: coalesce(nullif(e, {_UNDEFINED_JSON}) ->> '$', '')), s)""",
    f"create or replace macro {_NS}_sf_arrays_overlap(a, b) as list_has_any({_NS}_sf_list(a), {_NS}_sf_list(b))",
    f"""create or replace macro {_NS}_sf_array_slice(a, f, t) as to_json(list_slice({_NS}_sf_list(a),
          case when f >= 0 then f + 1 else f end, case when t < 0 then t - 1 else t end))""",
    f"create or replace macro {_NS}_sf_array_cat(a, b) as to_json(list_concat({_NS}_sf_list(a), {_NS}_sf_list(b)))",
    f"create or replace macro {_NS}_sf_array_append(a, v) as to_json(list_append({_NS}_sf_list(a), coalesce(to_json(v), {_UNDEFINED_JSON})))",
    f"create or replace macro {_NS}_sf_array_prepend(a, v) as to_json(list_prepend(coalesce(to_json(v), {_UNDEFINED_JSON}), {_NS}_sf_list(a)))",
    f"create or replace macro {_NS}_sf_array_unique(a) as to_json({_NS}_sf_array_distinct({_NS}_sf_list(a)))",
    f"""create or replace macro {_NS}_sf_array_sort
          (a) as to_json({_NS}_sf_sorted(a)),
          (a, up) as to_json(case when up then {_NS}_sf_sorted(a) else list_reverse({_NS}_sf_sorted(a)) end)""",
    f"create or replace macro {_NS}_sf_array_max(a) as list_last(list_filter({_NS}_sf_sorted(a), lambda e: json_type(e) <> 'NULL'))",
    f"create or replace macro {_NS}_sf_array_min(a) as list_first(list_filter({_NS}_sf_sorted(a), lambda e: json_type(e) <> 'NULL'))",
    f"""create or replace macro {_NS}_sf_array_compact(a) as
          to_json(list_filter({_NS}_sf_list(a), lambda e: e is not null and json_type(e) <> 'NULL' and e <> {_UNDEFINED_JSON}))""",
    *[f"""create or replace macro {_NS}_sf_array_{name}(a, b) as to_json(list_transform(list_filter(
          list_zip({_NS}_sf_list(a), range(1, len({_NS}_sf_list(a)) + 1)),
          lambda p: len(list_filter({_NS}_sf_list(a)[1:p[2]], lambda e: e = p[1])) {op} len(list_filter({_NS}_sf_list(b), lambda e: e = p[1]))),
          lambda p: p[1]))""" for name, op in (("intersection", "<="), ("except", ">"))],
    f"create or replace macro {_NS}_sf_array_remove(a, v) as to_json(list_filter({_NS}_sf_list(a), lambda e: e <> to_json(v)))",
    f"""create or replace macro {_NS}_sf_array_insert(a, p, v) as
          to_json(list_concat(list_concat({_NS}_sf_list(a), list_transform(range(greatest(p - len({_NS}_sf_list(a)), 0)),
            lambda i: {_UNDEFINED_JSON}))[1:p], [coalesce(to_json(v), {_UNDEFINED_JSON})], {_NS}_sf_list(a)[p + 1:]))""",
    f"create or replace macro {_NS}_sf_array_remove_at(a, p) as to_json(list_concat({_NS}_sf_list(a)[1:p], {_NS}_sf_list(a)[p + 2:]))",
    f"create or replace macro {_NS}_sf_array_flatten(a) as to_json(flatten(list_transform({_NS}_sf_list(a), lambda e: e::json[])))",
    f"create or replace macro {_NS}_sf_array_reverse(a) as to_json(list_reverse({_NS}_sf_list(a)))",
    f"""create or replace macro {_NS}_sf_array_range
          (f, t) as to_json(range(f, t)),
          (f, t, s) as to_json(range(f, t, s))""",
    # A float cast to an integer rounds half away from zero (2.5 -> 3). DuckDB rounds half to even.
    # Snowflake reads text as a NUMBER with a bare exponent mark ('12e' is 12, '.e' and '.' are 0); DOUBLE does not.
    # The text DuckDB can read instead, or NULL when it reads the text as it is
    f"""create or replace macro {_NS}_sf_num_text(x) as case
          when typeof(x) <> 'VARCHAR' then null
          when regexp_full_match(x::varchar, '\\s*[+-]?[0-9]*\\.?[0-9]*[eE]\\s*') then case
            when regexp_matches(x::varchar, '[0-9]') then regexp_replace(trim(x::varchar), '\\.?[eE]$', '') else '0' end
          when regexp_full_match(x::varchar, '\\s*[+-]?\\.\\s*') then '0' end""",
    # TIME(x): the time of day; a zoned timestamp is read in the session's zone (the emulator's is UTC)
    f"""create or replace macro {_NS}_sf_time(x) as case
          when typeof(x) like 'TIMESTAMP%' then cast(try_cast(x as timestamp) as time)
          else cast(x::varchar as time) end""",
    # CONDITIONAL_CHANGE_EVENT: how often the value changed from one row to the next (NULLs passed over), so far
    f"""create or replace macro {_NS}_sf_changes_in(l) as len(list_filter(range(1, len(l)), lambda i: l[i + 1] <> l[i]))""",
    f"""create or replace macro {_NS}_sf_change_event(l) as {_NS}_sf_changes_in(list_filter(l, lambda e: e is not null))""",
    f"""create or replace macro {_NS}_sf_int(x) as case
          when {_NS}_sf_num_text(x) is not null then cast(round({_NS}_sf_num_text(x)::decimal(38, 9)) as bigint)
          when typeof(x) in ('DOUBLE', 'FLOAT') then cast(round(try_cast(x::varchar as double)) as bigint)
          else cast(x as bigint) end""",
    f"""create or replace macro {_NS}_sf_to_object(x) as case
          when x is null then null
          when typeof(x) <> 'JSON' then x::json
          when json_type(x::json) = 'OBJECT' then x::json
          when json_type(x::json) = 'NULL' then null
          else error(chr(31) || 'fxerr:100071:22000:Failed to cast variant value ' || x::varchar || ' to OBJECT' || chr(31))::json end""",
    f"""create or replace macro {_NS}_sf_to_array(x) as case
          when x is null then null
          when typeof(x) like '%]' then to_json(x)
          when typeof(x) = 'JSON' and json_type(to_json(x)) = 'ARRAY' then to_json(x)
          else to_json([x]) end""",
    # A VARIANT holding the string 12 equals the text '12'. As JSON it is '"12"', so compare its value.
    # typeof() is known when the query is planned: for ordinary columns this is exactly `a = b`.
    f"""create or replace macro {_NS}_sf_eq(a, b) as case
          when typeof(a) = 'JSON' and typeof(b) <> 'JSON' then (a::json ->> '$') = b::varchar
          when typeof(b) = 'JSON' and typeof(a) <> 'JSON' then a::varchar = (b::json ->> '$')
          else a = b end""",
    # UPPER('ß') is 'SS'.
    f"create or replace macro {_NS}_sf_upper(s) as replace(upper(s), 'ẞ', 'SS')",
    # text = TRUE compares text, with 'true' or 'false' (case and all: 'TRUE' = TRUE is FALSE); anything else is read
    # as a boolean. (TRUE = text is the other way round: the text is converted, and refused if it is not a boolean)
    f"""create or replace macro {_NS}_sf_eq_bool(x, b) as case
          when typeof(x) = 'VARCHAR' then x::varchar = (case when b then 'true' when not b then 'false' end)
          else try_cast(x as boolean) = b end""",
]

_RENAMED = {  # Snowflake function -> macro, same arguments
    "width_bucket": "_sf_width_bucket",
    "jarowinkler_similarity": "_sf_jarowinkler", "object_insert": "_sf_object_insert",
    "object_delete": "_sf_object_delete", "object_pick": "_sf_object_pick",
    "current_account_name": "_sf_current_account_name", "try_base64_decode_string": "_sf_try_base64", "base64_decode_string": "_sf_base64",
    "soundex": "_sf_soundex", "parse_url": "_sf_parse_url", "parse_ip": "_sf_parse_ip",
    "get_ignore_case": "_sf_get_ignore_case", "object_keys": "_sf_object_keys", "hash_agg": "_sf_hash_agg",
    "try_hex_decode_string": "_sf_try_hex", "encrypt_raw": "_sf_encrypt_raw",
    "decrypt_raw": "_sf_decrypt_raw", "try_decrypt_raw": "_sf_try_decrypt_raw",
    "parse_xml": "_sf_parse_xml", "check_xml": "_sf_check_xml", "to_xml": "_sf_to_xml", "xmlget": "_sf_xmlget",
    "check_json": "_sf_check_json",
    **{f"is_{k}": f"_sf_is_{k}" for k in ("date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz")},
    **{f"as_{k}": f"_sf_as_{k}" for k in ("date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "double", "real",
                                          "number", "decimal", "boolean", "object", "array")}, "upper": "_sf_upper", "ucase": "_sf_upper",
    "as_integer": "_sf_as_integer", "as_varchar": "_sf_as_varchar", "as_char": "_sf_as_varchar",
    "arrays_to_object": "_sf_arrays_to_object", "arrays_zip": "_sf_arrays_zip", "try_base64_decode_binary": "_sf_try_base64_binary",
    "md5_number_lower64": "_sf_md5_lower64", "md5_number_upper64": "_sf_md5_upper64",
    "m_d5_number_lower64": "_sf_md5_lower64", "m_d5_number_upper64": "_sf_md5_upper64",
    "sha2_hex": "_sf_sha2", "sha2_binary": "_sf_sha2_binary", "array_union_agg": "_sf_array_union_agg",
    "negate": "_sf_negate", "bitcount": "_sf_bitcount", "getbit": "_sf_getbit", "array_repeat": "_sf_array_repeat",
    "binary_as_string": "_sf_binary_as_string", "string_as_binary": "_sf_string_as_binary", "jaccard_similarity": "_sf_jaccard",
    "approximate_count_distinct": "_sf_approx_count", "hll": "_sf_approx_count", "soundex_p123": "_sf_soundex",
    "try_hex_decode_binary": "_sf_try_hex_binary", "is_date_value": "_sf_is_date",
    "is_binary": "_sf_is_binary", "is_vector": "_sf_is_binary", "as_binary": "_sf_as_binary",
    "bitmap_absolute_position": "_sf_bitmap_position", "try_validate_utf8": "_sf_try_validate_utf8",
    **{f"is_{k}": f"_sf_is_{k}" for k in ("object", "array", "integer", "varchar", "char", "boolean", "decimal", "double", "real")},
}
_DATE_TOKENS = re.compile(r"YYYY|YY|MM|DD|HH|MI|SS|MON|DY|AM|PM", re.IGNORECASE)
_HASHES = (exp.MD5, exp.SHA, exp.SHA2, exp.MD5Digest)
UDFS: dict[str, str] = {}  # user-defined functions from the spec: name -> macro name
# What the session context functions answer. The role is the exception: it lives in a table
# (`USE ROLE x`, or `dbt: {role: ...}` in the spec), so policies can be exercised as different roles.
SESSION = {
    "CurrentAccount": "FAUXFLAKE", "CurrentAccountName": "FAUXFLAKE", "CurrentOrganizationName": "FAUXFLAKE",
    "CurrentRegion": "LOCAL", "CurrentWarehouse": "LOCAL", "CurrentUser": "CI",
    "CurrentSession": "1", "CurrentClient": "fauxflake", "CurrentIpAddress": "127.0.0.1",
    "CurrentAvailableRoles": '["SYSADMIN"]', "CurrentSecondaryRoles": '{"roles":"","value":""}',
    "CURRENT_ROLE_TYPE": "ROLE",
}
UDTFS: dict[str, str] = {}  # user-defined table functions: name -> table macro name
_TIMESTAMP_PARSERS = {"try_to_timestamp", "try_to_timestamp_ntz", "try_to_timestamp_ltz", "try_to_timestamp_tz",
                      "to_timestamp_ntz", "to_timestamp_ltz", "to_timestamp_tz"}
_DAY_UNITS = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "YEARS", "QUARTERS", "MONTHS", "WEEKS", "DAYS"}
_ARITHMETIC = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)
_NUMERIC_TEXT = re.compile(r"^\s*-?\d+(\.\d+)?\s*$")

_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)


def _func_args(node: exp.Func) -> list[exp.Expression]:
    out: list[exp.Expression] = []
    for key in node.arg_types:
        value = node.args.get(key)
        if isinstance(value, exp.Expression):
            out.append(value)
        elif isinstance(value, list):
            out.extend(value)
    return out


_NUMBER_FORMAT = re.compile(r"^(FM)?(S)?(\$)?([09,]+)(?:\.([09]+))?$", re.IGNORECASE)


def _number_format(value: exp.Expression, model: str) -> exp.Expression | None:
    """TO_CHAR(number, '9,999.00') as a DuckDB format() call, for the common format models."""
    if re.fullmatch(r"X+", model.strip()):  # hexadecimal
        return sqlglot.parse_one(f"format('{{:X}}', ({value.sql(dialect='duckdb')})::bigint)", read="duckdb")
    m = _NUMBER_FORMAT.match(model.strip())
    if not m:
        return None
    fill, signed, dollar, whole, frac = m.groups()
    decimals = len(frac or "")
    digits = whole.replace(",", "")
    spec = ("," if "," in whole else "") + f".{decimals}f"
    if "0" in digits and "," not in whole:  # zero padded to the width of the model
        spec = f"0{len(digits) + (decimals + 1 if decimals else 0)}.{decimals}f"
    v = f"({value.sql(dialect='duckdb')})::double"
    text = f"format('{{:{spec}}}', abs({v}))"
    if dollar:
        text = f"concat('$', {text})"
    text = f"(case when {v} < 0 then '-' else '{'+' if signed else ''}' end || {text})"
    if not fill:  # Snowflake right-aligns in the model's width, plus one position for the sign (S is that position)
        text = f"lpad({text}, {len(model.strip()) + (0 if signed else 1)}, ' ')"
    return sqlglot.parse_one(text, read="duckdb")



def _fn(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=_NS + name, expressions=list(args))


def _over(window: exp.Window | None, agg: exp.Expression) -> exp.Expression:
    if window is None:
        return agg
    w = window.copy()
    w.set("this", agg)
    return w


def _avg(avg: exp.Avg, window: exp.Window | None = None) -> exp.Expression:
    """AVG has division's scale rule, so compute it as SUM / COUNT."""
    total = _over(window, exp.Sum(this=avg.this.copy()))
    count = _over(window, exp.Count(this=avg.this.copy()))
    return exp.Case(
        ifs=[exp.If(this=exp.EQ(this=count, expression=exp.Literal.number(0)), true=exp.Null())],
        default=_fn("_sf_div", total, count.copy()),
    )


def _moment(node: exp.Skewness | exp.Kurtosis, window: exp.Window | None = None) -> exp.Expression:
    """SKEW and KURTOSIS with Snowflake's rounding and its refusal of a column with no spread."""
    x = node.this
    agg = lambda kind, arg=None: _over(window, kind(this=(arg or x).copy()))  # noqa: E731
    v, n = agg(exp.Variance), agg(exp.Count)
    if isinstance(node, exp.Kurtosis):
        return _fn("_sf_kurtosis", agg(exp.Kurtosis), v, n)
    return _fn("_sf_skew", agg(exp.Skewness), v, agg(exp.Avg, exp.cast(x.copy(), "double")), n, agg(exp.Min),
               exp.Boolean(this=window is not None))


_ARRAYS = {  # array function -> macro and the arguments it takes, in order
    exp.ArrayContains: ("_sf_array_contains", ("this", "expression")),
    exp.ArrayPosition: ("_sf_array_position", ("this", "expression")),
    exp.ArrayToString: ("_sf_array_to_string", ("this", "expression")),
    exp.ArrayOverlaps: ("_sf_arrays_overlap", ("this", "expression")),
    exp.ArraySlice: ("_sf_array_slice", ("this", "start", "end")),
    exp.ArrayConcat: ("_sf_array_cat", ("this", "expressions")),
    exp.ArrayAppend: ("_sf_array_append", ("this", "expression")),
    exp.ArrayPrepend: ("_sf_array_prepend", ("this", "expression")),
    exp.ArrayDistinct: ("_sf_array_unique", ("this",)),
    exp.SortArray: ("_sf_array_sort", ("this", "asc")),
    exp.ArrayMax: ("_sf_array_max", ("this",)),
    exp.ArrayMin: ("_sf_array_min", ("this",)),
    exp.ArrayCompact: ("_sf_array_compact", ("this",)),
    exp.ArrayIntersect: ("_sf_array_intersection", ("expressions",)),
    exp.ArrayExcept: ("_sf_array_except", ("this", "expression")),
    exp.ArrayRemove: ("_sf_array_remove", ("this", "expression")),
    exp.ArrayInsert: ("_sf_array_insert", ("this", "position", "expression")),
    exp.ArrayRemoveAt: ("_sf_array_remove_at", ("this", "position")),
    exp.Flatten: ("_sf_array_flatten", ("this",)),
    exp.ArrayReverse: ("_sf_array_reverse", ("this",)),
}
_ARRAY_FUNCTIONS = tuple(_ARRAYS)


def _is(node: exp.Expression, name: str) -> bool:
    return isinstance(node, exp.Anonymous) and node.name == _NS + name


def _average(node: exp.Expression) -> bool:
    return isinstance(node, exp.Case) and _is(node.args.get("default"), "_sf_div")


def _binary_like(node: exp.Expression) -> bool:
    """A value written as a BINARY: TO_BINARY, a cast to one, a function of binaries."""
    node = node.unnest()
    if isinstance(node, (exp.ToBinary, exp.Unhex, exp.FromBase64)) or (
            isinstance(node, exp.Cast) and node.to.this in (exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY, exp.DataType.Type.BLOB)):
        return True
    return isinstance(node, exp.Anonymous) and "binary" in str(node.name).lower()


_TEXT_MADE = ("_sf_text", "_sf_div_text", "_sf_round_div_text", "_fx_json_sorted")


def _known_text(node: exp.Expression) -> bool:
    """`node` is text as written: a string, `a || b`, CONCAT, COALESCE of text, text cast to text."""
    node = node.unnest()
    if isinstance(node, exp.Literal):
        return node.is_string
    if isinstance(node, (exp.DPipe, exp.Concat, exp.ConcatWs)) or any(_is(node, name) for name in _TEXT_MADE):
        return True
    if isinstance(node, exp.Coalesce):
        return all(_known_text(a) for a in [node.this, *node.expressions])
    if isinstance(node, exp.Cast) and node.to.is_type("varchar", "text") and not node.to.expressions:
        return _known_text(node.this)
    return False


def _text(node: exp.Expression) -> exp.Expression:
    """The value as Snowflake prints it. A division carries its scale into the text."""
    inner = node.unnest()
    if isinstance(inner, exp.Cast) and not isinstance(inner, exp.TryCast) and inner.to.is_type("varchar", "text") \
            and not inner.to.expressions:
        return _text(inner.this)  # (`x::varchar`, to be made text of x itself)
    if _known_text(inner):
        # already text: wrapping again only repeats the argument (_sf_text names it a dozen times, and nested
        # wrappings multiply: a surrogate key of eleven columns took DuckDB a minute to bind)
        return node
    if _is(inner, "_sf_div"):
        return _fn("_sf_div_text", *inner.expressions)
    if _average(inner):
        inner.set("default", _fn("_sf_div_text", *inner.args["default"].expressions))
        return inner
    if isinstance(inner, exp.Round) and isinstance(inner.args.get("decimals"), exp.Literal) and inner.args["decimals"].is_int:
        of, places = inner.this.unnest(), inner.args["decimals"]
        if _is(of, "_sf_div"):
            return _fn("_sf_round_div_text", *of.expressions, places)
        if _average(of):
            of.set("default", _fn("_sf_round_div_text", *of.args["default"].expressions, places))
            return of
    return _fn("_sf_text", node)


_OFFSET = re.compile(r"\s*(Z|[+-]\d{2}:?\d{2})$")
_SPLIT_TO_TABLE = ("(select 1 as seq, unnest(range(1, len(string_split(__s, __d)) + 1)) as index, "
                   "unnest(string_split(__s, __d)) as value)")


def _split_to_table(call: exp.SplitToTable) -> exp.Expression:
    body = sqlglot.parse_one(_SPLIT_TO_TABLE, read="duckdb")
    for split in list(body.find_all(exp.Split)):  # keep it a DuckDB list: a Snowflake SPLIT is an ARRAY
        split.replace(exp.Anonymous(this="string_split", expressions=[split.this, split.expression]))
    for col in list(body.find_all(exp.Column)):
        if col.name in ("__s", "__d"):
            col.replace((call.this if col.name == "__s" else call.expression).copy())
    return body


def _dynamic_pivot(pivot: exp.Pivot) -> exp.Expression | None:
    """PIVOT (agg FOR col IN (ANY)) names its columns from the data: DuckDB's PIVOT ... ON form."""
    field = pivot.args.get("fields") and pivot.args["fields"][0]
    if not isinstance(field, exp.In) or not any(isinstance(e, exp.PivotAny) for e in field.expressions):
        return None
    source = pivot.parent
    if not isinstance(source, (exp.Table, exp.Subquery)) or len(pivot.expressions) != 1:
        return None
    inner = source.copy()
    inner.set("pivots", None)
    inner.set("alias", None)
    if isinstance(inner, exp.Table):
        inner = exp.select("*").from_(inner).subquery()
    out = exp.Subquery(this=exp.Pivot(this=inner, expressions=[field.this.copy()], using=[pivot.expressions[0].copy()], unpivot=False))
    alias = pivot.args.get("alias") or source.args.get("alias")
    if alias:
        out.set("alias", alias.copy())
    return out


def _operand(node: exp.Expression) -> exp.Expression:
    """Snowflake converts these implicitly inside arithmetic."""
    if isinstance(node, exp.Literal) and node.is_string and _NUMERIC_TEXT.match(node.name):
        return exp.Literal.number(node.name.strip())  # '1' + 1
    if isinstance(node, exp.JSONExtract):
        return exp.cast(node, "double")  # v:n + 1
    return node


_VARIANT_ACCESS = (exp.JSONExtract, exp.Bracket)
_STRING_FUNCTIONS = (exp.Right, exp.Left, exp.Substring, exp.Upper, exp.Lower, exp.Trim, exp.Length, exp.RegexpReplace,
                     exp.RegexpExtract, exp.RegexpLike, exp.SplitPart, exp.Split, exp.StartsWith, exp.EndsWith, exp.Contains,
                     exp.Initcap, exp.Pad, exp.StrPosition, exp.Concat, exp.ConcatWs, exp.DPipe, exp.Reverse, exp.Repeat,
                     exp.RegexpExtractAll, exp.RegexpCount, exp.Replace)
_REGEX_FUNCTIONS = (exp.RegexpExtract, exp.RegexpExtractAll, exp.RegexpCount, exp.RegexpLike, exp.RegexpReplace, exp.RegexpInstr)
_MAPS = {"MAP_KEYS": "", "MAP_SIZE": "", "MAP_CONTAINS_KEY": "", "MAP_DELETE": "OBJECT_DELETE", "MAP_INSERT": "OBJECT_INSERT",
         "MAP_PICK": "OBJECT_PICK"}


def _collation(spec: str) -> str | None:
    """A Snowflake collation spec ('en-ci', 'en-ci-ai') as DuckDB's NOCASE / NOACCENT."""
    parts = spec.lower().replace("_", "-").split("-")
    names = [name for flag, name in (("ci", "NOCASE"), ("ai", "NOACCENT")) if flag in parts]
    return ".".join(names) or None


_TRY_SAME = {"DATE": ("DATE",), "BOOLEAN": ("BOOLEAN",), "DOUBLE": ("DOUBLE", "FLOAT"), "BLOB": ("BLOB",)}


def _whole(to: exp.DataType) -> bool:
    """NUMBER(38,0) by any of its names (INTEGER, BIGINT, NUMBER, DECIMAL(38, 0)): what a whole-number column already is.
    TRY_CAST takes a value of exactly the target type, so of these only whole numbers pass (measured on Snowflake)."""
    T = exp.DataType.Type
    if to.this in (T.INT, T.BIGINT, T.SMALLINT, T.TINYINT):
        return True
    if to.this in (T.DECIMAL, T.BIGDECIMAL):
        sizes = [p.name for p in to.expressions]
        return sizes in ([], ["38"], ["38", "0"])
    return False


def _try_target(node: exp.Expression) -> str | None:
    """The DuckDB type a TRY_CAST or TRY_TO_ function of Snowflake converts to, or None for anything else."""
    if isinstance(node, exp.TryCast) and node.args.get("requires_string"):
        kind = node.to.this
        if _whole(node.to):
            return "WHOLE"
        return {exp.DataType.Type.DATE: "DATE", exp.DataType.Type.BOOLEAN: "BOOLEAN", exp.DataType.Type.DOUBLE: "DOUBLE",
                exp.DataType.Type.FLOAT: "DOUBLE", exp.DataType.Type.BINARY: "BLOB", exp.DataType.Type.VARBINARY: "BLOB"}.get(kind, "OTHER")
    if not node.args.get("safe"):
        return None
    if isinstance(node, exp.ToNumber):
        precision, scale = node.args.get("precision"), node.args.get("scale")
        plain = node.args.get("format") is None and (precision is None or precision.name == "38") and (scale is None or scale.name == "0")
        return "WHOLE" if plain else "OTHER"
    if isinstance(node, exp.TsOrDsToDate):
        return "DATE"
    if isinstance(node, exp.ToBoolean):
        return "BOOLEAN"
    if isinstance(node, exp.ToDouble):
        return "DOUBLE"
    if isinstance(node, (exp.TsOrDsToTime, exp.StrToTime)):
        return "OTHER"
    return None


def _try_anonymous_target(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Anonymous) and isinstance(node.this, str) and re.fullmatch(
            r"TRY_TO_(TIMESTAMP(_NTZ|_LTZ|_TZ)?|TIME|NUMBER|NUMERIC|DECIMAL)", node.name.upper()):
        return "WHOLE" if re.fullmatch(r"TRY_TO_(NUMBER|NUMERIC|DECIMAL)", node.name.upper()) and len(node.expressions) == 1 else "OTHER"
    return None


ZONED_ARGUMENT = "_fx_zoned_argument"  # (a TIMESTAMP_TZ given to CONVERT_TIMEZONE's three-argument form)


def _maybe_text(node: exp.Expression) -> bool:
    """A side of a comparison with TRUE or FALSE that may be text: not a boolean, a number or a VARIANT as written."""
    node = node.unnest()
    return not isinstance(node, (exp.Boolean, exp.Null, exp.JSONExtract, exp.Bracket, exp.Predicate, exp.Not, exp.Connector)) \
        and not (isinstance(node, exp.Literal) and not node.is_string)


def _computed_index(node: exp.Expression) -> bool:
    """`v[i]` with an index that is not written out: a column, an expression (not a slice)."""
    return isinstance(node, exp.Bracket) and len(node.expressions) == 1 and not isinstance(
        node.expressions[0], (exp.Literal, exp.Slice, exp.Null)) and not (
        isinstance(node.expressions[0], exp.Neg) and isinstance(node.expressions[0].this, exp.Literal))


def _zoned(value: exp.Expression | None) -> bool:
    """`value` is a TIMESTAMP_TZ by how it is written: `x::timestamp_tz`, TO_TIMESTAMP_TZ(x), CONVERT_TIMEZONE(zone, x)."""
    if isinstance(value, exp.Cast) and not isinstance(value, exp.TryCast):
        return value.to.is_type("timestamptz")
    if isinstance(value, exp.ConvertTimezone):
        return value.args.get("source_tz") is None
    return isinstance(value, exp.Anonymous) and str(value.name).lower() == "to_timestamp_tz"


def zoned_argument(transformed: exp.Expression) -> tuple[int, str] | None:
    """The error Snowflake gives a statement that passes a TIMESTAMP_TZ to CONVERT_TIMEZONE(from, to, x), or None."""
    if any(str(f.name).split(".")[-1].lower() == ZONED_ARGUMENT for f in transformed.find_all(exp.Anonymous)):
        return 1044, "SQL compilation error:\nInvalid argument types for function 'CONVERT_TIMEZONE'"
    return None


_CLOCK_CALLS = {exp.CurrentDate: "_fx_current_date", exp.CurrentTimestamp: "_fx_current_timestamp",
                exp.CurrentTime: "_fx_current_time", exp.Localtime: "_fx_localtime", exp.Localtimestamp: "_fx_localtimestamp"}


def _secure_with(create: exp.Create) -> bool:
    props = create.args.get("properties")
    body = create.expression.unnest() if create.expression is not None else None
    return bool(props and any(isinstance(p, exp.SecureProperty) for p in props.expressions)) \
        and isinstance(body, exp.Query) and body.args.get("with_") is not None


def _fenced(body: exp.Expression | None) -> bool:
    """The body a secure view that is a WITH is made with (see _fix)."""
    return isinstance(body, exp.Select) and isinstance(body.args.get("offset"), exp.Offset) \
        and body.args["offset"].expression.name == "0" and isinstance(body.args.get("from_"), exp.From) \
        and isinstance(body.args["from_"].this, exp.Subquery) and body.args["from_"].this.this.args.get("with_") is not None


def _fix(node: exp.Expression) -> exp.Expression:
    if type(node) in _CLOCK_CALLS and not (isinstance(node, exp.CurrentDate) and node.this is not None):
        # as calls: `select current_date` is named CURRENT_DATE, and DuckDB reads the bare keyword as that name
        # (macros, which sqlglot does not turn back into the keyword when the statement is parsed again)
        return _fn(_CLOCK_CALLS[type(node)])
    if isinstance(node, exp.Create) and node.kind == "VIEW" and _secure_with(node):
        # measured: a filter goes no further into a secure view that is a WITH, so its body is evaluated whole (and
        # fails on what the filter would have kept out). DuckDB pushes the filter in, but not past an OFFSET; and it
        # drops a filter its statistics say keeps nothing (`is null` of a column with none), but not of a column
        # whose values it cannot foresee (a CASE on random(), always true: each column as it is, by its own name)
        fence = sqlglot.parse_one("CASE WHEN RANDOM() < 2 THEN COLUMNS(*) END", read="duckdb")
        node.set("expression", exp.select(fence).from_(exp.Subquery(this=node.expression.unnest().copy())).offset(0))
        return node
    target = _try_target(node) or _try_anonymous_target(node)
    if target and not (isinstance(node.this, exp.Anonymous) and node.this.name == _NS + "_sf_try_input"):
        # TRY_CAST and the TRY_TO_ functions take text (or the target type itself, for a few): anything else is an error
        # in Snowflake. The value passes through unchanged; another kind of value raises Snowflake's error
        arg = node.this if not isinstance(node, exp.Anonymous) else (node.expressions[0] if node.expressions else None)
        if arg is not None and not (isinstance(arg, exp.Literal) and arg.is_string) and not isinstance(arg, exp.Null):
            checked = _fn("_sf_try_input", arg.copy(), exp.Literal.string(target))
            if isinstance(node, exp.Anonymous):
                node.set("expressions", [checked, *node.expressions[1:]])
            else:
                node.set("this", checked)
    if isinstance(node, exp.Collate) and isinstance(node.expression, (exp.Literal, exp.Var, exp.Identifier)):
        name = _collation(node.expression.name)
        if name is None:
            return node.this
        node.set("expression", exp.var(name))
        return node
    if isinstance(node, exp.CollateColumnConstraint) and isinstance(node.this, (exp.Literal, exp.Var, exp.Identifier)):
        node.set("this", exp.var(_collation(node.this.name) or "BINARY"))
        return node
    if isinstance(node, exp.ConvertTimezone) and node.args.get("source_tz") is None and node.args.get("timestamp") is not None:
        # CONVERT_TIMEZONE(zone, x) takes a TIMESTAMP_TZ: x without a zone is read in the session's (UTC here), not as
        # the wall clock of `zone`. Its answer keeps the instant, so another conversion of it starts from that instant
        value = node.args["timestamp"]
        while isinstance(value, exp.ConvertTimezone) and value.args.get("source_tz") is None and value.args.get("timestamp") is not None:
            value = value.args["timestamp"]
        if not (isinstance(value, exp.Cast) and value.to.is_type("timestamptz", "timestampltz")):
            value = exp.cast(value.copy(), "timestamptz")
        node.set("timestamp", value)
    if isinstance(node, exp.ConvertTimezone) and node.args.get("source_tz") is not None and _zoned(node.args.get("timestamp")):
        # CONVERT_TIMEZONE(from, to, x) takes a timestamp without a zone: a TIMESTAMP_TZ is refused when the statement
        # compiles (a TIMESTAMP_LTZ is not). DuckDB holds both as one type: marked here, refused before it runs
        node.set("timestamp", _fn(ZONED_ARGUMENT, node.args["timestamp"]))
    if isinstance(node, exp.Interval) and isinstance(node.this, exp.Literal) and "," in node.this.name:
        node.this.set("this", node.this.name.replace(",", " "))  # INTERVAL '1 day, 2 hours'
    if isinstance(node, (exp.Mul, exp.Add, exp.Sub)) and all(
            isinstance(side, exp.Literal) and side.is_int for side in (node.this, node.expression)):
        node.set("this", exp.cast(node.this, "bigint"))  # 86400 * 30000 does not overflow a NUMBER
    if isinstance(node, exp.Select) and node.args.get("from_") is None and any(isinstance(e, exp.Star) for e in node.expressions):
        # `select *` with nothing to select from is one row with one NULL column, COLUMN1, in Snowflake
        node.set("expressions", [exp.alias_(exp.cast(exp.Null(), "varchar"), "COLUMN1") if isinstance(e, exp.Star) else e
                                 for e in node.expressions])
    if isinstance(node, exp.Coalesce):
        args = [node.this, *node.expressions]
        if any(isinstance(a, exp.Literal) and a.is_string for a in args) and any(isinstance(a, _VARIANT_ACCESS) for a in args):
            # coalesce(v:name, 'none') is text in Snowflake: the VARIANT gives its value, without quotes
            node.set("this", _fn("_sf_text", node.this) if isinstance(node.this, _VARIANT_ACCESS) else node.this)
            node.set("expressions", [_fn("_sf_text", a) if isinstance(a, _VARIANT_ACCESS) else a for a in node.expressions])
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast):
        to, inner = node.to, node.this
        if to.is_type("timestamp", "timestampntz") and isinstance(inner, exp.Cast) and inner.to.is_type("timestamptz") \
                and isinstance(inner.this, exp.Literal) and inner.this.is_string:
            # a TIMESTAMP_TZ keeps its wall-clock time when the zone is dropped
            return exp.cast(exp.Literal.string(_OFFSET.sub("", inner.this.name)), "timestamp")
        if to.is_type("timestamp", "timestampntz", "timestampltz", "timestamptz") and to.expressions \
                and to.expressions[0].name == "0":
            return exp.Anonymous(this="date_trunc", expressions=[exp.Literal.string("second"), node])
        if to.is_type("timestamp", "timestampntz", "timestampltz", "timestamptz") and (_is(inner, "_sf_int") or (
                isinstance(inner, exp.Cast) and inner.to.is_type("int", "bigint", "decimal", "double", "float"))):
            return exp.Cast(this=_fn("_sf_seconds", inner), to=to)  # x::number::timestamp: seconds since 1970
        if to.is_type("boolean") and not isinstance(inner, (*_VARIANT_ACCESS, exp.Boolean, exp.Null)):
            return _fn("_sf_bool", inner)
        if to.is_type("date") and not isinstance(inner, (*_VARIANT_ACCESS, exp.Null, exp.Cast, exp.Anonymous)) \
                and not (isinstance(inner, exp.Literal) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", inner.name)):
            return _fn("_sf_to_date", inner)
        whole = to.is_type("int", "bigint", "smallint", "tinyint") or (
            to.is_type("decimal") and (not to.expressions or (len(to.expressions) == 2 and to.expressions[1].name == "0")
                                       or len(to.expressions) == 1))
        if whole and not isinstance(inner, (*_VARIANT_ACCESS, exp.Literal, exp.Null, exp.Boolean)):
            return _fn("_sf_int", inner)
        if to.is_type("variant") and not isinstance(inner, exp.Null):
            return exp.ToVariant(this=inner)  # 'a'::variant is the string a, not JSON text to parse
        if to.is_type("map", "object") and not isinstance(inner, exp.Null):
            # a MAP or OBJECT is held as JSON, like every semi-structured value; a VARIANT that holds something else
            # than an object is an error, JSON null is NULL
            return _fn("_sf_to_object", inner)
    if isinstance(node, exp.TryCast) and node.to.is_type("date") and not isinstance(node.this, _VARIANT_ACCESS):
        return _fn("_sf_try_date", node.this)
    if isinstance(node, exp.TsOrDsToDate) and not node.args.get("format") and not isinstance(node.this, _VARIANT_ACCESS):
        return _fn("_sf_try_date" if node.args.get("safe") else "_sf_to_date", node.this)
    if isinstance(node, exp.TsOrDsToTime) and node.args.get("safe") and not node.args.get("format"):
        return _fn("_sf_try_time", node.this)
    if isinstance(node, exp.ToBoolean):
        return _fn("_sf_try_bool" if node.args.get("safe") else "_sf_bool", node.this)
    if isinstance(node, exp.DateFromParts):
        return _fn("_sf_date_from_parts", node.args["year"], node.args["month"], node.args["day"])
    if isinstance(node, exp.TimestampFromParts):
        parts = [node.args.get(k) for k in ("year", "month", "day", "hour", "min", "sec", "nano")]
        if node.args.get("year") is None:
            return _fn("_sf_timestamp_from_parts", node.this, node.expression)
        return _fn("_sf_timestamp_from_parts", *[p for p in parts if p is not None])
    if isinstance(node, exp.ByteLength):
        return exp.Anonymous(this="strlen", expressions=[exp.cast(node.this, "varchar")])
    if isinstance(node, exp.RegexpReplace) and (node.args.get("position") or node.args.get("occurrence")):
        position = node.args.pop("position", None) or exp.Literal.number(1)
        occurrence = node.args.pop("occurrence", None) or exp.Literal.number(0)
        if not (position.name == "1" and occurrence.name == "0"):
            return _fn("_sf_regexp_replace_nth", node.this, node.expression,
                       node.args.get("replacement") or exp.Literal.string(""), position, occurrence)
    if isinstance(node, _STRING_FUNCTIONS):
        # RIGHT(v:a, 1), UPPER(SPLIT(s, '.')[0]): a string function reads a VARIANT as its text, without JSON quotes
        for key in ("this", "expression"):
            arg = node.args.get(key)
            if isinstance(arg, _VARIANT_ACCESS) and not (key == "expression" and isinstance(node, (exp.Split, exp.SplitPart))):
                node.set(key, _text(arg))
        if isinstance(node, (exp.Concat, exp.ConcatWs)):
            node.set("expressions", [_text(e) if isinstance(e, _VARIANT_ACCESS) else e for e in node.expressions])
        elif isinstance(node.this, exp.Column) and not isinstance(node, exp.DPipe):
            # a column may be a VARIANT (FLATTEN's VALUE, or one carried through a table): SPLIT(value, ' ') and
            # REPLACE(value, 'EVENT_JSON:') read its text, without JSON quotes. A number is read as its text too.
            node.set("this", _fn("_sf_text", node.this))
    if isinstance(node, exp.Randstr):
        # sqlglot writes RANDSTR as a correlated subquery, which the planner chokes on once it is repeated
        seed = node.args.get("generator") or exp.Rand()  # (RANDOM() is a 64-bit integer, a seed as any other)
        return _fn("_sf_randstr", node.this, exp.cast(seed, "hugeint"))
    if isinstance(node, exp.MapCat):  # keys of the second map win
        return exp.Anonymous(this="json_merge_patch", expressions=[exp.cast(node.this, "json"), exp.cast(node.expression, "json")])
    name = node.sql_name().upper() if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous) else \
        node.name.upper() if isinstance(node, exp.Anonymous) and isinstance(node.this, str) else ""
    if name.startswith("MAP_") and name in _MAPS:
        args = _func_args(node) if not isinstance(node, exp.Anonymous) else list(node.expressions)
        if name == "MAP_KEYS":
            return _fn("_sf_object_keys", args[0])
        if name == "MAP_SIZE":
            return exp.Anonymous(this="len", expressions=[exp.Anonymous(this="json_keys", expressions=[exp.cast(args[0], "json")])])
        if name == "MAP_CONTAINS_KEY":
            return exp.Anonymous(this="list_contains", expressions=[
                exp.Anonymous(this="json_keys", expressions=[exp.cast(args[1], "json")]), exp.cast(args[0], "varchar")])
        name = _MAPS[name]  # a MAP is an OBJECT here: same operation
    if name in ("OBJECT_DELETE", "OBJECT_INSERT", "OBJECT_PICK"):
        args = _func_args(node) if not isinstance(node, exp.Anonymous) else list(node.expressions)
        plain_fn = lambda fn, *a: exp.Anonymous(this=fn, expressions=list(a))  # noqa: E731
        target = exp.cast(args[0], "json")
        if name == "OBJECT_DELETE":  # merging a null deletes the key
            gone = [x for key in args[1:] for x in (key, exp.Null())]
            return plain_fn("json_merge_patch", target, plain_fn("json_object", *gone))
        if name == "OBJECT_INSERT":
            return plain_fn("json_merge_patch", target, plain_fn("json_object", args[1], exp.ToVariant(this=args[2])))
        keys = args[1] if len(args) == 2 and not (isinstance(args[1], exp.Literal) and args[1].is_string) else plain_fn("list_value", *args[1:])
        return _fn("_sf_object_pick", target, keys)
    if isinstance(node, exp.Struct) and not node.expressions:
        return exp.cast(exp.Literal.string("{}"), "json")
    if isinstance(node, exp.Struct) and node.expressions and all(isinstance(e, exp.PropertyEQ) for e in node.expressions):
        pairs = [x for e in node.expressions for x in (exp.Literal.string(e.this.name), e.expression)]
        return exp.func("OBJECT_CONSTRUCT", *pairs, dialect="snowflake")
    if isinstance(node, exp.Anonymous) and node.name.lower() == "to_object" and len(node.expressions) == 1:
        return node.expressions[0]
    if isinstance(node, (exp.Sum, exp.Avg)) and isinstance(node.this, _VARIANT_ACCESS):
        node.set("this", exp.cast(node.this, "double"))  # SUM(v:n) adds the numbers inside
    if isinstance(node, exp.Parameter) and isinstance(node.this, exp.Literal) and node.this.is_int:
        return exp.column(f"column{node.this.name}")  # $1 is the first column of a VALUES list
    if isinstance(node, exp.Count) and node.expressions and not isinstance(node.this, exp.Distinct):
        present = exp.and_(*[exp.Is(this=c.copy(), expression=exp.Null()).not_() for c in (node.this, *node.expressions)])
        return exp.Count(this=exp.Case(ifs=[exp.If(this=present, true=exp.Literal.number(1))]))
    if isinstance(node, exp.ToChar) and not node.args.get("format"):
        return _text(node.this)
    if isinstance(node, (exp.ArrayAgg, exp.ObjectAgg)) or (isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.ArrayAgg)):
        if not isinstance(node.parent, (exp.Window, exp.WithinGroup)):
            empty = "{}" if isinstance(node, exp.ObjectAgg) else "[]"  # no rows give an empty value, not NULL
            return exp.Coalesce(this=node, expressions=[exp.ParseJSON(this=exp.Literal.string(empty))])
    if isinstance(node, exp.Window) and (isinstance(node.this, exp.ArrayAgg) or (
            isinstance(node.this, exp.WithinGroup) and isinstance(node.this.this, exp.ArrayAgg))) \
            and not isinstance(node.parent, exp.Coalesce):
        return exp.Coalesce(this=node, expressions=[exp.ParseJSON(this=exp.Literal.string("[]"))])  # also over a window
    if (isinstance(node, exp.ArrayUniqueAgg) and not isinstance(node.parent, exp.Window)) or (
            isinstance(node, exp.Window) and isinstance(node.this, exp.ArrayUniqueAgg)):
        if not isinstance(node.parent, exp.Coalesce):
            return exp.Coalesce(this=node, expressions=[exp.Array(expressions=[])])  # only NULLs: an empty array
    if isinstance(node, exp.Lateral) and isinstance(node.this, exp.SplitToTable):
        node.set("this", _split_to_table(node.this))
        return node
    if isinstance(node, exp.Pivot):
        dynamic = _dynamic_pivot(node)
        if dynamic is not None:
            node.parent.replace(dynamic)
            return node
    if isinstance(node, _ARRAY_FUNCTIONS):
        name, keys = _ARRAYS[type(node)]
        args = [a for k in keys for a in (node.args.get(k) if isinstance(node.args.get(k), list) else [node.args.get(k)])]
        return _fn(name, *[a for a in args if a is not None])
    if isinstance(node, exp.GenerateSeries) and node.args.get("is_end_exclusive"):
        return _fn("_sf_array_range", *[node.args[k] for k in ("start", "end", "step") if node.args.get(k) is not None])
    if isinstance(node, exp.Array) and not isinstance(node.parent, exp.Anonymous):
        # an ARRAY is a VARIANT value wherever it appears: build it as JSON, element by element,
        # so arrays from different branches of a CASE or UNION have one type
        node.set("expressions", [e if isinstance(e, exp.Null) else exp.ToVariant(this=e) for e in node.expressions])
        return exp.Anonymous(this="to_json", expressions=[node])
    if isinstance(node, (exp.ArrayFilter, exp.Transform, exp.Reduce)):
        node.set("this", _fn("_sf_list", node.this))  # lambdas walk a list; the array itself is JSON
    if isinstance(node, exp.ParseJSON) and not isinstance(node.this, (exp.Literal, exp.Null)) \
            and not (isinstance(node.this, exp.Anonymous) and node.this.name == "_fxpy_json_text"):
        # (as text the way a cast gives it: a VARIANT that holds a string gives the string, not its JSON spelling
        # with quotes, so PARSE_JSON of `"[1, 2]"` held in a VARIANT is the list), read as Snowflake reads JSON
        text = _fn("_sf_text", node.this)
        node.set("this", exp.Anonymous(this="_fxpy_json_text", expressions=[text, exp.Boolean(this=bool(node.args.get("safe")))]))
    elif isinstance(node, exp.ParseJSON) and isinstance(node.this, exp.Literal) and node.this.is_string:
        try:  # a literal: read here, once
            read = jsondoc.normalise(node.this.name)
        except jsondoc.JSONError:  # (refused where it is read: Snowflake's error, or NULL for TRY_)
            node.set("this", exp.Anonymous(this="_fxpy_json_text", expressions=[node.this.copy(), exp.Boolean(this=bool(node.args.get("safe")))]))
            return node
        if read is None:
            return exp.Null()
        if read != node.this.name:
            node.set("this", exp.Literal.string(read))
    if isinstance(node, exp.Bracket) and len(node.expressions) == 1 and isinstance(node.expressions[0], exp.Literal):
        # v['a']['b'][0] is the path v:a.b[0]. Written as a path, every step stays a VARIANT access
        key, base = node.expressions[0], node.this
        inside = isinstance(base, exp.JSONExtract) and base.args.get("variant_extract")
        if key.is_string or (key.is_int and (inside or isinstance(base, exp.ParseJSON))):
            step = exp.JSONPathKey(this=key.name, quoted=True) if key.is_string else exp.JSONPathSubscript(this=int(key.name))
            if inside:
                base.args["expression"].append("expressions", step)
                return base
            return exp.JSONExtract(this=base, expression=exp.JSONPath(expressions=[exp.JSONPathRoot(), step]),
                                   variant_extract=True, requires_json=True)
    if _computed_index(node):
        return _fn("_sf_element", node.this, node.expressions[0])  # arr[array_size(arr) - 1], obj[key_column]
    if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Identifier):
        # v[0].name: a field of an array element. It is a path step, not a struct field
        base, key = node.this, exp.JSONPathKey(this=node.expression.name, quoted=True)
        if _computed_index(base) or (isinstance(base, exp.Anonymous) and str(base.name).lower().endswith("_sf_element")):
            return exp.JSONExtract(this=base, expression=exp.JSONPath(expressions=[exp.JSONPathRoot(), key]),
                                   variant_extract=True, requires_json=True)
        if isinstance(base, exp.JSONExtract) and base.args.get("variant_extract"):
            base.args["expression"].append("expressions", key)
            return base
        if isinstance(base, exp.Bracket) and len(base.expressions) == 1 and isinstance(base.expressions[0], exp.Literal):
            index = base.expressions[0]
            first = exp.JSONPathKey(this=index.name, quoted=True) if index.is_string else exp.JSONPathSubscript(this=int(index.name))
            return exp.JSONExtract(this=base.this, expression=exp.JSONPath(expressions=[exp.JSONPathRoot(), first, key]),
                                   variant_extract=True, requires_json=True)
    if isinstance(node, exp.Nullif) and isinstance(node.this, _VARIANT_ACCESS) \
            and isinstance(node.expression, exp.Literal) and node.expression.is_string:
        node.set("this", exp.cast(node.this, "varchar"))  # NULLIF(v:x, '') compares the value, not JSON text
    if isinstance(node, exp.ToArray):
        return _fn("_sf_to_array", node.this)
    if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.ARRAY and not node.to.expressions \
            and not isinstance(node.this, (exp.Array, exp.Null)):
        return _fn("_sf_to_array", node.this)  # x::array wraps a value that is not a list, as TO_ARRAY does
    if isinstance(node, exp.Uniform):
        # UNIFORM(lo, hi, RANDOM()): written out here so it also works inside CTEs and views
        gen = node.args.get("gen")
        unit = exp.Anonymous(this="random") if isinstance(gen, exp.Rand) or gen is None else \
            exp.Anonymous(this="_fxpy_seeded_unit", expressions=[exp.cast(gen.copy(), "hugeint")])  # (Snowflake's, for a seed)
        lo, hi = node.this, node.expression
        real = any(isinstance(b, exp.Cast) and b.to.is_type("double", "float") or (isinstance(b, exp.Literal) and "." in b.name)
                   for b in (lo, hi))
        if real:
            return exp.paren(exp.Add(this=lo, expression=exp.Mul(this=unit, expression=exp.paren(exp.Sub(this=hi, expression=lo.copy())))))
        span = exp.paren(exp.Add(this=exp.Sub(this=hi, expression=lo.copy()), expression=exp.Literal.number(1)))
        return exp.cast(exp.Floor(this=exp.Add(this=lo, expression=exp.Mul(this=unit, expression=span))), "bigint")
    if isinstance(node, exp.JSONKeys):
        return _fn("_sf_object_keys", node.this)
    if isinstance(node, exp.GetExtract):
        # GET(v, key) is v[key]; as a bracket it gets the same handling as every other access
        bracket = sqlglot.parse_one("x[0]", read="snowflake")
        bracket.set("this", node.this)
        bracket.set("expressions", [node.expression])
        return _fn("_sf_element", node.this, node.expression) if _computed_index(bracket) else bracket
    if isinstance(node, exp.Literal) and not node.is_string and "." in node.name and "e" not in node.name.lower():
        # 1.50 is NUMBER(2,1) in Snowflake: trailing zeros are not part of a literal's scale
        trimmed = node.name.rstrip("0").rstrip(".")
        return exp.Literal.number(trimmed or "0") if trimmed != node.name else node
    if isinstance(node, _ARITHMETIC):
        node.set("this", _operand(node.this))
        node.set("expression", _operand(node.expression))
    if isinstance(node, (exp.Concat, exp.ConcatWs)) and not any(_binary_like(e) for e in node.expressions):
        # CONCAT(x, ...) writes each value as Snowflake prints it, as || does (a FLOAT with its 10 digits); CONCAT of
        # binaries is a binary, left as it is
        node.set("expressions", [e if (isinstance(e, exp.Literal) and e.is_string) or isinstance(e, (exp.JSONExtract, exp.Null))
                                 else _text(e) for e in node.expressions])
    if isinstance(node, exp.GroupConcat) and not isinstance(node.this, exp.Distinct):
        # LISTAGG(x) too (not LISTAGG(DISTINCT x) ... WITHIN GROUP (ORDER BY x): DuckDB wants the same x in both)
        inner = node.this
        if isinstance(inner, exp.Order):
            if inner.this is not None and not isinstance(inner.this, exp.Distinct) \
                    and not (isinstance(inner.this, exp.Literal) and inner.this.is_string):
                inner.set("this", _text(inner.this))
        elif inner is not None and not (isinstance(inner, exp.Literal) and inner.is_string):
            node.set("this", _text(inner))
    if isinstance(node, exp.DPipe):
        for side in ("this", "expression"):
            part = node.args[side]
            if isinstance(part, exp.JSONExtract):
                node.set(side, exp.cast(part, "varchar"))  # v:s || 'y'
            elif not (isinstance(part, exp.Literal) and part.is_string) and not isinstance(part, exp.DPipe):
                node.set(side, _text(part))
    if isinstance(node, (exp.EQ, exp.NEQ)) and isinstance(node.expression, exp.Boolean) and _maybe_text(node.this):
        same = _fn("_sf_eq_bool", node.this, node.expression)  # is_success = TRUE, is_success a VARCHAR
        return same if isinstance(node, exp.EQ) else exp.Not(this=same)
    if isinstance(node, exp.In) and node.expressions and not node.args.get("query") and _maybe_text(node.this) \
            and all(isinstance(e, exp.Boolean) for e in node.expressions):
        tests = [_fn("_sf_eq_bool", node.this.copy(), e.copy()) for e in node.expressions]  # x IN (TRUE)
        return exp.paren(tests[0] if len(tests) == 1 else exp.or_(*tests))
    if isinstance(node, (exp.EQ, exp.NEQ)) and node.find_ancestor(exp.Select) is not None \
            and node.find_ancestor(exp.Merge, exp.Update, exp.Delete) is None:
        left, right = node.this.unnest(), node.expression.unnest()
        join = node.find_ancestor(exp.Join)
        asof = join is not None and (join.args.get("match_condition") is not None or "ASOF" in str(join.args.get("method") or "").upper())
        fixed = (exp.Boolean, exp.Null)  # sides that cannot be a VARIANT and need no care
        skip = any(isinstance(x, fixed) or (isinstance(x, exp.Literal) and not x.is_string) or x.find(exp.Select) is not None
                   for x in (left, right))
        if (isinstance(left, exp.Column) or isinstance(right, exp.Column)) and not skip and not asof:
            same = _fn("_sf_eq", node.this, node.expression)
            return same if isinstance(node, exp.EQ) else exp.Not(this=same)
    if isinstance(node, exp.Nullif) and isinstance(node.this, exp.Column) and isinstance(node.expression, exp.Literal) \
            and node.expression.is_string and node.find_ancestor(exp.Select) is not None:
        # nullif(col, ''): a VARIANT column is compared by its value (DuckDB would read the '' as a JSON document)
        return exp.Case(ifs=[exp.If(this=_fn("_sf_eq", node.this.copy(), node.expression.copy()), true=exp.Null())], default=node.this.copy())
    if isinstance(node, exp.In) and isinstance(node.this, exp.Column) and node.expressions and not node.args.get("query") \
            and all(isinstance(e, exp.Literal) and e.is_string for e in node.expressions) and any(e.name == "" for e in node.expressions):
        node.set("this", _fn("_sf_text", node.this))  # col in ('', 'x'): by its text, whatever the column holds
    if isinstance(node, _COMPARISONS) or isinstance(node, exp.In):
        other = node.expression if isinstance(node, _COMPARISONS) else (node.expressions[0] if node.expressions else None)
        if isinstance(node.this, _VARIANT_ACCESS) and isinstance(other, exp.Literal) and other.is_string:
            node.set("this", exp.cast(node.this, "varchar"))  # v:s = 'x' compares the value, not JSON text
    if isinstance(node, (exp.Like, exp.ILike)) and isinstance(node.this, _VARIANT_ACCESS):
        node.set("this", exp.cast(node.this, "varchar"))
    elif isinstance(node, (exp.Like, exp.ILike)) and isinstance(node.this, exp.Column):
        node.set("this", _fn("_sf_text", node.this))  # a VARIANT column matches by its text, without JSON quotes
    if isinstance(node, exp.TryCast) and node.to.is_type("timestamp", "timestampntz") and node.args.get("requires_string"):
        return _fn("_sf_try_timestamp", node.this)
    # (TO_TIMESTAMP('1735689600123'): text of digits, read by its size as seconds or milliseconds, in _fs_to_timestamp)
    if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_DATE" and len(node.expressions) == 1 \
            and isinstance(node.expressions[0], exp.Literal) and re.fullmatch(r"\d{8,}", node.expressions[0].name):
        return exp.cast(exp.UnixToTime(this=exp.Literal.number(node.expressions[0].name)), "date")
    if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
        renamed = next((_RENAMED[n.lower()] for n in type(node).sql_names() if n.lower() in _RENAMED), None)
        if renamed == "_sf_decrypt_raw" and node.args.get("safe"):
            renamed = "_sf_try_decrypt_raw"  # (sqlglot reads TRY_DECRYPT_RAW as DECRYPT_RAW, marked safe)
        if renamed:
            return _fn(renamed, *_func_args(node))
    if isinstance(node, exp.SHA2) and isinstance(node.args.get("length"), exp.Literal) and node.args["length"].name not in ("256", ""):
        return _fn("_sf_sha2", node.this, node.args["length"])  # sha2(x, 512): DuckDB has 256 only
    if isinstance(node, exp.Anonymous) and isinstance(node.this, str) and node.name.lower() in _RENAMED:
        return _fn(_RENAMED[node.name.lower()], *node.expressions)  # (a function sqlglot has no class for)
    if isinstance(node, exp.JSONFormat) and not node.args.get("options") \
            and not (isinstance(node.parent, exp.Anonymous) and node.parent.name == _NS + "_fx_json_sorted"):
        return _fn("_fx_json_sorted", node)  # TO_JSON: an object's keys in Snowflake's order
    if isinstance(node, exp.Anonymous) and node.name.upper() == "ARRAY_CONSTRUCT_STRUCTURED":
        return exp.Array(expressions=list(node.expressions))  # (the same values; only the declared type differs)
    if isinstance(node, exp.Time) and node.this is not None and not node.args.get("zone"):
        if isinstance(node.this, exp.Literal) and node.this.is_string:
            return exp.cast(node.this, "time")  # TIME('10:20:30')
        return _fn("_sf_time", node.this)  # TIME(ts): the time of day it holds (sqlglot reads a zone into it)
    if isinstance(node, exp.Window) and isinstance(node.this, exp.Anonymous) and isinstance(node.this.this, str) \
            and node.this.name.upper() in ("CONDITIONAL_TRUE_EVENT", "CONDITIONAL_CHANGE_EVENT") and node.this.expressions:
        # so far in the window's order: how many rows met the condition / how often the value changed
        arg = node.this.expressions[0].copy()
        so_far = exp.WindowSpec(kind="ROWS", start="UNBOUNDED", start_side="PRECEDING", end="CURRENT ROW")
        window = lambda f: exp.Window(this=f, partition_by=[p.copy() for p in node.args.get("partition_by") or []],  # noqa: E731
                                      order=node.args["order"].copy() if node.args.get("order") else None, spec=so_far)
        if node.this.name.upper() == "CONDITIONAL_TRUE_EVENT":
            return window(exp.Sum(this=exp.Case(ifs=[exp.If(this=arg, true=exp.Literal.number(1))], default=exp.Literal.number(0))))
        return _fn("_sf_change_event", window(exp.Anonymous(this="LIST", expressions=[arg])))
    if isinstance(node, exp.Anonymous) and isinstance(node.this, str) and node.name.lower() == "map_construct" \
            and len(node.expressions) % 2 == 0:
        pairs = [x for k, v in zip(node.expressions[::2], node.expressions[1::2]) for x in (exp.cast(k.copy(), "varchar"), v.copy())]
        return exp.Anonymous(this="json_object", expressions=pairs)  # a MAP reads back as the object it lists
    if isinstance(node, exp.Anonymous) and isinstance(node.this, str) and node.name.lower() == "like_any" and len(node.expressions) >= 2:
        subject, *patterns = node.expressions  # like_any(s, p1, p2): like one of them
        tests = [exp.Like(this=subject.copy(), expression=p.copy()) for p in patterns]
        return exp.paren(tests[0] if len(tests) == 1 else exp.or_(*tests))
    if isinstance(node, (exp.TimeToStr, exp.StrToTime, exp.StrToDate)) and isinstance(node.args.get("format"), exp.Literal):
        fmt = node.args["format"]
        fmt.set("this", fmt.name.replace("Mon", "%b").replace("Dy", "%a"))
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and node.to.is_type("varchar", "text") \
            and not node.to.expressions and not (isinstance(node.this, (exp.JSONExtract, exp.Bracket)) and not _computed_index(node.this)) \
            and not (isinstance(node.this, exp.Literal) and node.this.is_string):
        return _text(node.this)
    if isinstance(node, exp.Add) and isinstance(node.expression, exp.Interval) \
            and (node.expression.unit and node.expression.unit.name.upper() in _DAY_UNITS):
        return _fn("_sf_like", node.this.copy(), node)
    if isinstance(node, (exp.DateAdd, exp.DateSub, exp.TimestampAdd)) and isinstance(node.this, (exp.Concat, exp.DPipe)):
        # DATEADD(month, 1, CONCAT(year, '-01-01')): text put together is read as a timestamp
        node.set("this", exp.cast(exp.paren(node.this.copy()), "timestamp"))
    if isinstance(node, exp.DateAdd) and node.unit and node.unit.name.upper() in _DAY_UNITS \
            and not (isinstance(node.parent, exp.Anonymous) and node.parent.name.lower().endswith("_sf_like")):
        if node.unit.name.upper() == "QUARTER":
            node.set("unit", exp.var("MONTH"))
            node.set("expression", exp.Mul(this=exp.paren(node.expression), expression=exp.Literal.number(3)))
        # DATEADD(day, 1, a_date) is a DATE on Snowflake; DuckDB's date + interval is a TIMESTAMP
        return _fn("_sf_like", node.this.copy(), node)
    if isinstance(node, exp.MonthsBetween):
        return exp.func("round", node, exp.Literal.number(6))
    if isinstance(node, exp.RegexpInstr) and not isinstance(node.parent, exp.Cast):
        return exp.cast(node, "bigint")  # a position, as a NUMBER: DuckDB sums list lengths into a HUGEINT
    if isinstance(node, (exp.Variance, exp.VariancePop)) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_var", node, exp.Min(this=node.this.copy()))
    if isinstance(node, (exp.Skewness, exp.Kurtosis)) and not isinstance(node.parent, exp.Window):
        return _moment(node)
    if isinstance(node, (exp.Stddev, exp.StddevSamp)) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_std", exp.Variance(this=node.this.copy()), exp.Min(this=node.this.copy()))
    if isinstance(node, exp.StddevPop) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_std", exp.VariancePop(this=node.this.copy()), exp.Min(this=node.this.copy()))
    if isinstance(node, exp.GroupConcat):
        if node.args.get("separator") is None:
            node.set("separator", exp.Literal.string(""))  # the default delimiter is empty, not a comma
        if not isinstance(node.parent, (exp.Window, exp.WithinGroup)):
            return exp.Coalesce(this=node, expressions=[exp.Literal.string("")])  # and no rows gives ''
    if isinstance(node, (exp.ToChar, exp.StrToTime, exp.StrToDate, exp.TsOrDsToDate)) and isinstance(node.args.get("format"), exp.Literal):
        node.args["format"].set("this", node.args["format"].name.replace("Mon", "MON").replace("Dy", "DY"))
    if isinstance(node, exp.ArraySize):
        return _fn("_sf_array_size", node.this)
    if isinstance(node, exp.Anonymous) and node.name.lower() in _TIMESTAMP_PARSERS and len(node.expressions) == 1:
        # TRY_TO_TIMESTAMP_NTZ(column): the one-argument forms on a non-literal
        arg = node.expressions[0]
        if node.name.lower().startswith("try_"):
            return _fn("_sf_try_timestamp", arg)
        if _is(arg, "_sf_int") or (isinstance(arg, exp.Cast) and arg.to.is_type("int", "bigint", "decimal", "double", "float")):
            return exp.cast(_fn("_sf_seconds", arg), "timestamp")  # a number: seconds since 1970
        return exp.cast(arg, "timestamp")
    if isinstance(node, exp.ToNumber) and isinstance(node.args.get("format"), exp.Literal) \
            and re.fullmatch(r"[09,.$]+", node.args["format"].name.strip()):
        declared = node.args.get("scale")  # the format only guides parsing; the result scale is 0 unless given
        scale = int(declared.name) if isinstance(declared, exp.Literal) else 0
        return exp.cast(exp.func("replace", node.this, exp.Literal.string(","), exp.Literal.string("")), f"decimal(38, {scale})")
    if isinstance(node, exp.Anonymous) and node.name.lower() in _RENAMED:
        return _fn(_RENAMED[node.name.lower()], *node.expressions)
    if isinstance(node, exp.TableFromRows) and isinstance(node.this, exp.SplitToTable):
        call = _split_to_table(node.this)
        if node.args.get("alias"):
            call.set("alias", node.args["alias"])
        return call
    if isinstance(node, exp.Join) and node.args.get("match_condition") is not None:
        cond = node.args.pop("match_condition")  # ASOF JOIN ... MATCH_CONDITION (c) is ASOF JOIN ... ON c
        node.set("on", exp.and_(cond, node.args["on"]) if node.args.get("on") else cond)
    if isinstance(node, exp.Div):
        return _fn("_sf_div", node.this, node.expression)
    if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
        return _fn("_sf_date_trunc", exp.Literal.string(node.unit.name.lower()), node.this)
    if isinstance(node, _HASHES) and not (isinstance(node.this, exp.Literal) and node.this.is_string):
        node.set("this", _fn("_sf_text", node.this))  # MD5(42) hashes the text '42'
    if isinstance(node, exp.CurrentRole) or (isinstance(node, exp.Anonymous) and node.name.upper() == "INVOKER_ROLE"):
        # looked up when the query runs, so a policy or view written under one role answers for the role reading it
        role = _fn("_sf_current_role")
        return exp.alias_(role, "CURRENT_ROLE()", quoted=True) if isinstance(node.parent, exp.Select) else role
    session = SESSION.get(type(node).__name__) or (isinstance(node, exp.Anonymous) and SESSION.get(node.name.upper()))
    if session:
        value = exp.Literal.string(session)  # as a projection it keeps the name Snowflake gives it
        return exp.alias_(value, node.sql(dialect="snowflake").upper(), quoted=True) if isinstance(node.parent, exp.Select) else value
    if isinstance(node, exp.Anonymous) and node.name.upper() == "IS_ROLE_IN_SESSION" and node.expressions:
        return exp.EQ(this=exp.Upper(this=node.expressions[0]), expression=_fn("_sf_current_role"))
    if isinstance(node, exp.Anonymous) and node.name.upper() == "GET_DDL" and len(node.expressions) >= 2:
        return _fn("_sf_get_ddl", *node.expressions[:2])
    if isinstance(node, exp.TableFromRows):
        call = node.this
        while isinstance(call, exp.Dot):  # table(db.schema.my_udtf(x)): drop the qualification
            call = call.expression
        if isinstance(call, exp.Anonymous) and call.name.lower() in UDTFS:
            return exp.Table(this=_fn(UDTFS[call.name.lower()], *call.expressions), alias=node.args.get("alias"))
        columns = catalog.table_function(call.name) if isinstance(call, exp.Anonymous) and isinstance(call.this, str) else None
        if columns:
            # a Snowflake system table function (task_history, copy_history...): its columns, and no rows locally
            empty = exp.select(*[exp.alias_(exp.cast(exp.Null(), kind), col, quoted=True) for col, kind in columns.items()]).where("false")
            alias = node.args.get("alias") or exp.TableAlias(this=exp.to_identifier(call.name.upper()))
            return exp.Subquery(this=empty, alias=alias)
    if isinstance(node, exp.Anonymous) and node.name.lower() in UDFS:
        call = _fn(UDFS[node.name.lower()], *node.expressions)
        if isinstance(node.parent, exp.Dot):  # db.schema.my_udf(x): drop the qualification
            top = node.parent
            while isinstance(top.parent, exp.Dot):
                top = top.parent
            top.replace(call)
        return call
    if isinstance(node, exp.Typeof):
        return _fn("_sf_typeof", node.this)
    if isinstance(node, exp.ToChar) and isinstance(node.args.get("format"), exp.Literal):
        formatted = _number_format(node.this, node.args["format"].name)
        if formatted is not None:
            return formatted
        if _DATE_TOKENS.search(node.args["format"].name):
            # TO_CHAR(column, 'YYYYMMDD'): without a known column type the format was dropped
            from sqlglot.dialects.snowflake import Snowflake
            from sqlglot.time import format_time

            model = node.args["format"].name.replace("Mon", "MON").replace("Dy", "DY")
            fmt = format_time(model, Snowflake.TIME_MAPPING, Snowflake.TIME_TRIE)
            return exp.TimeToStr(this=exp.cast(node.this, "timestamp"), format=exp.Literal.string(fmt))
    if isinstance(node, exp.Avg) and not (isinstance(node.parent, exp.Window) and node.parent.this is node):
        return _avg(node)
    if isinstance(node, exp.Window):
        inner = node.this
        if isinstance(inner, (exp.Skewness, exp.Kurtosis)):
            return _moment(inner, node)
        if isinstance(inner, exp.Avg):
            return _fn("_sf_div3", _over(node, exp.Sum(this=inner.this.copy())), _over(node, exp.Count(this=inner.this.copy())))
        if isinstance(inner, exp.Anonymous) and inner.expressions:
            name, arg = inner.name.lower(), inner.expressions[0]
            if name == "ratio_to_report":
                # (NULL when the group's total is zero, as Snowflake documents: not the error a plain `/` gives)
                total = _over(node, exp.Sum(this=arg.copy()))
                return _fn("_sf_div", arg.copy(), exp.Nullif(this=total, expression=exp.Literal.number(0)))
            if name == "conditional_true_event":
                flag = exp.Case(ifs=[exp.If(this=arg.copy(), true=exp.Literal.number(1))], default=exp.Literal.number(0))
                return _over(node, exp.Sum(this=flag))
    return node


def _self_referencing(cte: exp.CTE) -> bool:
    return any(not t.db and t.name.lower() == cte.alias.lower() for t in cte.this.find_all(exp.Table))


def _layer_joined_windows(select: exp.Select) -> bool:
    """The same for a SELECT with joins. Its FROM cannot be folded into `select *`, so the split goes
    the other way: the query as written, minus the projections that use an aliased window, becomes
    the inner query, and those projections are computed outside from its output columns."""
    projections = list(select.expressions)
    if any(isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star)) for p in projections):
        return False
    if any(not isinstance(p, (exp.Alias, exp.Column)) for p in projections):
        return False  # an unnamed expression has no name to carry it outside
    names = [p.alias_or_name.lower() for p in projections]
    windowed = {p.alias.lower() for p in projections if isinstance(p, exp.Alias) and p.find(exp.Window)}

    def reads(expr: exp.Expression, aliases: set[str], inside_window: bool) -> bool:
        for col in expr.find_all(exp.Column):
            if col.table or col.name.lower() not in aliases or col.find_ancestor(exp.Select) is not select:
                continue
            if not inside_window or col.find_ancestor(exp.Window) is not None:
                return True
        return False

    outside = {i for i, p in enumerate(projections) if isinstance(p, exp.Alias) and p.find(exp.Window)
               and reads(p.this, windowed - {p.alias.lower()}, True)}
    if not outside:
        return False
    for _ in range(len(projections)):  # and whatever reads those in turn
        more = {i for i, p in enumerate(projections) if i not in outside and isinstance(p, exp.Alias)
                and reads(p.this, {names[j] for j in outside}, False)}
        if not more:
            break
        outside |= more
    inner = select.copy()
    kept = [p for i, p in enumerate(inner.expressions) if i not in outside]
    inner_names = {names[i] for i in range(len(projections)) if i not in outside}
    hidden: dict[str, str] = {}

    def lift(expr: exp.Expression) -> exp.Expression:
        """Columns of the joined tables that an outside expression needs come along under hidden names."""
        expr = expr.copy()
        for col in list(expr.find_all(exp.Column)):
            if not col.table and col.name.lower() in (inner_names | {names[j] for j in outside}):
                continue
            key = col.sql(dialect="snowflake")
            hidden.setdefault(key, f"_lw_{len(hidden)}")
            col.replace(exp.column(hidden[key]))
        return expr

    outer_projections = []
    for i, p in enumerate(projections):
        if i in outside:
            outer_projections.append(exp.alias_(lift(p.this), p.args["alias"].copy()))
        else:
            outer_projections.append(exp.Column(this=(p.args["alias"] if isinstance(p, exp.Alias) else p.this).copy()))
    trailing = {}
    for key in ("qualify", "order", "limit", "offset"):
        clause = inner.args.pop(key, None)
        select.args.pop(key, None)
        if clause is not None:
            trailing[key] = lift(clause)
    inner.set("expressions", kept + [exp.alias_(sqlglot.parse_one(key, into=exp.Column, read="snowflake"), name)
                                    for key, name in hidden.items()])
    with_ = inner.args.pop("with_", None)
    outer = exp.select(*outer_projections).from_(inner.subquery("_lw"))
    for key, clause in trailing.items():
        outer.set(key, clause)
    if with_ is not None:
        outer.set("with_", with_)
    select.replace(outer) if select.parent is not None else None
    if select.parent is None:  # the statement itself: rebuild it in place
        select.args.clear()
        for key, value in outer.args.items():
            select.set(key, value)
    return True


def layer_windows(tree: exp.Expression) -> exp.Expression:
    """`lag(x) over (...) as prev, sum(prev) over (...)`: Snowflake computes the first window, then
    the second. DuckDB will not nest them, so compute the aliased windows in a subquery first."""
    for _ in range(6):
        changed = False
        for select in list(tree.find_all(exp.Select)):
            source = select.args.get("from_")
            if source is not None and select.args.get("joins") and not select.args.get("group"):
                changed |= _layer_joined_windows(select)
                continue
            if source is None or select.args.get("group") or select.args.get("joins") or select.args.get("laterals"):
                continue
            windowed = {p.alias.lower(): p for p in select.expressions if isinstance(p, exp.Alias) and p.find(exp.Window)}
            nested = set()
            for window in select.find_all(exp.Window):
                if window.find_ancestor(exp.Select) is not select:
                    continue
                for col in window.find_all(exp.Column):
                    owner = windowed.get(col.name.lower())
                    if owner is not None and not col.table and col.find_ancestor(exp.Alias) is not owner:
                        nested.add(col.name.lower())
            if not nested:
                continue
            hidden = {name: f"_la_{i}_{name}" for i, name in enumerate(sorted(nested))}
            # the windows move one level down, where the other aliases of this SELECT do not exist:
            # write those out (`a || b as k, lag(k) over (...) as prev, count_if(k != prev) over (...)`)
            plain = {p.alias.lower(): p.this for p in select.expressions if isinstance(p, exp.Alias) and not p.find(exp.Window)
                     and not (isinstance(p.this, exp.Column) and p.this.name.lower() == p.alias.lower())}

            def moved(expression: exp.Expression) -> exp.Expression:
                out = expression.copy()
                for _ in range(4):
                    again = False
                    for col in list(out.find_all(exp.Column)):
                        if not col.table and col.name.lower() in plain:
                            replacement = exp.paren(plain[col.name.lower()].copy())
                            if col is out:
                                out = replacement
                            else:
                                col.replace(replacement)
                            again = True
                    if not again:
                        break
                return out

            inner = exp.select("*", *[exp.alias_(moved(windowed[n].this), hidden[n]) for n in sorted(nested)])
            inner.set("from_", source.copy())
            if select.args.get("where"):
                inner.set("where", select.args.pop("where"))
            qualifier = source.this.alias_or_name.lower()
            for col in list(select.find_all(exp.Column)):
                if col.find_ancestor(exp.Select) is not select:
                    continue
                if col.table and col.table.lower() == qualifier:
                    col.set("table", None)
                if not col.table and col.name.lower() in hidden and isinstance(col.find_ancestor(exp.Window), exp.Window):
                    col.replace(exp.column(hidden[col.name.lower()]))
            for name in nested:
                windowed[name].set("this", exp.column(hidden[name]))
            for star in [p for p in select.expressions if isinstance(p, exp.Star)]:
                star.set("except_", [*(star.args.get("except_") or []), *[exp.column(h) for h in hidden.values()]])
            select.set("from_", exp.From(this=inner.subquery("_la")))
            changed = True
        if not changed:
            break
    return tree


def inline_aliases(tree: exp.Expression, names: set[str]) -> exp.Expression:
    """`cast(ts as date) as d, md5(d || id) as k ... group by k`: DuckDB will not group by an
    alias built from another alias. Write the first alias out where the second one uses it."""
    for _ in range(4):
        changed = False
        for select in tree.find_all(exp.Select):
            defined = {p.alias.upper(): p for p in select.expressions if isinstance(p, exp.Alias) and p.alias.upper() in names}
            # the other projections, and the JOIN conditions: Snowflake lets `on` use a SELECT alias
            users = [*select.expressions, *[j.args["on"] for j in select.args.get("joins") or [] if j.args.get("on")],
                     # and WHERE, QUALIFY, HAVING: `f(x) as links ... where array_size(links) != 0` with a function DuckDB
                     # will not evaluate twice by itself
                     *[select.args[k] for k in ("where", "qualify", "having") if select.args.get(k)],
                     # FLATTEN(links) next to the SELECT that defines `links`
                     *[j.this for j in select.args.get("joins") or [] if isinstance(j.this, (exp.Lateral, exp.Unnest, exp.Table))
                       and not isinstance(j.this.this, exp.Identifier)]]
            for proj in users:
                for col in list(proj.find_all(exp.Column)):
                    owner = defined.get(col.name.upper())
                    if owner is None or owner is proj or col.table or col.find_ancestor(exp.Select) is not select:
                        continue
                    col.replace(exp.paren(owner.this.copy()))
                    changed = True
        if not changed:
            break
    return tree


def qualify_schemas(tree: exp.Expression, database: str) -> exp.Expression:
    """`schema.table` always means the current database in Snowflake. DuckDB hesitates when a
    database carries the schema's name: spell the database out."""
    for table in tree.find_all(exp.Table):
        if table.db and not table.catalog and isinstance(table.this, exp.Identifier) and table.db.upper() != "INFORMATION_SCHEMA":
            table.set("catalog", exp.to_identifier(database))
    return tree


def qualify_using(tree: exp.Expression, owners: dict[str, str]) -> exp.Expression:
    """In `a JOIN b USING (id)`, a column both tables have (other than id) is read from the left
    table in Snowflake. DuckDB calls it ambiguous: name the table it suggested first."""
    for select in tree.find_all(exp.Select):
        if not any(j.args.get("using") for j in select.args.get("joins") or []):
            continue
        for col in select.find_all(exp.Column):
            if not col.table and col.name.upper() in owners and col.find_ancestor(exp.Select) is select:
                col.set("table", exp.to_identifier(owners[col.name.upper()]))
    return tree


def reserved_words(conn) -> None:  # noqa: ANN001
    _RESERVED.update(w.upper() for (w,) in conn.execute(
        "select keyword_name from duckdb_keywords() where keyword_category in ('reserved', 'type_function')").fetchall())


_RESERVED: set[str] = set()


def _balanced(kind: type, operands: list[exp.Expression]) -> exp.Expression:
    if len(operands) == 1:
        return operands[0]
    half = len(operands) // 2
    return exp.paren(kind(this=_balanced(kind, operands[:half]), expression=_balanced(kind, operands[half:])))


def balance_chains(transformed: exp.Expression) -> exp.Expression:
    """`a or b or c or ...` with thousands of terms (a pivot, a generated filter) is a tree as deep
    as it is long, and DuckDB stops at a depth of 1000. The same terms as a balanced tree."""
    for kind in (exp.Or, exp.And, exp.DPipe, exp.Add):
        for node in list(transformed.find_all(kind)):
            if isinstance(node.parent, kind) and node.parent.this is node:
                continue  # not the top of its chain
            operands, cursor, steps = [], node, 0
            while type(cursor) is kind:
                operands.append(cursor.expression)
                cursor, steps = cursor.this, steps + 1
            if steps < 400:
                continue
            operands.append(cursor)
            operands.reverse()
            node.replace(_balanced(kind, operands))
    return transformed


_NOT_TEXT = (exp.Binary, exp.AggFunc, exp.Null, exp.Neg, exp.Abs, exp.Round, exp.Floor, exp.Ceil)


def _number_texts(transformed: exp.Expression) -> None:
    """A cast of text to an integer or decimal also reads what Snowflake reads as a number and DuckDB does not."""
    for cast in list(transformed.find_all(exp.Cast, exp.TryCast)):
        if not cast.to.is_type("decimal", "bigint", "int", "smallint", "tinyint", "hugeint"):
            continue
        arg = cast.this
        if isinstance(arg, _NOT_TEXT) or (isinstance(arg, exp.Literal) and not arg.is_string) \
                or (isinstance(arg, exp.Cast) and not arg.to.is_type("varchar", "text")) or _is(arg, "_sf_num_text") \
                or any(_is(c, "_sf_num_text") for c in (cast.parent.find_all(exp.Anonymous) if isinstance(cast.parent, exp.If) else [])):
            continue
        if isinstance(arg, exp.Literal) and not re.fullmatch(r"\s*[+-]?[0-9]*\.?[0-9]*[eE]?\s*", arg.name):
            continue  # text that is not a number of this kind: as it is
        fixed = _fn("_sf_num_text", arg.copy())
        cast.replace(exp.Case(ifs=[exp.If(this=exp.Not(this=exp.Is(this=fixed.copy(), expression=exp.Null())),
                                          true=cast.__class__(this=fixed, to=cast.to.copy()))], default=cast.copy()))


def _undefined_in_arrays(transformed: exp.Expression) -> None:
    """ARRAY_CONSTRUCT(NULL, 1) holds `undefined` where it was given a SQL NULL; an element read back is SQL NULL."""
    marker = sqlglot.parse_one(f"select {_UNDEFINED_JSON}", read="duckdb").expressions[0]
    def built(array: exp.Array) -> bool:  # an ARRAY_CONSTRUCT: in TO_JSON, or in one that is (nested arrays)
        up = array.parent
        while isinstance(up, exp.Array):
            up = up.parent
        return isinstance(up, exp.Anonymous) and up.name.upper() == "TO_JSON"

    for array in reversed(list(transformed.find_all(exp.Array))):  # (the innermost first: an outer one copies it)
        if built(array) and array.expressions:
            array.set("expressions", [e if isinstance(e, exp.Literal) else exp.func("coalesce", e.copy(), marker.copy())
                                      for e in array.expressions])
    for access in list(transformed.find_all(exp.JSONExtract, exp.JSONExtractScalar)):
        path = access.expression
        indexed = not isinstance(path, (exp.JSONPath, exp.Literal)) or (
            isinstance(path, exp.JSONPath) and any(isinstance(p, (exp.JSONPathSubscript, exp.JSONPathSlice)) for p in path.expressions)) \
            or (isinstance(path, exp.Literal) and "[" in path.name)
        if not indexed or (isinstance(access.parent, exp.Nullif) and access.parent.this is access):
            continue
        other = marker.copy() if isinstance(access, exp.JSONExtract) else \
            exp.JSONExtractScalar(this=marker.copy(), expression=exp.Literal.string("$"))
        access.replace(exp.Nullif(this=access.copy(), expression=other))


def _cased_twins(transformed: exp.Expression) -> None:
    """`select "Metric_Name" from (select 1 as metric_name, 2 as "Metric_Name")`: two columns Snowflake tells apart
    by their case, which DuckDB takes for one (the first). The second is renamed where it is made and where it is read."""
    if not any(i.quoted and i.this != i.this.upper() for i in transformed.find_all(exp.Identifier)):
        return
    from sqlglot.optimizer.scope import Scope, traverse_scope

    def target(ident: exp.Identifier) -> str:
        return ident.this if ident.quoted else ident.this.upper()

    for scope in traverse_scope(transformed):
        for name, (_node, source) in scope.selected_sources.items():
            if not isinstance(source, Scope) or not isinstance(source.expression, exp.Select):
                continue
            seen: dict[str, str] = {}
            renamed: dict[str, str] = {}
            for e in source.expression.expressions:
                if not (isinstance(e, exp.Alias) and isinstance(e.args.get("alias"), exp.Identifier)):
                    continue
                said = target(e.args["alias"])
                if said.lower() in seen and seen[said.lower()] != said:
                    renamed[said] = f"{said}_fx{len(renamed) + 1}"
                    e.set("alias", exp.to_identifier(renamed[said], quoted=True))
                else:
                    seen.setdefault(said.lower(), said)
            if not renamed:
                continue
            for ref in scope.columns:
                if isinstance(ref.this, exp.Identifier) and target(ref.this) in renamed and (not ref.table or ref.table.upper() == name.upper()):
                    original = target(ref.this)
                    ref.set("this", exp.to_identifier(renamed[original], quoted=True))
                    if ref.parent is scope.expression and ref in scope.expression.expressions:
                        ref.replace(exp.alias_(ref.copy(), exp.to_identifier(original, quoted=True)))


def _windows_without_errors(transformed: exp.Expression) -> None:
    """`row_number() over (partition by try_to_number(id))`: the check of what a TRY_ function is given raises with
    error(), which DuckDB takes for volatile and refuses in a window's PARTITION BY and ORDER BY. There the check is
    the one made when the statement is bound (a date, a time or a timestamp is refused), which is not volatile."""
    for window in list(transformed.find_all(exp.Window)):
        for part in [*(window.args.get("partition_by") or []), window.args.get("order")]:
            if part is None:
                continue
            for call in list(part.find_all(exp.Anonymous)):
                if str(call.name).split(".")[-1].lower() == "_sf_try_input" and call.expressions:
                    target = call.expressions[1].name.upper() if len(call.expressions) > 1 else ""
                    guard = _NS + ("_fx_try_date" if target == "DATE" else "_fx_try_binary" if target == "BLOB" else "_fx_convertible")
                    value = call.expressions[0]
                    call.replace(exp.Case(ifs=[exp.If(this=exp.Anonymous(this=guard, expressions=[value.copy()]), true=value.copy())]))


def _documents_as_text(transformed: exp.Expression) -> None:
    """`v:x::varchar` when v:x is an object or an array: Snowflake writes it with its keys in order (an XML element as
    XML); DuckDB's ->> gives the JSON as it is stored. A string at that path stays the string."""
    for cast in list(transformed.find_all(exp.Cast)):
        inner = cast.this.unnest() if isinstance(cast.this, exp.Paren) else cast.this
        if not (isinstance(inner, exp.JSONExtractScalar) and cast.to.is_type("text", "varchar")):
            continue
        doc, path = inner.this, inner.expression
        if doc is None or path is None:
            continue
        kind = exp.Anonymous(this="json_type", expressions=[doc.copy(), path.copy()])
        whole = exp.cast(exp.Anonymous(this="json_extract", expressions=[doc.copy(), path.copy()]), "varchar")
        cast.replace(exp.Case(ifs=[exp.If(this=exp.In(this=kind, expressions=[exp.Literal.string("OBJECT"), exp.Literal.string("ARRAY")]),
                                          true=_fn("_fx_json_sorted", whole))], default=cast.copy()))


def finish(transformed: exp.Expression) -> exp.Expression:
    """Fix-ups on the DuckDB side of the translation."""
    transformed = balance_chains(transformed)
    _windows_without_errors(transformed)
    _cased_twins(transformed)
    _undefined_in_arrays(transformed)
    _number_texts(transformed)
    for agg in list(transformed.find_all(exp.ArrayAgg)):
        # ARRAY_AGG(OBJECT_CONSTRUCT(*)) is written for DuckDB with a filter that leaves out NULLs and
        # repeats the argument. An object of every column is never NULL, and DuckDB will not expand a
        # star under NOT: the aggregate goes as it is
        if isinstance(agg.this, exp.Anonymous) and "*COLUMNS(" in agg.this.sql(dialect="duckdb"):
            agg.replace(exp.Anonymous(this="ARRAY_AGG", expressions=[agg.this.copy()]))
    for ident in transformed.find_all(exp.Identifier):
        # `end`, `overlaps`... are ordinary names in Snowflake and reserved words in DuckDB
        if not ident.args.get("quoted") and ident.name.upper() in _RESERVED \
                and not (isinstance(ident.parent, exp.Column) and ident.name.upper() in _BARE):
            ident.set("quoted", True)
    for access in list(transformed.find_all(exp.JSONExtract, exp.JSONExtractScalar)):
        # v:"" reads the key that is the empty string. DuckDB's JSONPath cannot name it; a JSON pointer can
        path = access.expression
        if isinstance(path, exp.JSONPath) and any(isinstance(p, exp.JSONPathKey) and p.this == "" for p in path.expressions):
            steps = [str(p.this).replace("~", "~0").replace("/", "~1") for p in path.expressions
                     if isinstance(p, (exp.JSONPathKey, exp.JSONPathSubscript))]
            access.set("expression", exp.Literal.string("/" + "/".join(steps)))
    for agg in list(transformed.find_all(exp.ArrayAgg)):
        # ARRAY_AGG leaves NULLs out, also with DISTINCT or WITHIN GROUP (ORDER BY ...)
        agg.args.pop("nulls_excluded", None)  # the filter is added here, once, for every form
        if isinstance(agg.parent, exp.Filter):
            continue
        value = agg.this
        while isinstance(value, (exp.Order, exp.Distinct)):
            value = value.this if isinstance(value, exp.Order) else value.expressions[0]
        slot = exp.Null()
        agg.replace(slot)
        slot.replace(exp.Filter(this=agg, expression=exp.Where(this=exp.Is(this=value.copy(), expression=exp.Null()).not_())))
    for window in list(transformed.find_all(exp.Window)):
        # OBJECT_AGG(...) OVER (...) became to_json(map(list(..), list(..))) OVER: window each aggregate
        wrapper = window.this
        if isinstance(wrapper, exp.AggFunc) or not (isinstance(wrapper, exp.Anonymous) and wrapper.name.upper() == "TO_JSON"):
            continue
        tops = [n for n in wrapper.find_all(exp.Filter, exp.AggFunc, exp.Anonymous)
                if not isinstance(n.parent, (exp.Filter, exp.Window))
                and (not isinstance(n, exp.Anonymous) or n.name.upper() == "LIST")]
        for top in tops:
            over, slot = window.copy(), exp.Null()
            top.replace(slot)
            over.set("this", top)
            slot.replace(over)
        if tops:
            window.replace(wrapper)
    for cmp in list(transformed.find_all(*_COMPARISONS)):
        # a = 'x' = FALSE: DuckDB will not chain comparisons without parentheses
        if isinstance(cmp.this, _COMPARISONS):
            cmp.set("this", exp.paren(cmp.this))
    _documents_as_text(transformed)  # (last: the paths are as DuckDB reads them by now)
    return transformed


_BARE = {"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "LOCALTIME", "LOCALTIMESTAMP",
         "TRUE", "FALSE", "NULL", "DEFAULT", "SESSION_USER", "USER"}


_FLATTEN_OUTPUT = {"SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"}


_hierarchies = 0


def hierarchical(select: exp.Select) -> None:
    """`START WITH ... CONNECT BY ... PRIOR ...` as the recursive CTE it describes. LEVEL,
    CONNECT_BY_ROOT and SYS_CONNECT_BY_PATH are carried along each path."""
    global _hierarchies
    connect, source = select.args["connect"], select.args["from_"].this
    _hierarchies += 1
    name = f"__CB_{_hierarchies}"
    alias = source.alias_or_name
    plain = source.copy()
    plain.set("alias", None)

    def qualified(expr: exp.Expression, row: str) -> exp.Expression:
        expr = expr.copy()
        for col in ([expr] if isinstance(expr, exp.Column) else []) + list(expr.find_all(exp.Column)):
            if not col.table or col.table.upper() == alias.upper():
                col.set("table", exp.to_identifier(row))
        return expr

    def sql(expr: exp.Expression) -> str:
        return expr.sql(dialect="snowflake")

    carried: list[tuple[str, str, str]] = []  # (column, value on the first row, value on each next row)
    scope = [n for n in select.walk() if n.find_ancestor(exp.Select) is select or n is select]
    for node in scope:
        if isinstance(node, exp.ConnectByRoot):
            col = f"__CB_ROOT_{len(carried)}"
            carried.append((col, sql(qualified(node.this, "__S")), f'__P."{col}"'))
            node.replace(exp.column(col, quoted=True))
        elif isinstance(node, exp.Anonymous) and node.name.upper() == "SYS_CONNECT_BY_PATH" and len(node.expressions) == 2:
            col = f"__CB_PATH_{len(carried)}"
            value, sep = sql(qualified(node.expressions[0], "__S")), sql(node.expressions[1])
            carried.append((col, f"{sep} || ({value})::varchar", f'__P."{col}" || {sep} || ({value})::varchar'))
            node.replace(exp.column(col, quoted=True))
    condition = connect.args["connect"].copy()
    for prior in list(condition.find_all(exp.Prior)):
        prior.replace(exp.paren(qualified(prior.this, "__P")))
    for col in condition.find_all(exp.Column):
        if not col.table or col.table.upper() == alias.upper():
            col.set("table", exp.to_identifier("__S"))
    start = sql(qualified(connect.args["start"], "__S")) if connect.args.get("start") is not None else "true"
    first = "".join(f', {value} as "{col}"' for col, value, _ in carried)
    following = "".join(f', {value} as "{col}"' for col, _, value in carried)
    body = (f'select __S.*, 1 as "LEVEL"{first} from {sql(plain)} as __S where {start} union all '
            f'select __S.*, __P."LEVEL" + 1{following} from {sql(plain)} as __S join "{name}" as __P on {sql(condition)} '
            f'where __P."LEVEL" < 500')
    cte = exp.CTE(this=sqlglot.parse_one(body, read="snowflake"), alias=exp.TableAlias(this=exp.to_identifier(name, quoted=True)))
    existing = select.args.get("with_")
    if existing is not None:
        existing.append("expressions", cte)
        existing.set("recursive", True)
    else:
        select.set("with_", exp.With(expressions=[cte], recursive=True))
    select.args["from_"].set("this", exp.Table(this=exp.to_identifier(name, quoted=True),
                                              alias=exp.TableAlias(this=exp.to_identifier(alias))))
    select.set("connect", None)


def _format_chosen_at_run_time(node: exp.Expression) -> exp.Expression | None:
    """`to_date(text, fmt)` where `fmt` is a column or an alias (`case ... then 'YYYY_MM_DD' else 'YYYY_MM' end`).
    DuckDB wants the format written in the query, and it is: one of the format literals the statement
    holds. One branch per literal."""
    fmt = node.args.get("format")
    if fmt is None or isinstance(fmt, exp.Literal) or isinstance(node.parent, exp.If):
        return None
    written = list(dict.fromkeys(
        lit.name for lit in node.root().find_all(exp.Literal)
        if lit.is_string and re.search(r"YYYY|YY|MM|DD|HH24|MON", lit.name) and not re.search(r"[a-z%\\]", lit.name) and len(lit.name) <= 40))[:12]
    if not written:
        return None
    name = "to_timestamp" if isinstance(node, exp.StrToTime) else "try_to_date" if node.args.get("safe") else "to_date"
    ifs = []
    for text in written:
        try:
            parsed = sqlglot.parse_one(f"select {name}(x, '{text}')", read="snowflake").expressions[0]
        except Exception:  # noqa: BLE001, S112 - not a format after all
            continue
        parsed.set("this", node.this.copy())
        ifs.append(exp.If(this=exp.EQ(this=fmt.copy(), expression=exp.Literal.string(text)), true=parsed))
    return exp.Case(ifs=ifs) if ifs else None


def rewrite(node: exp.Expression) -> exp.Expression:
    """Apply the fixes bottom-up, so nested cases (a ratio of averages) all get rewritten."""
    if isinstance(node, (exp.TsOrDsToDate, exp.StrToTime, exp.StrToDate, exp.TsOrDsToTimestamp)):
        chosen = _format_chosen_at_run_time(node)
        if chosen is not None:
            return chosen
    if isinstance(node, exp.Select) and node.args.get("connect") is not None and node.args.get("from_") is not None \
            and not node.args.get("joins"):
        hierarchical(node)
    if isinstance(node, exp.Select) and node.args.get("having") is not None and node.args.get("group") is None:
        own = [a for e in [*node.expressions, node.args["having"]] for a in e.find_all(exp.AggFunc)
               if a.find_ancestor(exp.Select) is node and a.find_ancestor(exp.Window) is None]
        if not own:
            # HAVING with nothing grouped and nothing aggregated is one more filter on the rows, and may name
            # the columns just defined. DuckDB would aggregate the whole table.
            condition = node.args["having"].this
            node.set("having", None)
            if node.args.get("qualify") is not None:
                node.args["qualify"].set("this", exp.and_(exp.paren(node.args["qualify"].this), exp.paren(condition)))
            else:
                where = node.args.get("where")
                node.set("where", exp.Where(this=exp.and_(exp.paren(where.this), exp.paren(condition)) if where else condition))
    if isinstance(node, exp.With) and not node.args.get("recursive") and any(
            isinstance(c.this, exp.SetOperation) and _self_referencing(c) for c in node.expressions):
        node.set("recursive", True)  # the keyword is optional in Snowflake
    if isinstance(node, exp.Select) and node.args.get("joins") and node.args.get("from_") is not None:
        # FROM t, TABLE(FLATTEN(value['items'])): the bare column is t's. DuckDB would take it for
        # the FLATTEN's own VALUE output, so say which table it comes from.
        base = node.args["from_"].this
        laterals = [j.this for j in node.args["joins"] if isinstance(j.this, (exp.Lateral, exp.TableFromRows))]
        if isinstance(base, (exp.Table, exp.Subquery)) and base.alias_or_name and len(laterals) == len(node.args["joins"]):
            for i, lateral in enumerate(laterals):
                for col in lateral.find_all(exp.Column):
                    if not col.table and (i == 0 or col.name.upper() not in _FLATTEN_OUTPUT):
                        col.set("table", exp.to_identifier(base.alias_or_name))
    if isinstance(node, exp.With):
        seen: set[str] = set()
        for cte in list(node.expressions):  # two CTEs with one name: Snowflake uses the first
            if cte.alias.upper() in seen:
                cte.pop()
            seen.add(cte.alias.upper())
    if isinstance(node, exp.Identifier) and node.args.get("quoted") and node.name == "":
        node.set("this", " ")  # "" is a legal name in Snowflake
    for child in list(node.iter_expressions()):
        # remember the slot first: a fix may wrap the child, which re-parents it
        parent, key, index = child.parent, child.arg_key, child.index
        new = rewrite(child)
        if new is not child:
            parent.set(key, new, index)
    return _fix(node)


_AS_BIGINT = {"TINYINT", "SMALLINT", "UTINYINT", "USMALLINT", "UINTEGER"}
_SUPPORTED = {"BIGINT", "BLOB", "BOOLEAN", "DATE", "DOUBLE", "INTEGER", "JSON", "TIME", "TIMESTAMP",
              "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "VARCHAR", '"NULL"'}
_AS_NUMBER = {"UBIGINT", "HUGEINT", "UHUGEINT"}  # beyond a signed 64-bit integer: Snowflake's NUMBER(38,0)


def _undefined_out(cursor) -> None:  # noqa: ANN001
    """A VARIANT holding an array with `undefined` in it reaches the client the way Snowflake writes it."""
    import pyarrow as pa
    import pyarrow.compute as pc

    table = cursor._arrow_table
    for i, (_name, kind, *_rest) in enumerate(cursor._duck_conn.description):
        if str(kind).upper() == "JSON" and i < table.num_columns and pa.types.is_string(table.column(i).type) \
                and pc.any(pc.match_substring(table.column(i), UNDEFINED)).as_py():
            table = table.set_column(i, table.field(i), pc.replace_substring(table.column(i), f'"{UNDEFINED}"', "undefined"))
    cursor._arrow_table = table


def coerce_result(cursor) -> None:  # noqa: ANN001
    """Re-select result columns whose DuckDB type the Snowflake wire protocol cannot carry:
    ARRAY and OBJECT values travel as JSON, small integers as BIGINT, everything else as text."""
    if cursor._arrow_table is None or not cursor._last_sql or not cursor._duck_conn.description:
        return
    _undefined_out(cursor)
    casts, changed = [], False
    for i, (name, kind, *_rest) in enumerate(cursor._duck_conn.description):
        kind, alias = str(kind).upper(), '"' + name.replace('"', '""') + '"'
        if kind in _SUPPORTED or kind.startswith("DECIMAL"):
            casts.append(f"_c{i} as {alias}")
            continue
        changed = True
        if kind.endswith("]") or kind.startswith(("STRUCT", "MAP", "UNION")):
            casts.append(f"to_json(_c{i})::JSON as {alias}")
        elif kind in _AS_BIGINT:
            casts.append(f"_c{i}::BIGINT as {alias}")
        elif kind in _AS_NUMBER:
            casts.append(f"_c{i}::DECIMAL(38, 0) as {alias}")
        elif kind == "FLOAT":
            casts.append(f"_c{i}::DOUBLE as {alias}")
        else:
            casts.append(f"_c{i}::VARCHAR as {alias}")
    if not changed:
        return
    names = ", ".join(f"_c{i}" for i in range(len(casts)))
    sql = f"SELECT {', '.join(casts)} FROM ({cursor._last_sql.rstrip().rstrip(';')}) AS _r({names})"
    cursor._duck_conn.execute(sql, cursor._last_params)
    cursor._arrow_table = cursor._duck_conn.to_arrow_table()
    cursor._last_sql = sql


_FLATTEN_CALL = re.compile(r"\bflatten\s*\(", re.IGNORECASE)


def outer_flattens(sql: str, option: str = "outer") -> list[bool]:
    """For each FLATTEN(...) in order: was it called with OUTER => TRUE (or another option)?"""
    flags = []
    for m in _FLATTEN_CALL.finditer(sql):
        depth, i = 1, m.end()
        while i < len(sql) and depth:
            depth += {"(": 1, ")": -1}.get(sql[i], 0)
            i += 1
        flags.append(bool(re.search(option + r"\s*=>\s*true", sql[m.end():i], re.IGNORECASE)))
    return flags


def flatten_paths(sql: str) -> list[str | None]:
    """For each FLATTEN(...) in order: the PATH => '...' it was called with, if any."""
    paths = []
    for m in _FLATTEN_CALL.finditer(sql):
        depth, i = 1, m.end()
        while i < len(sql) and depth:
            depth += {"(": 1, ")": -1}.get(sql[i], 0)
            i += 1
        given = re.search(r"\bpath\s*=>\s*'((?:[^']|'')*)'", sql[m.end():i], re.IGNORECASE)
        paths.append(given.group(1).replace("''", "'") if given else None)
    return paths


def _flatten_at(call: exp.Anonymous, path: str) -> None:
    """FLATTEN(input => x, path => 'a.b'): what is expanded is x:a.b, and each row's PATH starts with a.b."""
    if not call.expressions:
        return
    first = call.expressions[0]
    holder, value = (first, first.expressions[0]) if isinstance(first, exp.Anonymous) and first.name.lower().endswith("_sf_flattenable") \
        else (None, first)
    jsonpath = "$" + ("" if path.startswith("[") else ".") + path
    reached = exp.Anonymous(this="json_extract", expressions=[exp.Anonymous(this="to_json", expressions=[value.copy()]),
                                                              exp.Literal.string(jsonpath)])
    if holder is not None:
        holder.set("expressions", [reached, *holder.expressions[1:]])
    else:
        call.set("expressions", [reached, *call.expressions[1:]])
    table = call.parent if isinstance(call.parent, exp.Table) else None
    if table is None:
        return
    quoted = path.replace("'", "''")
    rows = sqlglot.parse_one(
        f"select SEQ, KEY, case when PATH = '' then '{quoted}' when PATH like '[%' then '{quoted}' || PATH "
        f"else '{quoted}.' || PATH end as PATH, INDEX, VALUE, THIS from _fx_expand", read="duckdb")
    for t in list(rows.find_all(exp.Table)):
        if t.name == "_fx_expand":
            t.set("this", call)  # (the call itself, moved: what is done to it next, OUTER or RECURSIVE, is done here)
    alias = table.args.get("alias")
    table.replace(exp.Subquery(this=rows, alias=alias.copy() if alias is not None else exp.TableAlias(this=exp.to_identifier("_fx_flatten"))))


_OPERATORS = (exp.Binary, exp.Unary, exp.In, exp.Between, exp.Is)


def parenthesize_arrows(transformed: exp.Expression) -> exp.Expression:
    """DuckDB's `->` binds more loosely than `=`: `a -> '$[2]' = 'c'` parses as
    `a -> ('$[2]' = 'c')`. Wrap every JSON access that sits inside an operator."""
    for node in list(transformed.find_all(exp.JSONExtract, exp.JSONExtractScalar)):
        if isinstance(node.parent, _OPERATORS):
            node.replace(exp.paren(node.copy()))
    return transformed


def keep_outer_rows(transformed: exp.Expression, flags: list[bool], recursive: list[bool] = (),
                    paths: list[str | None] = ()) -> exp.Expression:
    """FLATTEN(..., OUTER => TRUE) keeps rows that have nothing to expand: a LEFT lateral join. Also the other
    options: RECURSIVE, and PATH (see `_flatten_at`)."""
    calls = [n for n in transformed.find_all(exp.Anonymous) if n.name.upper() == "_FS_FLATTEN"]
    for call in calls:
        if call.expressions and not (isinstance(call.expressions[0], exp.Anonymous) and call.expressions[0].name.lower().endswith("_sf_flattenable")):
            call.set("expressions", [_fn("_sf_flattenable", call.expressions[0]), *call.expressions[1:]])
    if len(paths) == len(calls):
        for call, path in zip(calls, paths):
            if path:
                _flatten_at(call, path)
    if not any(flags) and not any(recursive):
        return transformed
    for call, deep in zip(calls, recursive):
        if deep:
            call.set("this", _NS + "_sf_flatten_recursive")
            first = call.expressions[0] if call.expressions else None
            if isinstance(first, exp.Anonymous) and first.name.lower().endswith("_sf_flattenable"):
                call.set("expressions", [first.expressions[0], *call.expressions[1:]])  # (a scalar is a row of its own)
    for call, outer, deep in zip(calls, flags, [*recursive, *[False] * len(calls)][:len(calls)]):
        join = call.find_ancestor(exp.Join)
        if outer and join is not None:
            join.set("kind", None)  # an explicit INNER JOIN LATERAL FLATTEN(... OUTER => TRUE)
            join.set("side", "LEFT")
            join.set("on", exp.true())
        elif outer:
            _outer_on_its_own(call)
    return transformed


def _outer_on_its_own(call: exp.Anonymous) -> None:
    """FLATTEN(..., OUTER => TRUE) on its own, not joined: a row of NULLs when there is nothing to expand (its PATH ''
    and THIS the input when that was an empty list or object). Written into the query: fakesnow's FLATTEN is a
    macro of each database."""
    table = call.parent if isinstance(call.parent, exp.Table) else None
    if table is None or not call.expressions:
        return
    value = call.expressions[0]
    given = value.expressions[0] if isinstance(value, exp.Anonymous) and value.name.lower().endswith("_sf_flattenable") else value
    rows = sqlglot.parse_one(
        "select * from _fx_expand union all select hash(to_json(_fx_in))::ubigint, null::varchar, "
        "case when json_type(to_json(_fx_in)) in ('ARRAY', 'OBJECT') then '' end, null::bigint, null::json, "
        "case when json_type(to_json(_fx_in)) in ('ARRAY', 'OBJECT') then to_json(_fx_in) end "
        "where not exists (select 1 from _fx_expand)", read="duckdb")
    for t in list(rows.find_all(exp.Table)):
        if t.name == "_fx_expand":
            t.set("this", call.copy())
    for c in list(rows.find_all(exp.Column)):
        if c.name == "_fx_in":
            c.replace(given.copy())
    alias = table.args.get("alias")
    table.replace(exp.Subquery(this=rows, alias=alias.copy() if alias is not None else exp.TableAlias(this=exp.to_identifier("_fx_flatten"))))


_QUOTED = re.compile(r"""'(?:[^'\\]|''|\\.)*'|"(?:[^"]|"")*"|\$\$.*?\$\$""", re.DOTALL)
_DOLLAR_NAME = re.compile(r"(?<![\$\w])\$(?!\d+)\w+")


def _inline_variables(self, sql: str) -> str:  # noqa: ANN001
    """Session variables, outside of string literals only: '$USD' in a string is text, and a
    `$name` that is not a variable is a column called "$NAME" (loaders produce those)."""
    def outside(text: str) -> str:
        for name, value in self._variables.items():
            text = re.sub(rf"\${name}\b", lambda _m: value, text, flags=re.IGNORECASE)
        return _DOLLAR_NAME.sub(lambda m: '"' + m.group().upper() + '"', text)

    out, last = [], 0
    for m in _QUOTED.finditer(sql):
        out += [outside(sql[last:m.start()]), m.group()]
        last = m.end()
    return "".join([*out, outside(sql[last:])])


def patch_upstream() -> None:
    """Switch off emulator transforms that change answers."""
    import fakesnow.transforms as upstream
    from fakesnow.variables import Variables

    Variables.inline_variables = _inline_variables

    # Casting every SUM/AVG argument to DOUBLE loses exact decimals and the result type.
    upstream.numeric_agg_implicit_cast = lambda e: e

    trim = upstream.trim_cast_varchar

    def trim_cast_varchar(e):  # noqa: ANN001
        # LTRIM/RTRIM/TRIM with explicit characters were collapsed to a plain TRIM.
        if isinstance(e, exp.Trim) and (e.args.get("position") or e.args.get("expression")):
            return e
        return trim(e)

    upstream.trim_cast_varchar = trim_cast_varchar

    # NULL timestamps were sent to the client as 1970-01-01: the struct that carries a
    # timestamp on the wire was built without its null mask.
    import fakesnow.arrow as wire
    import pyarrow as pa
    import pyarrow.compute as pc

    to_struct = wire.timestamp_to_sf_struct

    def timestamp_to_sf_struct(ts):  # noqa: ANN001
        if isinstance(ts, pa.ChunkedArray):
            ts = ts.combine_chunks()
        out = to_struct(ts)
        if ts.null_count == 0:
            return out
        children = [pc.fill_null(out.field(i), 0) for i in range(out.type.num_fields)]
        return pa.StructArray.from_arrays(children, fields=list(out.type), mask=ts.is_null())

    wire.timestamp_to_sf_struct = timestamp_to_sf_struct


# ---------------------------------------------------------------- implicit conversions
# Snowflake converts text to numbers and dates wherever a function or operator needs one
# (SUM(text_column), text_column * 2, DATEDIFF(day, text_column, ...)). DuckDB refuses to
# bind those. When it does, find the call it complained about and add the conversion.

_OPERATORS_BY_SIGN = {"+": exp.Add, "-": (exp.Sub, exp.Neg), "*": exp.Mul, "/": exp.Div, "%": exp.Mod, "//": exp.IntDiv}
_NUMERIC_CALLS = {"sum", "avg", "round", "abs", "floor", "ceil", "ceiling", "sqrt", "power", "pow", "ln", "log", "log10",
                  "log2", "exp", "sign", "trunc", "mod", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp",
                  "var_pop", "median", "quantile_cont", "quantile_disc", "fmod", "bit_count", "cbrt", "sin", "cos", "tan",
                  "product", "corr", "covar_pop", "covar_samp", "regr_slope", "regr_intercept", "to_seconds", "to_days",
                  "to_hours", "to_minutes", "to_months", "to_years", "to_weeks", "to_milliseconds", "to_microseconds",
                  "isnan", "isinf", "kurtosis", "skewness", "favg", "fsum", "range", "generate_series", "chr",
                  "to_timestamp", "epoch_ms", "make_date", "make_timestamp", "repeat", "lpad", "rpad", "substring",
                  "substr", "left", "right", "nullifzero", "width_bucket"}
_DATE_CALLS = {"date_diff", "datediff", "date_add", "date_sub", "date_trunc", "datetrunc", "date_part", "datepart",
               "dayname", "monthname", "last_day", "strftime", "timezone", "year", "month", "day", "week", "quarter",
               "dayofweek", "dayofyear", "dayofmonth", "hour", "minute", "second", "epoch", "weekofyear", "isodow",
               "yearweek", "time_bucket", "age", "isoyear", "millisecond", "microsecond", "epoch_us", "epoch_ns",
               "julian", "weekday", "century", "decade", "era", "extract"}
_TEXT_TYPES = {"VARCHAR", "STRING_LITERAL"}
_DATE_TYPES = ("INTERVAL", "DATE", "TIMESTAMP", "TIME")


def _error_details(message: str) -> dict | None:
    import json

    start = message.find("{")
    if start < 0:
        return None
    try:
        return json.loads(message[start:])
    except ValueError:
        return None


def _call_types(call: str) -> list[str]:
    inner, out, depth, cur = call[call.index("(") + 1:call.rindex(")")], [], 0, ""
    for ch in inner:
        depth += ch in "([<"
        depth -= ch in ")]>"
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    return [*out, cur.strip()] if cur.strip() else out


def _split_args(text: str) -> list[str]:
    out, depth, cur, quote = [], 0, "", None
    for ch in text:
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0 and not quote:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    return [*out, cur.strip()]


def _convert(node: exp.Expression, to: str) -> None:
    node.replace(exp.cast(node.copy(), to))


# Snowflake's string functions also take BINARY values, byte by byte. DuckDB's take text only: the bytes are
# worked on as hex digits, two to a byte, and turned back into bytes where the answer is a BINARY
_BINARY_CALLS = {"reverse": exp.Reverse, "lpad": exp.Pad, "rpad": exp.Pad, "concat_ws": exp.ConcatWs, "contains": exp.Contains,
                 "starts_with": exp.StartsWith, "ends_with": exp.EndsWith, "~": exp.BitwiseNot, "bit_length": exp.BitLength}


def _covers(text: str, sql: str, pos: int) -> bool:
    start = text.find(sql)
    while start >= 0:
        if start <= pos < start + len(sql):
            return True
        start = text.find(sql, start + 1)
    return False


def shifted_bytes(transformed: exp.Expression, conn, message: str) -> exp.Expression | None:  # noqa: ANN001
    """`BITSHIFTLEFT(to_binary('616263', 'hex'), 7)`: Snowflake shifts the bits of the bytes and keeps their number.
    DuckDB reads the operand as a whole number and cannot. The shifts of BINARY operands are made on BIT strings."""
    if "Unimplemented type for cast (BLOB ->" not in message:
        return None
    tree, changed = transformed.copy(), False
    for node in list(tree.find_all(exp.BitwiseLeftShift, exp.BitwiseRightShift)):
        operand = node.this.this if isinstance(node.this, exp.Cast) else node.this
        select = node.find_ancestor(exp.Select)
        probe = exp.Select(expressions=[exp.alias_(operand.copy(), "_k")])
        if select is not None and select.args.get("from_") is not None:
            probe.set("from_", select.args["from_"].copy())
            if select.args.get("joins"):
                probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            kind = conn.execute("describe " + (_cte_prefix(select) if select is not None else "") + probe.sql(dialect="duckdb")).fetchall()[0][1]
        except Exception:  # noqa: BLE001 - an operand that only binds in its place
            continue
        if kind == "BLOB":
            node.replace(exp.cast(node.__class__(this=exp.cast(operand.copy(), "bit"), expression=node.expression.copy()), "blob"))
            changed = True
    return tree if changed else None


def _binary_call(tree: exp.Expression, text: str, name: str, pos: int) -> bool:
    hexed = lambda e: exp.func("hex", e.copy())  # noqa: E731
    unhex = lambda e: exp.func("unhex", e)  # noqa: E731
    found = [n for n in tree.find_all(_BINARY_CALLS[name]) if _covers(text, n.sql(dialect="duckdb"), pos)]
    if not found:
        return False
    node = min(found, key=lambda n: len(n.sql(dialect="duckdb")))  # (the innermost)
    if name == "reverse":
        made = unhex(exp.func("array_to_string", exp.func("list_reverse", exp.func("regexp_extract_all", hexed(node.this), exp.Literal.string(".."))),
                                  exp.Literal.string("")))
    elif name in ("lpad", "rpad"):
        fill = node.args.get("fill_pattern")
        made = unhex(exp.func(name, hexed(node.this), exp.Mul(this=exp.paren(node.expression.copy()), expression=exp.Literal.number(2)),
                              *([hexed(fill)] if fill is not None else [])))
    elif name == "concat_ws":
        made = unhex(exp.ConcatWs(expressions=[hexed(e) for e in node.expressions]))
    elif name == "contains":  # (on a byte boundary)
        made = exp.func("regexp_matches", hexed(node.this), exp.DPipe(this=exp.Literal.string("^(..)*?"), expression=hexed(node.expression)))
    elif name in ("starts_with", "ends_with"):
        made = node.__class__(this=hexed(node.this), expression=hexed(node.expression))
    elif name == "~":
        made = exp.cast(exp.BitwiseNot(this=exp.cast(node.this.copy(), "bit")), "blob")
    else:  # bit_length
        made = exp.Mul(this=exp.func("octet_length", node.this.copy()), expression=exp.Literal.number(8))
    node.replace(made)
    return True


_TEXT_CALLS = {"substring", "substr", "left", "right"}


def _repair(tree: exp.Expression, text: str, details: dict) -> bool:
    """Add the one conversion DuckDB asked for. False when the complaint is not one we understand."""
    name, call, pos = details.get("name"), details.get("call"), int(details.get("position") or -1)
    ambiguous = re.search(r'function call "([^"(]+)(\(.*?\))"', details.get("exception_message", ""))
    if not name and ambiguous:
        name, call = ambiguous.group(1), ambiguous.group(1) + ambiguous.group(2)
    if not name or not call or pos < 0:
        return False
    types = _call_types(call)
    types = ["VARCHAR" if t == "JSON" else t for t in types]  # a VARIANT converts the same way
    if name.lower() == "to_timestamp" and len(types) == 1 and types[0].startswith(("TIMESTAMP", "DATE")):
        # TO_TIMESTAMP of something that already is a point in time gives it back (as TIMESTAMP_NTZ)
        for node in tree.find_all(exp.Func):
            sql = node.sql(dialect="duckdb")
            args = list(node.iter_expressions())
            if sql.lower().startswith("to_timestamp(") and text.startswith(sql, pos) and len(args) == 1:
                node.replace(exp.cast(args[0].copy(), "timestamp"))
                return True
        return False
    if name.lower() in _TEXT_CALLS and any(t != "VARCHAR" for t in types):
        # SUBSTRING(number_column, REGEXP_INSTR(...), ...): Snowflake reads the number as text, and
        # takes any whole number for a position
        for node in tree.find_all(exp.Func):
            sql = node.sql(dialect="duckdb")
            if not sql.lower().startswith(name.lower() + "(") or not text.startswith(sql, pos):
                continue
            children = list(node.iter_expressions())  # in the order they are written
            if len(children) != len(types):
                continue
            done = False
            for i, (child, kind) in enumerate(zip(children, types)):
                wanted = "varchar" if i == 0 else "bigint"
                if kind != wanted.upper() and not (i > 0 and kind in ("INTEGER", "SMALLINT", "TINYINT")):
                    child.replace(exp.cast(child.copy(), wanted))
                    done = True
            if done:
                return True
        return False
    if "BLOB" in types and name.lower() in _BINARY_CALLS:
        return _binary_call(tree, text, name.lower(), pos)
    if "VARCHAR" not in types:
        return False
    if name in _OPERATORS_BY_SIGN:
        temporal = any(t.startswith(_DATE_TYPES) for t in types)
        for node in tree.find_all(_OPERATORS_BY_SIGN[name]):
            sql = node.sql(dialect="duckdb")
            sides = [node.this] if isinstance(node, exp.Neg) else [node.this, node.expression]
            left = sides[0].sql(dialect="duckdb")
            at_operator = len(sides) == 2 and text.startswith(sql, pos - len(left) - 1)
            if not (text.startswith(sql, pos) or at_operator) or len(sides) != len(types):
                continue
            for side, kind in zip(sides, types):
                if kind == "VARCHAR":
                    _convert(side, "timestamp" if temporal else "double")
            return True
        for node in tree.find_all(exp.DateAdd, exp.DateSub, exp.TsOrDsAdd, exp.TimestampAdd, exp.DatetimeAdd,
                                  exp.TimestampSub, exp.DatetimeSub, exp.TimeAdd, exp.TimeSub):
            sql, left = node.sql(dialect="duckdb"), node.this.sql(dialect="duckdb")
            if sql.startswith(left) and (text.startswith(sql, pos) or text.startswith(sql, pos - len(left) - 1)) \
                    and types[0] == "VARCHAR":
                _convert(node.this, "timestamp")  # DATEADD(day, 1, text_column)
                return True
        return False
    if name.lower() == "timezone":
        for node in tree.find_all(exp.AtTimeZone, exp.ConvertTimezone):
            sql = node.sql(dialect="duckdb")
            operand = node.args.get("timestamp") if isinstance(node, exp.ConvertTimezone) else node.this
            start = text.find(sql)
            if operand is not None and start >= 0 and start <= pos <= start + len(sql) and not isinstance(operand, exp.Cast):
                _convert(operand, "timestamp")  # CONVERT_TIMEZONE on a text column
                return True
    lowered = name.lower()
    to = "double" if lowered in _NUMERIC_CALLS else "timestamp" if lowered in _DATE_CALLS else None
    if to is None:
        return False
    for node in tree.find_all(exp.Func):
        sql = node.sql(dialect="duckdb")
        if not sql.lower().startswith(lowered + "(") or not text.startswith(sql, pos):
            continue
        parts = _split_args(sql[len(lowered) + 1:-1])
        if len(parts) != len(types):
            continue
        children = {c.sql(dialect="duckdb"): c for c in node.iter_expressions()}
        done = False
        for part, kind in zip(parts, types):
            child = children.get(part) or children.get(part.split(" ORDER BY ")[0])
            if kind == "VARCHAR" and child is not None:
                _convert(child, to)
                done = True
        if done:
            return True
    return False


def _branches(node: exp.Expression) -> list[exp.Expression]:
    node = node.unnest()
    return _branches(node.left) + _branches(node.right) if isinstance(node, exp.SetOperation) else [node]


def _cte_prefix(node: exp.Expression) -> str:
    """The WITH clause that makes `node` bind on its own: the CTEs of every query around it."""
    withs, scope = [], node
    while scope is not None:
        if scope.args.get("with_") is not None:
            withs.append(scope.args["with_"])
        scope = scope.parent
    ctes = [c for w in reversed(withs) for c in w.expressions]
    if not ctes:
        return ""
    recursive = any(w.args.get("recursive") for w in withs)
    return "with " + ("recursive " if recursive else "") + ", ".join(c.sql(dialect="duckdb") for c in ctes) + " "


_NUMBER_TO_TIME = re.compile(r"Unimplemented type for cast \((?:U?(?:TINY|SMALL|BIG|HUGE)?INT(?:EGER)?|DECIMAL\([\d,]+\)|DOUBLE|FLOAT) -> (?:TIMESTAMP|DATE)")


def numbers_as_times(transformed: exp.Expression, message: str) -> exp.Expression | None:
    """`number_column::timestamp` counts seconds since 1970 in Snowflake; DuckDB has
    no such cast. Run again with those casts written out, after DuckDB named the problem."""
    if not _NUMBER_TO_TIME.search(message):
        return None
    named = re.search(r"when casting from source column (\S+)", message)
    changed = False
    for cast in list(transformed.find_all(exp.Cast)):
        if not cast.to.is_type("timestamp", "timestampntz", "timestampltz", "timestamptz", "date", "datetime"):
            continue
        operand = cast.this
        if not isinstance(operand, exp.Column) or (named and operand.name.upper() != named.group(1).upper().strip('"')):
            continue
        cast.set("this", _fn("_sf_seconds", operand.copy()))
        changed = True
    return transformed if changed else None


_OFFSET_LITERAL = re.compile(r"^(\d{4}-\d\d-\d\d[ T]\d\d:\d\d(?::\d\d(?:\.\d+)?)?) ?([+-]\d\d):?(\d\d)$")


def offsets_as_written(transformed: exp.Expression, message: str) -> exp.Expression | None:
    """'2024-01-01 00:00:00.000 +0000' is a timestamp with an offset to Snowflake. DuckDB wants it
    as +00:00, without the space: write such literals that way once it has refused one."""
    if "Unknown TimeZone" not in message:
        return None
    changed = False
    for literal in list(transformed.find_all(exp.Literal)):
        m = _OFFSET_LITERAL.match(literal.name) if literal.is_string else None
        if m and literal.name != f"{m.group(1)}{m.group(2)}:{m.group(3)}":
            literal.replace(exp.Literal.string(f"{m.group(1)}{m.group(2)}:{m.group(3)}"))
            changed = True
    return transformed if changed else None


def shared_aliases(transformed: exp.Expression, conn, limit: int = 20_000) -> exp.Expression:  # noqa: ANN001
    """`a + 1 AS b, b * 2 AS c, b + c AS d`: Snowflake computes each column once. DuckDB writes every
    reference out again, so a long chain of columns built on each other (scores made of weights
    made of scores) grows until the statement never plans. Where a select would grow that large,
    the columns others build on are computed once each, in nested subqueries, level by level."""
    for select in list(transformed.find_all(exp.Select)):
        if select.args.get("group") or select.args.get("distinct") or select.args.get("from_") is None \
                or select.args.get("having") or select.args.get("windows"):
            continue
        index: dict[str, int] = {}
        refs: list = []
        for i, projection in enumerate(select.expressions):  # every use counts: each one is written out again
            refs.append([index[c.name.upper()] for c in projection.find_all(exp.Column) if not c.table and c.name.upper() in index])
            if isinstance(projection, exp.Alias):
                index.setdefault(projection.alias.upper(), i)
        if not any(refs):
            continue
        size = [0] * len(select.expressions)
        for i, projection in enumerate(select.expressions):
            size[i] = sum(1 for _ in projection.walk()) + sum(size[j] for j in refs[i])
        if max(size) < limit:
            continue
        outside = [select.args[k] for k in ("qualify", "order") if select.args.get(k) is not None]
        if any(p.find(exp.Star, exp.Select) for p in [*select.expressions, *outside]):
            continue  # a star would show the helper columns; a subquery reads the tables by their names
        probe = exp.Select(expressions=[exp.Star()])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            real = {row[0].upper() for row in conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()}
        except Exception:  # noqa: BLE001 - binds only in its context: leave it as it is
            continue
        where = select.args.get("where")
        if where is not None and any(not c.table and c.name.upper() in index and c.name.upper() not in real for c in where.find_all(exp.Column)):
            continue  # filtered on a computed column
        # a table's column wins over an alias of the same name
        refs = [{index[c.name.upper()] for c in projection.find_all(exp.Column)
                 if not c.table and c.name.upper() in index and c.name.upper() not in real and index[c.name.upper()] < i}
                for i, projection in enumerate(select.expressions)]
        built_on = set().union(*refs)
        if not built_on or any(select.expressions[i].find(exp.Window, exp.AggFunc) for i in built_on):
            continue
        level: dict[int, int] = {}
        for i in sorted(built_on):
            level[i] = 1 + max((level[j] for j in refs[i]), default=-1)
        # the innermost query reads the tables: every column the select names, under a name of its own
        carried: dict[str, exp.Expression] = {}

        def carry(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node
            if node.table:
                key = node.sql(dialect="duckdb").upper()
                if key not in carried:
                    carried[key] = exp.alias_(node.copy(), f"__FXQ{len(carried)}", quoted=True)
                return exp.column(carried[key].alias, quoted=True)
            if node.name.upper() in real and node.name.upper() not in carried:
                carried[node.name.upper()] = node.copy()
            return node

        for holder in outside:
            holder.set("this", holder.this.transform(carry)) if isinstance(holder, exp.Qualify) else \
                holder.set("expressions", [e.transform(carry) for e in holder.expressions])
        columns = [exp.alias_(p.transform(carry), p.this.copy()) if isinstance(p, exp.Column) and p.table else p.transform(carry)
                   for p in select.expressions]  # `t.a` is still called A
        base = exp.Select(expressions=list(carried.values()))
        for key in ("from_", "joins", "where"):
            if select.args.get(key) is not None:
                base.set(key, select.args[key])
                select.set(key, None)
        for depth in range(max(level.values()) + 1):
            base = exp.Select(expressions=[exp.Star(), *[columns[i] for i in sorted(built_on) if level[i] == depth]]).from_(
                exp.Subquery(this=base, alias=exp.TableAlias(this=exp.to_identifier(f"__FXA{depth}", quoted=True))))
        select.set("expressions", [exp.column(c.alias) if i in built_on else c for i, c in enumerate(columns)])
        select.set("from_", exp.From(this=exp.Subquery(this=base, alias=exp.TableAlias(this=exp.to_identifier("__FXA", quoted=True)))))
    return transformed


def resolve_equalities(transformed: exp.Expression, conn) -> exp.Expression:  # noqa: ANN001
    """`_sf_eq(a, b)` compares a VARIANT with text by value. Inside a JOIN condition or a WHERE it also
    hides the equality from DuckDB, which then cannot hash-join and falls back to comparing every
    pair of rows. The types are known once the query binds: ask for them, and write each comparison
    out as the plain equality (or the explicit VARIANT-to-text one) it stands for."""
    for select in list(transformed.find_all(exp.Select)):
        calls = [n for n in select.find_all(exp.Anonymous)
                 if _is(n, "_sf_eq") and n.find_ancestor(exp.Select) is select and n.find_ancestor(exp.Join, exp.Where) is not None]
        if not calls or select.args.get("from_") is None:
            continue
        probe = exp.Select(expressions=[exp.alias_(side.copy(), f"_k{2 * i + j}") for i, call in enumerate(calls)
                                        for j, side in enumerate(call.expressions)])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            described = conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()
        except Exception:  # noqa: BLE001 - binds only in its context (a correlated subquery): leave it as it is
            continue
        if len(described) != 2 * len(calls):
            continue
        for i, call in enumerate(calls):
            left, right = call.expressions
            kinds = described[2 * i][1], described[2 * i + 1][1]
            value = lambda side: exp.Anonymous(this="json_extract_string", expressions=[side.copy(), exp.Literal.string("$")])  # noqa: E731
            if kinds[0] == "JSON" and kinds[1] != "JSON":
                same = exp.EQ(this=value(left), expression=exp.cast(right.copy(), "varchar"))
            elif kinds[1] == "JSON" and kinds[0] != "JSON":
                same = exp.EQ(this=exp.cast(left.copy(), "varchar"), expression=value(right))
            else:
                same = exp.EQ(this=left.copy(), expression=right.copy())
            call.replace(exp.paren(same))
    return transformed


def coerce_union_literals(transformed: exp.Expression, conn) -> exp.Expression:  # noqa: ANN001
    """`select created_at from t union all select '2025-01-04'`: Snowflake reads the text as a
    timestamp, because that is the column's type in the other branch. DuckDB makes the whole column
    text. Where one branch gives a string literal and another a typed value, cast the literal."""
    if not transformed.find(exp.SetOperation):
        return transformed
    for union in list(transformed.find_all(exp.SetOperation)):
        if isinstance(union.parent, exp.SetOperation) or (isinstance(union.parent, exp.Subquery) and isinstance(union.parent.parent, exp.SetOperation)):
            continue  # handled with the outermost operation of its chain
        prefix = _cte_prefix(union)  # the CTEs a branch can see: those of the union and of every query around it
        branches = [b for b in _branches(union) if isinstance(b, exp.Select)]
        if len(branches) < 2 or len({len(b.expressions) for b in branches}) != 1:
            continue
        literal_at: dict[int, list[exp.Expression]] = {}
        for branch in branches:
            for i, proj in enumerate(branch.expressions):
                value = proj.unalias()
                if isinstance(value, exp.Literal) and value.is_string:
                    literal_at.setdefault(i, []).append(value)
        if not literal_at:
            continue
        types: dict[int, str] = {}
        for branch in branches:
            if all(isinstance(v := branch.expressions[i].unalias(), exp.Literal) and v.is_string for i in literal_at):
                continue  # nothing to learn from this one
            try:
                described = conn.execute("describe " + prefix + branch.sql(dialect="duckdb")).fetchall()
            except Exception:  # noqa: BLE001 - a branch that only binds in its context: leave the union as it is
                continue
            for i in literal_at:
                kind = described[i][1] if i < len(described) else "VARCHAR"
                value = branch.expressions[i].unalias()
                if not (isinstance(value, exp.Literal) and value.is_string) and kind not in ("VARCHAR", "JSON", "BLOB", '"NULL"', "NULL"):
                    types.setdefault(i, kind)
        for i, kind in types.items():
            if not kind.startswith(("TIMESTAMP", "DATE", "TIME", "DECIMAL", "BIGINT", "INTEGER", "DOUBLE", "FLOAT", "HUGEINT", "SMALLINT",
                                    "TINYINT", "BOOLEAN", "UBIGINT", "UINTEGER")):
                continue
            for value in literal_at[i]:
                written = value.copy()
                m = _OFFSET_LITERAL.match(value.name) if kind.startswith(("TIMESTAMP", "DATE")) else None
                if m:  # '9999-12-31 00:00:00.000 +0000': DuckDB reads the offset as +00:00, without the space
                    written = exp.Literal.string(f"{m.group(1)}{m.group(2)}:{m.group(3)}")
                value.replace(exp.cast(written, exp.DataType.build(kind, dialect="duckdb")))
    return transformed


def days_added_to_aggregates(transformed: exp.Expression, conn) -> exp.Expression:  # noqa: ANN001
    """`dateadd(day, -2, max(received_at))`: `_sf_like` keeps a DATE a DATE by its overloads, but DuckDB cannot tell
    an aggregate's type when it expands the macro and takes the DATE one: the time of day is lost. For those, the
    argument's type is asked and the call written out (a DATE cast back, anything else as it is)."""
    name = _NS + "_sf_like"
    calls = [c for c in transformed.find_all(exp.Anonymous) if c.name == name and len(c.expressions) == 2
             and c.expressions[0].find(exp.AggFunc, exp.Window, exp.Subquery) is not None]
    for call in calls:
        value, added = call.expressions
        select = call.find_ancestor(exp.Select)
        probe = exp.Select(expressions=[exp.alias_(value.copy(), "_k")])
        if select is not None and select.args.get("from_") is not None:
            probe.set("from_", select.args["from_"].copy())
            if select.args.get("joins"):
                probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            kind = conn.execute("describe " + (_cte_prefix(select) if select is not None else "") + probe.sql(dialect="duckdb")).fetchall()[0][1]
        except Exception:  # noqa: BLE001, S112 - it reads the query around it: left to the overloads
            continue
        call.replace(exp.cast(added.copy(), "date") if str(kind).upper() == "DATE" else added.copy())
    return transformed


MIXED_CASE: set[str] = set()  # the names, in lower case, of columns somewhere named in other than upper case


def learn_names(transformed: exp.Expression, conn=None) -> None:  # noqa: ANN001
    """Keep in MIXED_CASE the names a statement may give columns in other than upper case (a quoted name), and,
    given a connection, those of the columns already there."""
    if conn is not None:
        MIXED_CASE.update(name.lower() for (name,) in conn.execute(
            "select distinct column_name from duckdb_columns() where column_name <> upper(column_name) and not internal "
            "and database_name not in ('system', 'temp') and lower(schema_name) not in ('information_schema', 'pg_catalog') "
            "and not starts_with(lower(database_name), '_fs') and not starts_with(lower(schema_name), '_fs')").fetchall())
    if isinstance(transformed, (exp.Create, exp.Alter)):
        MIXED_CASE.update(i.this.lower() for i in transformed.find_all(exp.Identifier) if i.quoted and i.this != i.this.upper())


def misnamed_column(transformed: exp.Expression, conn, probe: bool = True) -> str | None:  # noqa: ANN001
    """Snowflake's compilation errors for names that DuckDB resolves where Snowflake does not:

    * a quoted name is that name in its case, an unquoted one upper case: `"Metric_Name"` is not the column
      METRIC_NAME, nor `metric_name` the column "Metric_Name" (DuckDB matches names in any case);
    * a name that a subquery gives twice is ambiguous, `select id from (select 1 as id, * from t)` with an ID in t
      (DuckDB takes the first).

    The columns of the relations a name may come from are followed through tables, subqueries, CTEs, `*`, VALUES
    and FLATTEN. `probe`: the columns of a table may be asked by reading none of its rows (not in a transaction,
    which a failed statement would abort)."""
    from sqlglot.optimizer.scope import Scope, traverse_scope

    def target(ident: exp.Identifier) -> str:
        return ident.this if ident.quoted else ident.this.upper()

    def starred(e: exp.Expression) -> bool:
        return isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))

    refs = [c for c in transformed.find_all(exp.Column) if isinstance(c.this, exp.Identifier) and c.this.this
            and not c.this.this.lower().startswith(("_fs", "_fx", "_sf"))]
    said = {i.this.lower() for i in transformed.find_all(exp.Identifier) if i.quoted and i.this != i.this.upper()}
    cased = any(target(c.this) != c.this.this.upper() or c.this.this.lower() in MIXED_CASE or c.this.this.lower() in said
                for c in refs)
    twice = any((s is not transformed or s.args.get("order")) and len(s.expressions) > 1 and any(starred(e) for e in s.expressions)
                for s in transformed.find_all(exp.Select)) or any(
        len(names) != len(set(names)) for s in transformed.find_all(exp.Select) if s is not transformed
        for names in [[e.alias_or_name.upper() for e in s.expressions if not starred(e)]])
    if not (cased or twice) or (isinstance(transformed, exp.Create) and isinstance(transformed.this, exp.Table)
                                and transformed.this.name.upper() == "MERGE_CANDIDATES"):
        return None  # (the common statement; or fakesnow's own for a MERGE, which repeats columns by design)
    internal = ("_fs", "information_schema", "pg_catalog", "duckdb_", "system", "temp")
    if any(p.lower().startswith(internal) for t in transformed.find_all(exp.Table) for p in (t.name, t.db, t.catalog) if p):
        return None  # (the emulator's own statements, over DuckDB's catalog: its names are lower case)

    looked: dict[str, list[str] | None] = {}

    def stored(table: exp.Table) -> list[str] | None:
        bare = exp.Table(this=table.this.copy(), db=table.args.get("db"), catalog=table.args.get("catalog")).sql(dialect="duckdb")
        if bare not in looked:
            if probe:
                try:
                    looked[bare] = [d[0] for d in conn.execute(f"select * from {bare} limit 0").description]
                except Exception:  # noqa: BLE001 - not a table this connection can read: not checked
                    looked[bare] = None
            else:
                rows = conn.execute(
                    "select column_name from duckdb_columns() where lower(table_name) = lower(?) "
                    "and lower(schema_name) = lower(coalesce(?, current_schema())) "
                    "and lower(database_name) = lower(coalesce(?, current_database())) order by column_index",
                    [table.name, table.db or None, table.catalog or None]).fetchall()
                looked[bare] = [name for (name,) in rows] or None
        return looked[bare]

    scopes = traverse_scope(transformed)
    scope_of = {id(sc.expression): sc for sc in scopes}

    def outputs(node: exp.Expression) -> list[str] | None:
        """The names of the columns a query gives, in order and with repeats, or None when they cannot be told."""
        if isinstance(node, exp.SetOperation):
            return outputs(node.left)
        if isinstance(node, exp.Subquery):
            return outputs(node.this)
        if isinstance(node, exp.Values):
            alias = node.args.get("alias")
            listed = alias.columns if isinstance(alias, exp.TableAlias) else []
            width = len(node.expressions[0].expressions) if node.expressions and isinstance(node.expressions[0], exp.Tuple) else 1
            return [target(c) for c in listed] if listed else [f"COLUMN{i + 1}" for i in range(width)]
        if not isinstance(node, exp.Select):
            return None
        names: list[str] = []
        for e in node.expressions:
            if isinstance(e, exp.Alias) and isinstance(e.args.get("alias"), exp.Identifier):
                names.append(target(e.args["alias"]))
            elif isinstance(e, exp.Column) and isinstance(e.this, exp.Identifier):
                names.append(target(e.this))
            elif starred(e):
                if e.args.get("except_") or e.args.get("replace") or e.args.get("rename") or (
                        isinstance(e, exp.Column) and (e.this.args.get("except_") or e.this.args.get("replace"))):
                    return None
                sc = scope_of.get(id(node))
                joins = node.args.get("joins") or []
                wanted = e.table.upper() if isinstance(e, exp.Column) and e.table else None
                if sc is None or any(j.args.get("kind") == "NATURAL" or j.args.get("method") == "NATURAL" for j in joins):
                    return None
                # (an unqualified `*` over a USING join gives the columns joined on once; `a.*, b.*` gives them twice)
                shared = {target(u) if isinstance(u, exp.Identifier) else u.name.upper()
                          for j in joins for u in j.args.get("using") or []} if not wanted else set()
                given: set[str] = set()
                for alias, (_n, src) in sc.selected_sources.items():
                    if wanted and alias.upper() != wanted:
                        continue
                    more = known(src)
                    if more is None:
                        return None
                    for n in more:
                        if n in shared and n in given:
                            continue
                        given.add(n)
                        names.append(n)
            else:
                names.append("")  # an expression without a name of its own: a name no one asks for
        return names

    def known(source) -> list[str] | None:  # noqa: ANN001
        flatten = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]
        if isinstance(source, Scope):
            inner = source.expression
            call = inner.this if isinstance(inner, exp.Table) else None
            if isinstance(call, exp.Func) and re.search("flatten|explode", call.name if isinstance(call, exp.Anonymous) else call.sql_name(), re.I):
                return flatten
            return outputs(inner)
        if isinstance(source, exp.Table):
            alias = source.args.get("alias")
            if isinstance(alias, exp.TableAlias) and alias.columns:
                return [target(c) for c in alias.columns]
            if not isinstance(source.this, exp.Identifier):  # a table function
                name = (source.this.name if isinstance(source.this, exp.Anonymous) else source.this.sql_name()) if isinstance(source.this, exp.Func) else ""
                return flatten if "flatten" in name.lower() else None
            return stored(source)
        if isinstance(source, (exp.Values, exp.Subquery)):
            return outputs(source)
        return None

    def where(ref: exp.Column) -> str:
        meta = ref.this.meta or {}
        if "line" in meta and "col" in meta:
            return f"error line {meta['line']} at position {max(0, meta['col'] - len(ref.this.this) - (2 if ref.this.quoted else 0))}\n"
        return ""

    for scope in scopes:
        sources: dict[str, list[str] | None] = {}
        for name, (_node, source) in scope.selected_sources.items():
            try:
                sources[name.upper()] = known(source)
            except Exception:  # noqa: BLE001 - a relation that cannot be looked up is not checked
                sources[name.upper()] = None
        lateral = [target(e.args["alias"]) for e in getattr(scope.expression, "expressions", [])
                   if isinstance(e, exp.Alias) and isinstance(e.args.get("alias"), exp.Identifier)]
        order = scope.expression.args.get("order") if isinstance(scope.expression, exp.Select) else None
        if order is not None:  # ORDER BY reads the select's own columns first
            given = outputs(scope.expression)
            for ref in order.find_all(exp.Column):
                if given is not None and not ref.table and isinstance(ref.this, exp.Identifier) \
                        and ref.find_ancestor(exp.Select) is scope.expression and given.count(target(ref.this)) > 1:
                    return f"SQL compilation error: ambiguous column name '{target(ref.this)}'"
        for ref in scope.columns:
            if not isinstance(ref.this, exp.Identifier) or ref.this.this.lower().startswith(("_fs", "_fx", "_sf")):
                continue
            wanted = target(ref.this)
            if ref.table:
                places = [sources.get(ref.table.upper())] if ref.table.upper() in sources else [None]
            else:
                # (an expression does not read its own alias: `"current"::number as "current"` reads a column)
                own = next((e for e in getattr(scope.expression, "expressions", []) if ref is e or e.find(exp.Column) is not None
                            and any(c is ref for c in e.find_all(exp.Column))), None)
                mine_alias = target(own.args["alias"]) if isinstance(own, exp.Alias) and isinstance(own.args.get("alias"), exp.Identifier) else None
                places = [*sources.values(), [a for a in lateral if a != mine_alias]]
            if not places or any(p is None for p in places):
                continue
            having = [p for p in places if wanted in p]
            if len(having) == 1 and having[0] is not lateral and having[0].count(wanted) > 1 and wanted not in lateral:
                return f"SQL compilation error: ambiguous column name '{wanted}'"
            if having or not any(wanted.lower() == n.lower() for p in places for n in p):
                continue  # (found; or not a name of these relations in any case: an outer query's, left to DuckDB)
            shown = '"' + ref.this.this + '"' if ref.this.quoted else ref.this.this.upper()
            return f"SQL compilation error: {where(ref)}invalid identifier '{shown}'"
    made = transformed.expression if isinstance(transformed, exp.Create) else None
    while isinstance(made, (exp.Subquery, exp.Paren)):
        made = made.this  # (`create table t as (select ...)`)
    if isinstance(transformed, exp.Create) and str(transformed.args.get("kind", "")).upper() in ("TABLE", "VIEW") \
            and isinstance(made, exp.Select):
        given = outputs(made)  # a table or a view has each column once
        named = [n for n in given or [] if n]
        doubled = next((n for i, n in enumerate(named) if n in named[:i]), None)
        if doubled is not None:
            return f"SQL compilation error:\nduplicate column name '{doubled}'"
    return None


_CONVERSION = re.compile(r"_(?:sf|fs)_(?:to_(?!.*try)\w+|int|integer|text|number\w*|decimal\w*|double|float|bool\w*|date|time\w*)", re.I)


STOPPED = exp.Null()  # (pruned_nulls: a view a filter is not pushed into)


def pruned_nulls(transformed: exp.Expression, conn) -> exp.Expression | None:  # noqa: ANN001
    """`select id from orders_v where id is null`, with orders_v converting text to a number (`order_id::number as id`)
    and text that is no number in it: Snowflake answers no rows rather than its conversion error. A conversion is NULL
    only when what it converts is, and each micro-partition knows how many NULLs a column holds: a column with none
    leaves no partition to read, and the conversion is never made. DuckDB makes it, on every row. Seen through views,
    subqueries and CTEs, as Snowflake pushes the filter down (not past a LIMIT, nor to the side of an outer join
    that adds NULLs). The predicate is then false; returns the statement so, or None when no predicate is."""
    from sqlglot.optimizer.scope import Scope, traverse_scope

    def called(node: exp.Expression) -> tuple[str, list] | None:
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Anonymous):
            node = node.expression
        if isinstance(node, exp.Anonymous):
            return str(node.name).split(".")[-1], list(node.expressions)
        return None

    def argument(node: exp.Expression) -> exp.Expression:
        call = called(node)  # (text read as a number first: `_sf_num_text(x)`, still x)
        return call[1][0] if call and call[0].lower() == "_sf_num_text" and call[1] else node

    def conversion(node: exp.Expression) -> exp.Expression | None:
        """What a conversion converts, when it is one of a column."""
        while isinstance(node, exp.Paren):
            node = node.this
        if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and not node.args.get("safe"):
            inner = argument(node.this)
            return inner if isinstance(inner, exp.Column) else None
        if isinstance(node, exp.Case) and node.args.get("default") is not None:
            # TO_NUMBER(x) is written `CASE WHEN <x read as a number> IS NOT NULL THEN CAST(...) ELSE CAST(x ...) END`
            results = [conversion(i.args["true"]) for i in node.args.get("ifs") or []] + [conversion(node.args["default"])]
            tests = [c for i in node.args.get("ifs") or [] for c in i.this.find_all(exp.Column)]
            names = {r.sql() if r is not None else None for r in results} | {c.sql() for c in tests}
            return results[0] if len(names) == 1 and None not in names else None
        call = called(node)
        if call and _CONVERSION.fullmatch(call[0]) and call[1] and isinstance(argument(call[1][0]), exp.Column):
            return argument(call[1][0])
        return None

    views: dict[str, exp.Expression | None] = {}

    def view(table: exp.Table) -> exp.Expression | None:
        key = table.sql(dialect="duckdb")
        if key not in views:
            row = conn.execute(
                "select sql from duckdb_views() where lower(view_name) = lower(?) and not internal "
                "and lower(schema_name) = lower(coalesce(?, current_schema())) "
                "and lower(database_name) = lower(coalesce(?, current_database()))",
                [table.name, table.db or None, table.catalog or None]).fetchone()
            body = None
            if row:
                parsed = sqlglot.parse_one(row[0], read="duckdb")
                body = parsed.expression if isinstance(parsed, exp.Create) else None
                if isinstance(body, exp.Query) and (body.args.get("with_") is not None or _fenced(body)) and conn.execute(
                        f"select count(*) from {_NS}_fx_secure_views where view_name = upper(?) "
                        "and schema_name = upper(coalesce(?, current_schema())) and database_name = upper(coalesce(?, current_database()))",
                        [table.name, table.db or None, table.catalog or None]).fetchone()[0]:
                    body = STOPPED  # (measured: a filter goes no further into a secure view that is a WITH)
                if body is not None and table.catalog:  # (its tables are named as the view's database sees them)
                    for t in body.find_all(exp.Table):
                        if t.db and not t.catalog:
                            t.set("catalog", exp.to_identifier(table.catalog))
            views[key] = body
        return views[key]

    def none_null(table: exp.Table, column: str) -> bool:
        bare = exp.Table(this=table.this.copy(), db=table.args.get("db"), catalog=table.args.get("catalog"))
        (nulls,) = conn.execute(f"select count(*) from {bare.sql(dialect='duckdb')} where {exp.column(column, quoted=True).sql(dialect='duckdb')} is null").fetchone()
        return nulls == 0

    def outer_side(scope: Scope, alias: str) -> bool:
        """The relation `alias` stands on a side of an outer join that is filled with NULLs."""
        select = scope.expression
        joins = select.args.get("joins") or [] if isinstance(select, exp.Select) else []
        first = select.args.get("from_").this.alias_or_name if isinstance(select, exp.Select) and select.args.get("from_") else None
        for j in joins:
            side = (j.args.get("side") or "").upper()
            if side in ("LEFT", "FULL") and j.this.alias_or_name.upper() == alias.upper():
                return True
            if side in ("RIGHT", "FULL") and first and first.upper() == alias.upper():
                return True
        return False

    def follow(column: exp.Column, scope: Scope, converted: bool, depth: int = 0) -> bool:
        """The column, read in `scope`, is (after a conversion when `converted`) one of a table with no NULLs."""
        if depth > 30:
            return False
        sources = scope.selected_sources
        if column.table:
            picked = [(a, s) for a, (_n, s) in sources.items() if a.upper() == column.table.upper()]
        else:
            picked = [(a, s) for a, (_n, s) in sources.items()]
        if len(picked) != 1 or outer_side(scope, picked[0][0]):
            return False
        _alias, source = picked[0]
        name = column.name
        if isinstance(source, exp.Table):
            body = view(source)
            if body is STOPPED:
                return False
            if body is None:
                return converted and isinstance(source.this, exp.Identifier) and none_null(source, name)
            inner = traverse_scope(body)
            return bool(inner) and through(name, inner[-1], converted, depth + 1)
        if isinstance(source, Scope):
            return through(name, source, converted, depth + 1)
        return False

    def through(name: str, scope: Scope, converted: bool, depth: int) -> bool:
        """The column `name` that `scope` gives is (after a conversion when `converted`) one with no NULLs."""
        select = scope.expression
        # (measured: a filter goes no further down than a LIMIT, a GROUP BY, a window whose result is read, or a
        # QUALIFY whose windows are not partitioned by the column filtered)
        if not isinstance(select, exp.Select) or select.args.get("limit") or select.args.get("offset") or select.args.get("fetch") \
                or select.args.get("group"):
            return False
        windowed = {e.alias_or_name.upper() for e in select.expressions if e.find(exp.Window)}
        if windowed & {c.name.upper() for c in tree.find_all(exp.Column) if c.find_ancestor(exp.Select) is not select}:
            return False
        qualify = select.args.get("qualify")
        if qualify is not None:
            source_name = next((e.this.name if isinstance(e, exp.Alias) and isinstance(e.this, exp.Column) else e.name
                                for e in select.expressions if e.alias_or_name.upper() == name.upper()), name)
            if not all(any(isinstance(p, exp.Column) and p.name.upper() == source_name.upper() for p in w.args.get("partition_by") or [])
                       for w in qualify.find_all(exp.Window)):
                return False
        for e in select.expressions:
            if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
                continue
            if e.alias_or_name.upper() != name.upper():
                continue
            value = e.this if isinstance(e, exp.Alias) else e
            if isinstance(value, exp.Column):
                return follow(value, scope, converted, depth)
            inner = conversion(value)
            return inner is not None and not converted and follow(inner, scope, True, depth)
        if any(isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)) for e in select.expressions):
            return follow(exp.column(name), scope, converted, depth)
        return False

    tree = transformed.copy()
    body = tree.expression if isinstance(tree, (exp.Create, exp.Insert)) and isinstance(tree.expression, exp.Query) else tree
    changed = False
    for scope in traverse_scope(body):
        where = scope.expression.args.get("where") if isinstance(scope.expression, exp.Select) else None
        if where is None:
            continue
        for test in list(where.find_all(exp.Is)):
            # (IS NULL in this select's own WHERE, not negated: NOT (x IS NULL) reads every partition with a value)
            if not isinstance(test.expression, exp.Null) or test.find_ancestor(exp.Select) is not scope.expression \
                    or isinstance(test.find_ancestor(exp.Not, exp.Select), exp.Not):
                continue
            operand = test.this
            try:
                inner = conversion(operand)
                found = follow(inner, scope, True) if inner is not None else (
                    isinstance(operand, exp.Column) and follow(operand, scope, False))
            except Exception:  # noqa: BLE001 - what cannot be followed is read as written
                found = False
            if found:
                test.replace(exp.false())
                changed = True
    return tree if changed else None


_TO_NUMBER = ("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "BOOLEAN")


def refused_conversions(transformed: exp.Expression, conn) -> tuple[int, str] | None:  # noqa: ANN001
    """Snowflake refuses, when it compiles a statement, one that converts a date, a time or a timestamp to a number
    (`uploaded_at::float`, `to_numeric(created_at)`) or adds one to a date (`dateadd('s', uploaded_at, ...)`), whatever
    the rows. DuckDB fails only when it converts a value: a view would be created and fail where it is read, and a
    query over NULLs or no rows would answer. A copy of the query, each conversion guarded by a macro DuckDB binds
    for the types Snowflake converts only, is bound (not run): the error Snowflake gives, or None."""
    body = transformed.expression if isinstance(transformed, (exp.Create, exp.Insert)) else transformed
    if not isinstance(body, exp.Query):
        return None
    guard = _NS + "_fx_convertible"

    def guard_of(node: exp.Expression) -> str:
        """The macro that refuses what `node` cannot take: a TRY_ function takes a date for a DATE, a binary for a BINARY."""
        name = str(node.name).split(".")[-1].lower() if isinstance(node, exp.Anonymous) else ""
        if name == "_sf_try_input":
            target = node.expressions[1].name.upper() if len(node.expressions) > 1 else ""
            return _NS + ("_fx_try_date" if target == "DATE" else "_fx_try_binary" if target == "BLOB" else "_fx_convertible")
        if name in ("_fs_to_timestamp", "_sf_to_date"):
            return _NS + "_fx_no_float"
        return guard

    def argument(node: exp.Expression) -> tuple[exp.Expression, str] | None:
        if isinstance(node, exp.Anonymous) and str(node.name).split(".")[-1].lower() == "_sf_try_input" and node.expressions:
            return node.expressions[0], "TRY_CAST"
        if isinstance(node, exp.Anonymous) and str(node.name).split(".")[-1].lower() in ("_fs_to_timestamp", "_sf_to_date") \
                and node.expressions:
            value = node.expressions[0]
            if isinstance(value, exp.Anonymous) and str(value.name).split(".")[-1].lower() == "_sf_seconds" and value.expressions:
                value = value.expressions[0]  # (a literal number of seconds, read exactly)
            if value.find(exp.Div, exp.Avg) or any("div" in str(a.name).lower() for a in value.find_all(exp.Anonymous)):
                return None  # (NUMBER / NUMBER is a NUMBER in Snowflake; the emulator works it out as a DOUBLE)
            return value, "TO_TIMESTAMP"
        if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and not node.args.get("safe") \
                and node.to.this.name.upper() in _TO_NUMBER:
            name = "TO_BOOLEAN" if node.to.this == exp.DataType.Type.BOOLEAN else "TO_DOUBLE" if node.to.this in (
                exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT) else "TO_NUMBER"
            return node.this, name
        if isinstance(node, exp.Interval) and node.this is not None and not isinstance(node.this, exp.Literal):
            return node.this, "DATEADD"
        if isinstance(node, (exp.DateAdd, exp.TimestampAdd, exp.TsOrDsAdd, exp.DatetimeAdd, exp.TimeAdd)) and node.expression is not None:
            return node.expression, "DATEADD"
        if isinstance(node, exp.Anonymous) and str(node.name).split(".")[-1].lower() in ("_sf_int", "_sf_bool") and node.expressions:
            return node.expressions[0], "TO_BOOLEAN" if str(node.name).lower().endswith("bool") else "TO_NUMBER"
        return None

    kinds = (exp.Cast, exp.Interval, exp.Anonymous, exp.DateAdd, exp.TimestampAdd, exp.TsOrDsAdd, exp.DatetimeAdd, exp.TimeAdd)
    candidates = [(n, *a) for n in body.find_all(*kinds)
                  if (a := argument(n)) is not None and not isinstance(a[0], (exp.Literal, exp.Null))]
    if not candidates:
        return None

    def guarded(only=None) -> str:  # noqa: ANN001
        copy = body.copy()
        marks = {id(n) for n, _a, _ in candidates if only is None or n is only}
        # (the copy's nodes are found again by their place in the same walk; the deepest are guarded first, so
        # that a conversion inside another is guarded inside the other's guard)
        pairs = list(zip(body.walk(), copy.walk()))
        for original, copied in reversed(pairs):
            if id(original) in marks:
                arg = argument(copied)[0]
                replaced = exp.Case(ifs=[exp.If(this=exp.Anonymous(this=guard_of(copied), expressions=[arg.copy()]), true=copied.copy())])
                if copied is copy:
                    copy = replaced
                else:
                    copied.replace(replaced)
        return copy.sql(dialect="duckdb")

    try:
        conn.execute("describe " + guarded())
        return None
    except Exception as e:  # noqa: BLE001
        if "_fx_refused_conversion" not in str(e).lower():
            return None
    for node, _arg, name in candidates:  # which conversion it is
        try:
            conn.execute("describe " + guarded(node))
        except Exception as e:  # noqa: BLE001
            if "_fx_refused_conversion" not in str(e).lower():
                continue
            if name == "DATEADD":
                return 1044, "SQL compilation error:\nInvalid argument types for function 'DATEADD'"
            if name == "TRY_CAST":
                return 1065, "SQL compilation error:\nFunction TRY_CAST cannot be used with arguments of these types"
            return 1007, f"SQL compilation error:\ninvalid type [{node.sql(dialect='snowflake')}] for parameter '{name}'"
    return 1007, "SQL compilation error:\ninvalid type for a conversion"


def union_text_kinds(transformed: exp.Expression, conn) -> exp.Expression:  # noqa: ANN001
    """`select n from a union all select t from b` with `n` a number and `t` text: Snowflake gives the column the
    number's own type (also a date's, a timestamp's, a boolean's) and text that is not one is an error. DuckDB makes
    the column text. Common with `dbt_utils.star` over two tables whose columns are in other orders."""
    if not transformed.find(exp.SetOperation):
        return transformed
    for union in list(transformed.find_all(exp.SetOperation)):
        if isinstance(union.parent, exp.SetOperation) or (isinstance(union.parent, exp.Subquery) and isinstance(union.parent.parent, exp.SetOperation)):
            continue
        branches = [b for b in _branches(union) if isinstance(b, exp.Select)]
        starred = lambda p: isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))  # noqa: E731
        # (a branch is either all star, `select * from x`, whose columns are converted in place, or lists no star)
        whole = {id(b) for b in branches if len(b.expressions) == 1 and isinstance(b.expressions[0], exp.Star)
                 and not b.expressions[0].args.get("replace") and not b.expressions[0].args.get("except_")}
        if len(branches) < 2 or len(branches) != len(_branches(union)) \
                or any(starred(p) for b in branches if id(b) not in whole for p in b.expressions) \
                or len({len(b.expressions) for b in branches if id(b) not in whole}) > 1:
            continue
        prefix = _cte_prefix(union)
        try:
            described = [[(str(r[0]), str(r[1]).upper()) for r in conn.execute("describe " + prefix + b.sql(dialect="duckdb")).fetchall()]
                         for b in branches]
        except Exception:  # noqa: BLE001, S112 - a branch that only binds in its context: left as it is
            continue
        width = {len(d) for d in described}
        if len(width) != 1 or any(len(d) != len(b.expressions) for b, d in zip(branches, described) if id(b) not in whole):
            continue
        replaced: dict[int, list[exp.Expression]] = {}
        for i in range(width.pop()):
            kinds = [d[i][1] for d in described]
            if "VARCHAR" not in kinds or "JSON" in kinds:
                continue
            typed = [k for k in kinds if k.startswith((*_NUMERIC_KINDS, "DECIMAL", "DOUBLE", "FLOAT", "TIMESTAMP", "DATE", "BOOLEAN"))]
            if not typed:
                continue
            target = exp.DataType.build("DOUBLE" if any(k.startswith(("DOUBLE", "FLOAT")) for k in typed) else typed[0], dialect="duckdb")
            for branch, (column, kind) in zip(branches, (d[i] for d in described)):
                if kind != "VARCHAR":
                    continue
                if id(branch) in whole:
                    name = exp.to_identifier(column, quoted=True)
                    replaced.setdefault(id(branch), []).append(exp.alias_(exp.cast(exp.column(name), target), name))
                    continue
                proj = branch.expressions[i]
                value = proj.unalias()
                if isinstance(value, (exp.Null, exp.Literal)):
                    continue  # (a text literal: coerce_union_literals)
                cast = exp.cast(value.copy(), target)
                proj.replace(exp.alias_(cast, proj.alias_or_name, quoted=True) if proj.alias_or_name else cast)
        for branch in branches:
            if replaced.get(id(branch)):  # select * replace (cast("x" as double) as "x") from ...
                branch.expressions[0].set("replace", replaced[id(branch)])
    return transformed


_NUMERIC_KINDS = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
                  "DOUBLE", "FLOAT", "DECIMAL")


def _kinds_past_aliases(select: exp.Select, calls: list, conn) -> tuple[list, list[str]]:  # noqa: ANN001
    """The kinds of the arguments that read aliases of their own SELECT, the aliases asked along. Only aliases made
    of columns, not of other aliases (a chain of them would be written out many times over)."""
    source = exp.Select(expressions=[exp.Star()])
    source.set("from_", select.args["from_"].copy())
    if select.args.get("joins"):
        source.set("joins", [j.copy() for j in select.args["joins"]])
    try:  # a name that is a column of what the SELECT reads is that column, also where an alias has the name
        columns = {str(row[0]).upper() for row in conn.execute("describe " + _cte_prefix(select) + source.sql(dialect="duckdb")).fetchall()}
    except Exception:  # noqa: BLE001
        return [], []
    aliases = {e.alias.upper(): e for e in select.expressions if isinstance(e, exp.Alias) and e.alias.upper() not in columns}

    def reads(e: exp.Expression) -> set[str]:
        return {c.name.upper() for c in e.find_all(exp.Column) if not c.table and c.name.upper() in aliases}

    kept, needed = [], {}
    for args in calls:
        named = set().union(*(reads(a) for a in args))
        if any(reads(aliases[n].this) for n in named):
            continue
        kept.append(args)
        needed.update({n: aliases[n] for n in named})
    flat = [a for args in kept for a in args]
    if not flat:
        return [], []
    probe = exp.Select(expressions=[e.copy() for e in needed.values()] + [exp.alias_(a.copy(), f"_k{i}") for i, a in enumerate(flat)])
    probe.set("from_", select.args["from_"].copy())
    if select.args.get("joins"):
        probe.set("joins", [j.copy() for j in select.args["joins"]])
    try:
        rows = conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()
    except Exception:  # noqa: BLE001 - binds only in its place: left as it is
        return [], []
    return kept, [str(row[1]).upper() for row in rows][len(needed):]


def mixed_kinds(transformed: exp.Expression, conn) -> exp.Expression:  # noqa: ANN001
    """`coalesce(licensed_seats, quantity)` with the one text and the other a number: Snowflake takes the number's
    type, and text that is not a number is an error. DuckDB takes text, and the value goes on as it is. The same for
    a date or a timestamp beside text (the text is read as one), and the other way round for a boolean beside text
    (Snowflake keeps the text). Asked of DuckDB one SELECT at a time; text literals are left alone, as DuckDB already
    reads them as the other type (and a branch that is never taken must not fail)."""
    by_select: dict[int, tuple[exp.Select, list[list[exp.Expression]]]] = {}
    for node in transformed.find_all(exp.Coalesce, exp.Case, exp.If, exp.Greatest, exp.Least):
        select = node.find_ancestor(exp.Select)
        if isinstance(node, exp.Case):
            args = [i.args.get("true") for i in node.args.get("ifs") or []] + [node.args.get("default")]
        elif isinstance(node, exp.If):
            if isinstance(node.parent, exp.Case):
                continue
            args = [node.args.get("true"), node.args.get("false")]
        else:
            args = [node.this, *node.expressions]
        args = [a for a in args if a is not None and not isinstance(a, exp.Null)]
        if any(isinstance(a, exp.Literal) and a.is_string for a in args) and any(isinstance(a, exp.Boolean) for a in args):
            for a in args:  # coalesce('x', false): text, as in Snowflake (DuckDB would read 'x' as a boolean)
                if isinstance(a, exp.Boolean):
                    a.replace(exp.Literal.string("true" if a.this else "false"))
            continue
        if select is None or select.args.get("from_") is None or len(args) < 2 or not any(a.find(exp.Column) for a in args):
            continue
        by_select.setdefault(id(select), (select, []))[1].append(args)
    for select, calls in by_select.values():
        flat = [a for args in calls for a in args]
        probe = exp.Select(expressions=[exp.alias_(a.copy(), f"_k{i}") for i, a in enumerate(flat)])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            kinds = [str(row[1]).upper() for row in conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()]
        except Exception:  # noqa: BLE001 - an argument may read an alias of the same SELECT
            calls, kinds = _kinds_past_aliases(select, calls, conn)
            flat = [a for args in calls for a in args]
        if len(kinds) != len(flat):
            continue
        at = 0
        for args in calls:
            mine, at = kinds[at:at + len(args)], at + len(args)
            if "VARCHAR" not in mine or "JSON" in mine:
                continue
            numbers = [k for k in mine if k.startswith(_NUMERIC_KINDS)]
            times = [k for k in mine if k.startswith("TIMESTAMP")]
            if numbers:
                target = "DOUBLE" if any(k.startswith(("DOUBLE", "FLOAT")) for k in numbers) else "DECIMAL(38, 5)"
                words = [a for a, k in zip(args, mine) if k == "VARCHAR" and isinstance(a, exp.Literal) and a.is_string
                         and not re.fullmatch(r"\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*", a.name)]
                if words and target != "DOUBLE":
                    # ifnull(crm_id, ''): text that is no number beside an exact number makes the result a NUMBER with 5
                    # decimals (at least: more when the number has more), measured: 384 is 384.00000. The text is an
                    # error only where it is taken
                    scales = [int(m.group(1)) for k in numbers if (m := re.match(r"DECIMAL\(\d+,\s*(\d+)\)", k))]
                    exact = f"DECIMAL(38, {max([5, *scales])})"
                    for arg, kind in zip(args, mine):
                        if arg in words:
                            said = "Numeric value '" + arg.name + "' is not recognized"
                            arg.replace(exp.cast(exp.Anonymous(this="error", expressions=[exp.Literal.string(said)]), exact))
                        elif kind.startswith(_NUMERIC_KINDS):
                            arg.replace(exp.cast(arg.copy(), exact))
                    continue
            elif times:
                target = times[0]
            elif "DATE" in mine:
                target = "DATE"
            elif "BOOLEAN" in mine:
                for arg, kind in zip(args, mine):
                    if kind == "BOOLEAN" and not isinstance(arg, exp.Boolean):
                        arg.replace(exp.cast(arg.copy(), "varchar"))
                continue
            else:
                continue
            for arg, kind in zip(args, mine):
                if kind == "VARCHAR" and not (isinstance(arg, exp.Literal) and arg.is_string):
                    arg.replace(exp.cast(arg.copy(), target))
    return transformed


def text_beside_variant(transformed: exp.Expression, conn) -> exp.Expression | None:  # noqa: ANN001
    """`select * from a union all select * from b` where a column is a VARIANT in one branch and text in
    another: Snowflake keeps the text as a VARIANT string. DuckDB parses it as JSON and stops at the first
    value that is not JSON. The text columns are wrapped as JSON strings. None when there is nothing to wrap."""
    tree, changed = transformed.copy(), False
    for union in list(tree.find_all(exp.SetOperation)):
        if isinstance(union.parent, exp.SetOperation) or (isinstance(union.parent, exp.Subquery) and isinstance(union.parent.parent, exp.SetOperation)):
            continue
        prefix = _cte_prefix(union)
        branches = [b for b in _branches(union) if isinstance(b, exp.Select)]
        shapes = []
        try:
            for branch in branches:
                shapes.append(conn.execute("describe " + prefix + branch.sql(dialect="duckdb")).fetchall())
        except Exception:  # noqa: BLE001 - a branch that only binds in its context
            continue
        if len(branches) < 2 or len({len(d) for d in shapes}) != 1:
            continue
        variant = {i for d in shapes for i, row in enumerate(d) if row[1] == "JSON"}
        for branch, described in zip(branches, shapes):
            wrap = [described[i][0] for i in sorted(variant) if described[i][1] == "VARCHAR"]
            if not wrap or len({row[0] for row in described}) != len(described):
                continue
            replaced = ", ".join(f'to_json("{c}") as "{c}"' for c in wrap)
            inner = branch.copy()
            outer = sqlglot.parse_one(f'select * replace ({replaced}) from (select 1) as "__FXV"', dialect="duckdb")
            outer.args["from_"].this.set("this", inner)
            branch.replace(outer)
            changed = True
    # `coalesce(account_id, customer_id)` with a text and a VARIANT argument: the same, per argument
    def kinds_of(select: exp.Select, args: list[exp.Expression]) -> list[str] | None:
        probe = exp.Select(expressions=[exp.alias_(a.copy(), f"_k{i}") for i, a in enumerate(args)])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            kinds = [row[1] for row in conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()]
        except Exception:  # noqa: BLE001 - an argument that only binds in its place
            return None
        return kinds if len(kinds) == len(args) else None

    # `where audit_field in ('sla_policy', 'priority')` on a VARIANT column: Snowflake compares the string it holds.
    # DuckDB would read the literal as JSON. The literals become JSON strings
    compared: dict[int, tuple[exp.Select, list[tuple[exp.Expression, list[exp.Expression]]]]] = {}
    for node in tree.find_all(exp.EQ, exp.NEQ, exp.In):
        select = node.find_ancestor(exp.Select)
        if select is None or select.args.get("from_") is None:
            continue
        if isinstance(node, exp.In):
            value, literals = node.this, [e for e in node.expressions if isinstance(e, exp.Literal) and e.is_string]
            if len(literals) != len(node.expressions) or not literals:
                continue
        else:
            sides = [node.this, node.expression]
            literals = [e for e in sides if isinstance(e, exp.Literal) and e.is_string]
            if len(literals) != 1:
                continue
            value = sides[1] if sides[0] is literals[0] else sides[0]
        if isinstance(value, exp.Column):  # (a plain column: the one case a literal meets a stored VARIANT as it is)
            compared.setdefault(id(select), (select, []))[1].append((value, literals))
    for select, tests in compared.values():
        kinds = kinds_of(select, [value for value, _ in tests])
        for (value, literals), kind in zip(tests, kinds or []):
            if kind == "JSON":
                for literal in literals:
                    literal.replace(exp.func("to_json", literal.copy()))
                changed = True
    by_select: dict[int, tuple[exp.Select, list[list[exp.Expression]]]] = {}
    for node in tree.find_all(exp.Coalesce, exp.Case, exp.If):
        select = node.find_ancestor(exp.Select)
        if isinstance(node, exp.Case):  # `case when v = 'x' then v else 'Other' end`: what the branches give
            args = [i.args.get("true") for i in node.args.get("ifs") or []] + [node.args.get("default")]
        elif isinstance(node, exp.If):
            args = [] if isinstance(node.parent, exp.Case) else [node.args.get("true"), node.args.get("false")]
        else:
            args = [node.this, *node.expressions]
        args = [a for a in args if a is not None and not isinstance(a, exp.Null)]
        if select is not None and select.args.get("from_") is not None and len(args) >= 2:
            by_select.setdefault(id(select), (select, []))[1].append(args)
    for select, calls in by_select.values():
        # every COALESCE of one SELECT in one question (a model can hold hundreds); one by one where that does not bind
        flat = [a for args in calls for a in args]
        kinds = kinds_of(select, flat)
        typed, at = [], 0
        for args in calls if kinds is not None else calls[:200]:
            typed.append((args, kinds[at:at + len(args)] if kinds is not None else kinds_of(select, args)))
            at += len(args)
        for args, their in typed:
            if not their or "JSON" not in their:
                continue
            for arg, kind in zip(args, their):
                if kind == "VARCHAR":
                    arg.replace(exp.func("to_json", arg.copy()))
                    changed = True
    return tree if changed else None


def dates_in_time_zones(transformed: exp.Expression, conn) -> exp.Expression | None:  # noqa: ANN001
    """`convert_timezone('UTC', a_date)`: Snowflake takes the date as midnight. DuckDB's `date AT TIME ZONE` gives a
    type it cannot cast onwards. The dates among the arguments (asked of DuckDB, one SELECT at a time) are made
    timestamps first. None when there is none."""
    tree, changed = transformed.copy(), False
    for node in list(tree.find_all(exp.AtTimeZone, exp.ConvertTimezone)):
        slot = "timestamp" if isinstance(node, exp.ConvertTimezone) else "this"
        value = node.args.get(slot)
        select = node.find_ancestor(exp.Select)
        if value is None or select is None or select.args.get("from_") is None \
                or (isinstance(value, (exp.Cast, exp.TryCast)) and value.to.is_type("timestamp")):
            continue
        probe = exp.Select(expressions=[exp.alias_(value.copy(), "_k")])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        try:
            kind = conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()[0][1]
        except Exception:  # noqa: BLE001, S112 - an argument that only binds in its place
            continue
        if kind == "DATE":
            node.set(slot, exp.cast(value.copy(), "timestamp"))
            changed = True
    return tree if changed else None


def text_beside_variant_in_views(transformed: exp.Expression, conn, define=None) -> exp.Expression | None:  # noqa: ANN001
    """The same, where the statement itself has nothing to wrap but reads a view that has (dbt's incremental MERGE
    reads the model through a temporary view): the view is defined again with its text wrapped. Returns the
    statement, to be run again, or None when no view changed. `define` runs the new definition (the emulator
    does it outside the session's transaction, so that a later rollback does not undo it)."""
    define = define or conn.execute
    names = {t.name.upper() for t in transformed.find_all(exp.Table)}
    changed = False
    try:
        views = conn.execute("select view_name, sql from duckdb_views() where not internal").fetchall()
    except Exception:  # noqa: BLE001
        return None
    for name, sql in views:
        if name.upper() not in names or not sql:
            continue
        try:
            create = sqlglot.parse_one(sql, dialect="duckdb")
            if not isinstance(create, exp.Create):
                continue
            create.set("replace", True)
            for _ in range(4):  # wrapping one layer changes the types of the next: until nothing is left to wrap
                body = text_beside_variant(create.expression, conn)
                if body is None:
                    break
                create.set("expression", body)
                define(create.sql(dialect="duckdb"))
                changed = True
        except Exception:  # noqa: BLE001, S112 - a view that cannot be read back or defined again stays as it is
            continue
    return transformed.copy() if changed else None


# --- ARRAY and OBJECT columns. DuckDB holds every semi-structured value as JSON; Snowflake types an expression
# as ARRAY, OBJECT or VARIANT when the statement is written, and DESCRIBE / INFORMATION_SCHEMA say which.
_ARRAY_MAKERS = {
    "ARRAY_CONSTRUCT", "ARRAY_CONSTRUCT_COMPACT", "ARRAY_AGG", "ARRAY_APPEND", "ARRAY_CAT", "ARRAY_COMPACT", "ARRAY_DISTINCT",
    "ARRAY_EXCEPT", "ARRAY_INSERT", "ARRAY_INTERSECTION", "ARRAY_PREPEND", "ARRAY_REMOVE", "ARRAY_REMOVE_AT", "ARRAY_SLICE",
    "ARRAY_SORT", "ARRAY_UNIQUE_AGG", "ARRAY_UNION_AGG", "ARRAY_FLATTEN", "ARRAY_GENERATE_RANGE", "ARRAY_REVERSE", "ARRAYS_ZIP",
    "TO_ARRAY", "STRTOK_TO_ARRAY", "SPLIT", "OBJECT_KEYS", "REGEXP_SUBSTR_ALL", "ARRAY", "ARRAY_CONCAT", "ARRAY_UNIQUE_AGG",
    "STRTOK_SPLIT_TO_TABLE", "ARRAYAGG", "STR_TO_ARRAY", "STRING_TO_ARRAY", "ARRAY_TO_ARRAY",
}
_OBJECT_MAKERS = {"OBJECT_CONSTRUCT", "OBJECT_CONSTRUCT_KEEP_NULL", "OBJECT_AGG", "OBJECT_INSERT", "OBJECT_DELETE", "OBJECT_PICK",
                  "TO_OBJECT", "STRUCT", "OBJECT", "PARSE_URL", "PARSE_IP", "MAP"}
_KEEPS_KIND = (exp.Coalesce, exp.If, exp.Case, exp.Max, exp.Min, exp.AnyValue, exp.FirstValue, exp.LastValue, exp.Lag, exp.Lead,
               exp.Nullif, exp.Window, exp.IgnoreNulls, exp.RespectNulls, exp.Paren, exp.Alias, exp.WithinGroup, exp.Subquery)


def _made_kind(node: exp.Expression) -> str | None:
    """ARRAY or OBJECT when the expression itself makes one (a constructor or a cast)."""
    if isinstance(node, (exp.Cast, exp.TryCast)):
        return "ARRAY" if node.to.is_type("array", "list") else "OBJECT" if node.to.is_type("object", "map", "struct") else ""
    if isinstance(node, exp.Anonymous):
        name = node.name.upper()
    elif isinstance(node, exp.Func):
        name = node.sql_name().upper()
    else:
        return None
    return "ARRAY" if name in _ARRAY_MAKERS else "OBJECT" if name in _OBJECT_MAKERS else None


def semi_kinds(create: exp.Create, known) -> dict[str, str]:  # noqa: ANN001
    """{COLUMN: 'ARRAY' | 'OBJECT'} for the relation a CREATE makes. `known(table)` gives the same for a
    relation that exists. Columns not listed are VARIANT (or not semi-structured at all)."""
    from sqlglot.optimizer.scope import Scope, build_scope

    target = create.this
    if isinstance(target, exp.Schema) and create.expression is None:
        out = {}
        for column in target.expressions:
            kind = column.args.get("kind") if isinstance(column, exp.ColumnDef) else None
            if kind is not None and kind.is_type("array", "list"):
                out[column.name.upper()] = "ARRAY"
            elif kind is not None and kind.is_type("object", "map", "struct"):
                out[column.name.upper()] = "OBJECT"
        return out
    query = create.expression
    while isinstance(query, (exp.Subquery, exp.Paren)):
        query = query.this
    if not isinstance(query, exp.Query):
        return {}
    root = build_scope(query)
    if root is None:
        return {}

    def of_source(source, name: str, depth: int) -> str | None:  # noqa: ANN001
        if isinstance(source, exp.Table):
            return known(source).get(name)
        return in_scope(source, name, depth + 1) if isinstance(source, Scope) else None

    def of_expression(node: exp.Expression, scope, depth: int) -> str | None:  # noqa: ANN001
        made = _made_kind(node)
        if made is not None:
            return made or None
        if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
            for alias, (_, source) in scope.selected_sources.items():
                if node.table and alias.upper() != node.table.upper():
                    continue
                kind = of_source(source, node.name.upper(), depth)
                if kind:
                    return kind
            return None
        if isinstance(node, exp.Subquery) and isinstance(node.this, exp.Select) and len(node.this.expressions) == 1:
            inner = next((s for s in scope.subquery_scopes if s.expression is node.this), None)
            return of_expression(node.this.expressions[0].unalias(), inner, depth + 1) if inner is not None else None
        if isinstance(node, _KEEPS_KIND) and depth < 30:
            if isinstance(node, exp.Case):
                values = [i.args.get("true") for i in node.args.get("ifs") or []] + [node.args.get("default")]
            elif isinstance(node, exp.If):
                values = [node.args.get("true"), node.args.get("false")]
            elif isinstance(node, exp.Coalesce):
                values = [node.this, *node.expressions]
            else:
                values = [node.this]
            kinds = {of_expression(v, scope, depth + 1) for v in values if v is not None and not isinstance(v, exp.Null)}
            return kinds.pop() if len(kinds) == 1 else None
        return None

    def in_scope(scope, name: str, depth: int = 0) -> str | None:  # noqa: ANN001
        if scope is None or depth > 30:
            return None
        select = scope.expression
        if isinstance(select, exp.SetOperation):
            return in_scope(scope.union_scopes[0], name, depth + 1) if scope.union_scopes else None
        if not isinstance(select, exp.Select):
            return None
        stars = []
        for projection in select.expressions:
            if isinstance(projection, exp.Star) or (isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)):
                stars.append(projection)
            elif projection.alias_or_name.upper() == name:
                return of_expression(projection.unalias(), scope, depth)
        for star in stars:
            only = star.table.upper() if isinstance(star, exp.Column) and star.table else None
            for alias, (_, source) in scope.selected_sources.items():
                if only and alias.upper() != only:
                    continue
                kind = of_source(source, name, depth)
                if kind:
                    return kind
        return None

    class Lazily(dict):
        """Asked for one column at a time, once the relation exists and its JSON columns are known."""

        def __missing__(self, name: str):  # noqa: ANN204
            self[name] = found = in_scope(root, name)
            return found

    return Lazily()


def _repair_by_types(tree: exp.Expression, conn, details: dict) -> bool:  # noqa: ANN001
    """`(a + b) * text_column * .01` inside an aggregate: DuckDB names the operator and not the place.
    Ask for the type of each operand of that operator and convert the text ones, as Snowflake does."""
    sign = re.search(r"argument types '([-+*/%])\(", details.get("exception_message", ""))
    if not sign or sign.group(1) not in _OPERATORS_BY_SIGN:
        return False
    kinds, done = _OPERATORS_BY_SIGN[sign.group(1)], False
    for select in list(tree.find_all(exp.Select)):
        # innermost first: `(a * text) * 2` cannot be asked about until `a * text` is repaired
        nodes = [n for n in select.find_all(kinds) if n.find_ancestor(exp.Select) is select and not isinstance(n, exp.Neg)
                 and not n.find(exp.AggFunc, exp.Window, exp.Subquery)
                 and n.this.find(kinds) is None and n.expression.find(kinds) is None]
        if not nodes or select.args.get("from_") is None:
            continue
        probe = exp.Select(expressions=[exp.alias_(side.copy(), f"_k{2 * i + j}") for i, n in enumerate(nodes)
                                        for j, side in enumerate((n.this, n.expression))])
        probe.set("from_", select.args["from_"].copy())
        if select.args.get("joins"):
            probe.set("joins", [j.copy() for j in select.args["joins"]])
        conn.execute("set errors_as_json = false")
        try:
            described = conn.execute("describe " + _cte_prefix(select) + probe.sql(dialect="duckdb")).fetchall()
        except Exception:  # noqa: BLE001 - some operand is itself the problem: nothing to learn here
            continue
        finally:
            conn.execute("set errors_as_json = true")
        if len(described) != 2 * len(nodes):
            continue
        for i, n in enumerate(nodes):
            left, right = described[2 * i][1], described[2 * i + 1][1]
            for side, kind, other in ((n.this, left, right), (n.expression, right, left)):
                if kind == "VARCHAR" and other != "VARCHAR":
                    _convert(side, "timestamp" if other.startswith(_DATE_TYPES) else "double")
                    done = True
    return done


_SNOWFLAKE = sqlglot.Dialect.get_or_raise("snowflake")
_ENDS_A_LISTING = {"FROM", "WHERE", "GROUP_BY", "ORDER_BY", "HAVING", "QUALIFY", "LIMIT", "UNION", "EXCEPT", "INTERSECT", "INTO",
                   "FETCH", "SEMICOLON", "WINDOW"}
_OPENS, _CLOSES = {"L_PAREN", "L_BRACKET", "L_BRACE"}, {"R_PAREN", "R_BRACKET", "R_BRACE"}


def name_expressions(tree: exp.Expression, command: str) -> exp.Expression:
    """`select count(*), a+1 from t`: Snowflake names those columns COUNT(*) and A+1, the text as written
    in upper case. DuckDB would call them count_star() and (a + 1). Give the outermost query's unnamed
    expressions the names Snowflake gives them."""
    query = tree.expression if isinstance(tree, exp.Create) else tree
    while isinstance(query, (exp.Subquery, exp.Paren, exp.SetOperation)):
        query = query.this
    if not isinstance(query, exp.Select):
        return tree

    def unnamed(e: exp.Expression) -> bool:  # (an expression, not a column, a `*` or `t.*`)
        return not isinstance(e, (exp.Alias, exp.Column, exp.Star, exp.Dot, exp.Aliases, exp.PropertyEQ))

    inner = any(unnamed(e) or (isinstance(e, exp.Paren) and isinstance(e.unnest(), exp.Column))
                for s in tree.find_all(exp.Select) if s is not query for e in s.expressions)
    if not inner and not any(unnamed(e) for e in query.expressions):
        return tree
    try:
        tokens = _SNOWFLAKE.tokenize(command)
    except Exception:  # noqa: BLE001 - the names stay DuckDB's
        return tree
    if inner:
        _name_inner_expressions(tree, query, command, tokens, unnamed)
    if not any(unnamed(e) for e in query.expressions):
        return tree
    depth, base, start, pieces, first = 0, None, None, [], None
    for i, token in enumerate(tokens):
        kind = token.token_type.name
        if kind in _OPENS:
            depth += 1
        elif kind in _CLOSES:
            depth -= 1
        if start is None:
            if base is None and kind in ("WITH", "SELECT"):
                base = depth
            if kind == "SELECT" and depth == base:
                start = i + 1
                while start < len(tokens) and tokens[start].token_type.name in ("DISTINCT", "ALL"):
                    start += 1
                first = start
            continue
        if i < first:
            continue
        if depth < base or (depth == base and kind in _ENDS_A_LISTING):
            pieces.append((first, i - 1))
            break
        if depth == base and kind == "COMMA":
            pieces.append((first, i - 1))
            first = i + 1
    else:
        if start is not None:
            pieces.append((first, len(tokens) - 1))
    if len(pieces) != len(query.expressions) or any(a > b for a, b in pieces):
        return tree  # not read the way the parser read it: leave the names alone
    for (a, b), e in zip(pieces, list(query.expressions)):
        if isinstance(e, exp.Paren) and isinstance(e.unnest(), exp.Column):
            e.replace(e.unnest().copy())  # (id) is named ID
        elif unnamed(e):
            name = command[tokens[a].start:tokens[b].end + 1].upper()
            if len(name) <= 250:
                e.replace(exp.alias_(e.copy(), exp.to_identifier(name, quoted=True)))
    return tree


def _name_inner_expressions(tree: exp.Expression, outer: exp.Select, command: str, tokens: list, unnamed) -> None:  # noqa: ANN001
    """The same names in a CTE or a subquery, whose columns a `select *` passes on (`delta::number` in a CTE is the
    column DELTA::NUMBER of the table). The select is found in the text by a name it reads (its own, not a
    subquery's): back from there to its SELECT, and its listing split at its commas."""
    depth, depths = 0, []
    for token in tokens:
        kind = token.token_type.name
        if kind in _CLOSES:
            depth -= 1
        depths.append(depth)
        if kind in _OPENS:
            depth += 1
    at = {token.start: i for i, token in enumerate(tokens)}
    for select in list(tree.find_all(exp.Select)):
        if select is outer or not any(unnamed(e) or (isinstance(e, exp.Paren) and isinstance(e.unnest(), exp.Column))
                                      for e in select.expressions):
            continue
        anchors = [i.meta["start"] for i in select.find_all(exp.Identifier)
                   if i.meta and i.meta.get("start") in at and i.find_ancestor(exp.Select) is select]
        if not anchors:
            continue
        k = at[min(anchors)]
        low, j = depths[k], k
        while j > 0:  # back to the SELECT of this select
            j -= 1
            low = min(low, depths[j])
            if tokens[j].token_type.name == "SELECT" and depths[j] <= low:
                break
        else:
            continue
        base, first = depths[j], j + 1
        while first < len(tokens) and tokens[first].token_type.name in ("DISTINCT", "ALL"):
            first += 1
        pieces = []
        for i in range(first, len(tokens)):
            kind = tokens[i].token_type.name
            if depths[i] < base or (depths[i] == base and kind in _ENDS_A_LISTING):
                pieces.append((first, i - 1))
                break
            if depths[i] == base and kind == "COMMA":
                pieces.append((first, i - 1))
                first = i + 1
        else:
            pieces.append((first, len(tokens) - 1))
        if len(pieces) != len(select.expressions) or any(x > y for x, y in pieces):
            continue  # (not read the way the parser read it)
        for (x, y), e in zip(pieces, list(select.expressions)):
            if isinstance(e, exp.Paren) and isinstance(e.unnest(), exp.Column):
                e.replace(e.unnest().copy())
            elif unnamed(e):
                name = command[tokens[x].start:tokens[y].end + 1].upper()
                if len(name) <= 250:
                    e.replace(exp.alias_(e.copy(), exp.to_identifier(name, quoted=True)))


def aliases_over_tables(transformed: exp.Expression) -> exp.Expression | None:
    """`select ... as list_price, qty * list_price from joined left join list_price on ...`: Snowflake
    reads the second `list_price` as the column just defined; DuckDB reads it as the joined table's
    whole row. Write the defined expression in its place. None when there is nothing of the kind."""
    tree, changed = transformed.copy(), False
    for select in tree.find_all(exp.Select):
        source = select.args.get("from_")
        if source is None:
            continue
        relations = {r.alias_or_name.upper() for r in [source.this, *[j.this for j in select.args.get("joins") or []]]}
        defined: dict[str, exp.Expression] = {}
        for e in select.expressions:
            for c in list(e.find_all(exp.Column)):
                if not c.table and c.name.upper() in defined and c.find_ancestor(exp.Select) is select:
                    c.replace(exp.paren(defined[c.name.upper()].copy()))
                    changed = True
            if isinstance(e, exp.Alias) and e.alias.upper() in relations:
                defined[e.alias.upper()] = e.this
    return tree if changed else None


def repair_types(transformed: exp.Expression, conn, params=None) -> exp.Expression | None:  # noqa: ANN001
    """The statement with Snowflake's implicit text conversions made explicit, or None."""
    tree, fixed = transformed.copy(), 0
    conn.execute("set errors_as_json = true")
    try:
        for _ in range(80):
            text = tree.sql(dialect="duckdb")
            try:
                conn.execute("explain " + text, params) if params else conn.execute("explain " + text)
                return tree if fixed else None
            except Exception as e:  # noqa: BLE001
                details = _error_details(str(e))
                if details and details.get("position"):
                    details["position"] = int(details["position"]) - len("explain ")
                elif details and _repair_by_types(tree, conn, details):
                    fixed += 1  # DuckDB did not say where: the operands were asked for their types instead
                    continue
                if not details or not _repair(tree, text, details):
                    return tree if fixed else None  # keep what was fixed: the remaining error is the one to report
                fixed += 1
        return tree
    finally:
        conn.execute("set errors_as_json = false")


def view_past_a_schema_named_like_a_database(transformed: exp.Expression, message: str) -> exp.Expression | None:
    """A view in PREP that reads PREP.SNOWFLAKE.T while a database SNOWFLAKE exists: DuckDB stores a view's
    references to its own database without the database, and then cannot tell the schema SNOWFLAKE from
    the database. Those tables are read by their full name instead. None when it is not that case."""
    m = re.search(r'Ambiguous reference to catalog or schema "([^"]+)"', message)
    if not m or not isinstance(transformed, exp.Create) or str(transformed.args.get("kind", "")).upper() != "VIEW":
        return None
    tree, changed = transformed.copy(), False
    for table in list(tree.expression.find_all(exp.Table)) if tree.expression is not None else []:
        if not (table.catalog and table.db.upper() == m.group(1).upper() and isinstance(table.this, exp.Identifier)):
            continue
        full = ".".join(f'"{part}"' for part in (table.catalog, table.db, table.name))
        read = exp.Table(this=exp.Anonymous(this="query_table", expressions=[exp.Literal.string(full)]),
                         alias=exp.TableAlias(this=exp.to_identifier(table.alias_or_name, quoted=True)))
        for extra in ("joins", "pivots", "sample"):
            if table.args.get(extra):
                read.set(extra, table.args[extra])
        table.replace(read)
        changed = True
    return tree if changed else None


def group_by_all_written_out(transformed: exp.Expression) -> exp.Expression | None:
    """`select dept, count(*) as n, round(mrs / nullif(n, 0)) as rate ... group by all`: Snowflake groups by
    the items that hold no aggregate, and `rate` holds one through the alias `n`. DuckDB does not look
    through the alias. Write the aggregate in its place and name the grouping items by position."""
    tree, changed = transformed.copy(), False
    for select in tree.find_all(exp.Select):
        group = select.args.get("group")
        if group is None or not group.args.get("all") or any(isinstance(e, exp.Star) or (
                isinstance(e, exp.Column) and isinstance(e.this, exp.Star)) for e in select.expressions):
            continue
        aggregated = {e.alias.upper(): e.this for e in select.expressions if isinstance(e, exp.Alias) and any(
            a.find_ancestor(exp.Window) is None or a.find_ancestor(exp.Window).find_ancestor(exp.Select) is not select
            for a in e.this.find_all(exp.AggFunc))}
        for e in select.expressions:
            for col in list(e.find_all(exp.Column)):
                own = e.alias.upper() if isinstance(e, exp.Alias) else None
                if not col.table and col.name.upper() in aggregated and col.name.upper() != own and col.find_ancestor(exp.Select) is select:
                    col.replace(exp.paren(aggregated[col.name.upper()].copy()))
        keys = [exp.Literal.number(i + 1) for i, e in enumerate(select.expressions)
                if not any(a.find_ancestor(exp.Select) is select for a in e.find_all(exp.AggFunc, exp.Window))]
        group.set("all", None)
        if keys:
            group.set("expressions", keys)
        else:
            select.set("group", None)
        changed = True
    return tree if changed else None
