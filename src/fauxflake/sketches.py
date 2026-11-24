"""Snowflake's sketches, as the states it writes: the t-digest of APPROX_PERCENTILE, the HyperLogLog of
APPROX_COUNT_DISTINCT and HLL, the space-saving counters of APPROX_TOP_K. Each takes the values of a group as a
list (DuckDB has no Python aggregates). The layouts and formulas were read off Snowflake's answers; the one thing
not reproduced is the 64-bit hash an HLL state is built from, which Snowflake does not publish (HASH() differs too),
so an HLL_ACCUMULATE state holds other registers than Snowflake's while estimating the same way."""
from __future__ import annotations

import decimal
import json
import math
import struct

# -- t-digest (APPROX_PERCENTILE_ACCUMULATE / _COMBINE / _ESTIMATE, APPROX_PERCENTILE) --------------------------
# measured: up to 1400 values the state is every value with weight 1, sorted; past that, neighbours near the median
# merge first, by the size bound 4 n q (1 - q) / 700 of the original t-digest
RAW = 1400
COMPRESSION = 700


def _float(v: float) -> str:
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    return "%.16e" % v  # (a FLOAT held in a VARIANT; see jsondoc.float_text)


def _compressed(centroids: list[tuple[float, float]]) -> list[tuple[float, float]]:
    total = sum(w for _, w in centroids)
    if len(centroids) <= RAW:
        return centroids
    out: list[list[float]] = []
    seen = 0.0
    for mean, weight in centroids:
        if out:
            last = out[-1]
            q = (seen - last[1] + (last[1] + weight) / 2) / total
            if last[1] + weight <= 4 * total * q * (1 - q) / COMPRESSION:
                merged = last[1] + weight
                last[0] += (mean - last[0]) * weight / merged
                last[1] = merged
                seen += weight
                continue
        out.append([mean, weight])
        seen += weight
    return [(m, w) for m, w in out]


def _digest(centroids: list[tuple[float, float]]) -> str:
    state = ",".join(f"{_float(m)},{_float(w)}" for m, w in centroids)
    return '{"state":[' + state + '],"type":"tdigest","version":1}'


def _centroids(state: str | None) -> list[tuple[float, float]]:
    if state is None:
        return []
    doc = json.loads(state) if isinstance(state, str) else state
    if not isinstance(doc, dict) or doc.get("type") != "tdigest":
        raise ValueError("Invalid parameter value: approx_percentile state")
    s = [float(x) for x in doc.get("state") or []]
    return list(zip(s[::2], s[1::2]))


def tdigest_accumulate(values: list | None) -> str:
    vals = sorted(float(v) for v in (values or []) if v is not None)
    return _digest(_compressed([(v, 1.0) for v in vals]))


def tdigest_combine(states: list | None) -> str | None:
    states = [s for s in (states or []) if s is not None]
    if not states:
        return None
    merged = sorted((c for s in states for c in _centroids(s)), key=lambda c: c[0])
    return _digest(_compressed(merged))


def _quantile(centroids: list[tuple[float, float]], p: float) -> float | None:
    if not centroids:
        return None
    if all(w == 1.0 for _, w in centroids):
        v = [m for m, _ in centroids]  # measured: the linear interpolation of PERCENTILE_CONT, to the last bit
    else:
        v = [m for m, w in centroids for _ in range(max(1, round(w)))]
    pos = p * (len(v) - 1)
    lo = math.floor(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def tdigest_estimate(state: str | None, p: float | None) -> float | None:
    if state is None or p is None:
        return None
    return _quantile(_centroids(state), float(p))


def tdigest_quantile(values: list | None, p: float | None) -> float | None:
    if p is None:
        return None
    vals = sorted(float(v) for v in (values or []) if v is not None)
    return _quantile(_compressed([(v, 1.0) for v in vals]), float(p))


# -- HyperLogLog (HLL_ACCUMULATE / _COMBINE / _ESTIMATE / _EXPORT / _IMPORT, HLL, APPROX_COUNT_DISTINCT) --------
# measured: precision 12; a sparse state is 3 bytes a register (index little-endian, then its count), sorted, and a
# dense one is the 4096 registers; the estimate is HLL++: linear counting up to 3100, then the raw estimate less its
# bias up to 5m. Snowflake's own bias table is not public: this is the one of Heule, Nunkesser and Hall (2013), which
# lands within a few in ten thousand of it
P = 12
M = 1 << P
DENSE_AT = M // 3 + 1  # (a sparse state as large as a dense one)
_ALPHA = (1 / (2 * math.log(2))) / (1 + 1.079 / M)


def _registers_of(state: bytes | None) -> dict[int, int]:
    if state is None:
        return {}
    b = bytes(state)
    if len(b) > M:
        raise ValueError("Invalid parameter value: HLL state. Reason: corrupted (binary too large)")
    if len(b) == M:
        return {i: r for i, r in enumerate(b) if r}
    out: dict[int, int] = {}
    for i in range(0, len(b) - len(b) % 3, 3):
        idx, lz = struct.unpack_from("<H", b, i)[0], b[i + 2]
        out[idx] = max(out.get(idx, 0), lz)
    return out


def _state(regs: dict[int, int]) -> bytes:
    if len(regs) >= DENSE_AT:
        return bytes(regs.get(i, 0) for i in range(M))
    return b"".join(struct.pack("<HB", i, regs[i]) for i in sorted(regs))


def _register(h: int) -> tuple[int, int]:
    h &= (1 << 64) - 1
    rest = h & ((1 << (64 - P)) - 1)
    return h >> (64 - P), min(64 - P - rest.bit_length() + 1, 64 - P + 1)


def hll_accumulate(hashes: list | None) -> bytes:
    regs: dict[int, int] = {}
    for h in hashes or []:
        if h is None:
            continue
        i, lz = _register(int(h))
        if lz > regs.get(i, 0):
            regs[i] = lz
    return _state(regs)


def hll_combine(states: list | None) -> bytes | None:
    states = [s for s in (states or []) if s is not None]
    if not states:
        return None
    regs: dict[int, int] = {}
    for s in states:
        for i, r in _registers_of(s).items():
            regs[i] = max(regs.get(i, 0), r)
    return _state(regs)


def _estimate(regs: dict[int, int]) -> int:
    empty = M - len(regs)
    if empty:
        lc = M * math.log(M / empty)
        if lc <= 3100:
            return round(lc)
    e = _ALPHA * M * M / (empty + sum(2.0 ** -r for r in regs.values()))
    if e <= 5 * M:
        e -= _bias(e)
    return round(e)


def hll_estimate(state: bytes | None) -> int | None:
    return None if state is None else _estimate(_registers_of(state))


def hll_count(hashes: list | None) -> int:
    regs: dict[int, int] = {}
    for h in hashes or []:
        if h is not None:
            i, lz = _register(int(h))
            if lz > regs.get(i, 0):
                regs[i] = lz
    return _estimate(regs)


def hll_export(state: bytes | None) -> str | None:
    if state is None:
        return None
    b = bytes(state)
    regs = _registers_of(b)
    if len(b) == M:
        return '{"dense":[' + ",".join(str(r) for r in b) + '],"precision":12,"version":4}'
    idx = sorted(regs)
    return ('{"precision":12,"sparse":{"indices":[' + ",".join(map(str, idx)) + '],"maxLzCounts":['
            + ",".join(str(regs[i]) for i in idx) + ']},"version":4}')


def hll_import(doc: str | None) -> bytes | None:
    if doc is None:
        return None
    d = json.loads(doc)
    if not isinstance(d, dict) or d.get("version") != 4 or d.get("precision") != 12:
        raise ValueError("Invalid parameter value: HLL state. Reason: unsupported version or precision")
    if "dense" in d:
        return bytes(int(r) for r in d["dense"])
    sp = d.get("sparse") or {}
    regs = dict(zip((int(i) for i in sp.get("indices") or []), (int(r) for r in sp.get("maxLzCounts") or [])))
    return b"".join(struct.pack("<HB", i, regs[i]) for i in sorted(regs))


# -- space-saving top-k (APPROX_TOP_K, APPROX_TOP_K_ACCUMULATE / _COMBINE / _ESTIMATE, TOP_K_SKETCH_OBJECT_CONSTRUCT)
# measured: counts in descending order, ties with the counter made last first; a full set of counters gives a new
# value the smallest count plus one
def _count(pairs, counters: int) -> list[list]:  # noqa: ANN001 - (value text, weight) in arrival order
    table: dict[str, list] = {}  # value -> [count, made]
    made = 0
    for value, weight in pairs:
        if value in table:
            table[value][0] += weight
            continue
        made += 1
        if len(table) >= counters:
            smallest = min(table, key=lambda k: (table[k][0], table[k][1]))
            weight += table.pop(smallest)[0]
        table[value] = [weight, made]
    return [[k, c] for k, (c, _) in sorted(table.items(), key=lambda kv: (-kv[1][0], -kv[1][1]))]


def _number(n: float) -> str:
    return str(int(n)) if float(n).is_integer() else repr(float(n))


_LITERAL_WHOLE = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")


def _datatype(kind: str, values: list | None = None) -> tuple[str, int, int]:
    kind = kind.upper()
    if kind.startswith("DECIMAL") or kind in _LITERAL_WHOLE:
        p, s = (int(x) for x in kind[kind.index("(") + 1:-1].split(",")) if kind.startswith("DECIMAL") else (38, 0)
        if kind in _LITERAL_WHOLE or p - s in (10, 19):
            # a whole number written out (DuckDB's INTEGER, or a DECIMAL widened to hold one): Snowflake types it by its
            # digits, NUMBER(1, 0) for 7, and a column of them by the widest
            digits = [len(str(abs(int(decimal.Decimal(v))))) for v in values or [] if v is not None]
            p = max(digits, default=1) + s
        return "FIXED", p, s
    if kind in ("DOUBLE", "FLOAT", "REAL"):
        return "REAL", 38, 0
    if kind == "DATE":
        return "DATE", 38, 0
    if kind.startswith("TIMESTAMP"):
        return "TIMESTAMP_TZ" if "TIME ZONE" in kind else "TIMESTAMP_NTZ", 38, 0
    if kind == "BOOLEAN":
        return "BOOLEAN", 38, 0
    return "TEXT", 38, 0


UNDEFINED = '"\ufdd0undefined"'  # (semantics.UNDEFINED, as JSON text)


def _element(text: str | None, datatype: str) -> str:
    if text is None:
        return UNDEFINED
    if datatype == "FIXED":
        d = decimal.Decimal(text)
        return str(d.quantize(1)) if d == d.to_integral_value() else format(d.normalize(), "f")
    if datatype == "REAL":
        return _float(float(text))
    if datatype == "BOOLEAN":
        return text.lower()
    return json.dumps(text, ensure_ascii=False)


def _sketch(state: list[list], counters: int, datatype: str, precision: int, scale: int) -> str:
    body = ",".join(f"[{_element(v, datatype)},{_number(c)}]" for v, c in state)
    return (f'{{"counters":{counters},"datatype":"{datatype}","precision":{precision},"scale":{scale},'
            f'"state":[{body}],"type":"approx_top_k"}}')


def topk_accumulate(values: list | None, counters: int | None, kind: str | None) -> str:
    counters = int(counters or 1)
    datatype, precision, scale = _datatype(kind or "VARCHAR", values)
    return _sketch(_count(((v, 1) for v in values or [] if v is not None), counters), counters, datatype, precision, scale)


def _topk_doc(state: str) -> dict:
    d = json.loads(state)
    if not isinstance(d, dict) or d.get("type") != "approx_top_k":
        raise ValueError("Invalid parameter value: approx_top_k state")
    return d


def _value_text(v, datatype: str) -> str | None:  # noqa: ANN001
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return _number(v) if datatype != "REAL" else repr(float(v))
    return str(v)


def topk_combine(states: list | None, counters: int | None) -> str | None:
    docs = [_topk_doc(s) for s in (states or []) if s is not None]
    if not docs:
        return None
    first = docs[0]
    c = int(counters) if counters is not None else int(first.get("counters") or 1)
    pairs = [(_value_text(v, first["datatype"]), n) for d in docs for v, n in d.get("state") or []]
    return _sketch(_count(pairs, c), c, first["datatype"], int(first.get("precision", 38)), int(first.get("scale", 0)))


def topk_estimate(state: str | None, k: int | None) -> str | None:
    if state is None:
        return None
    d = _topk_doc(state)
    k = 1 if k is None else int(k)
    return "[" + ",".join(f"[{_element(_value_text(v, d['datatype']), d['datatype'])},{_number(n)}]"
                          for v, n in (d.get("state") or [])[:k]) + "]"


def topk(values: list | None, k: int | None, counters: int | None, kind: str | None) -> str:
    k = 1 if k is None else int(k)
    datatype, _, _ = _datatype(kind or "VARCHAR")
    state = _count(((v, 1) for v in values or [] if v is not None), int(counters) if counters else max(k * 10, 1000))
    return "[" + ",".join(f"[{_element(v, datatype)},{_number(n)}]" for v, n in state[:k]) + "]"


def topk_construct(k1: str | None, c1: float | None, k2: str | None, c2: float | None) -> str:
    pairs = sorted(((k1, c1), (k2, c2)), key=lambda p: -(p[1] if p[1] is not None else 0))
    return _sketch([[k, c] for k, c in pairs], 50, "TEXT", 38, 0)


# -- the rest --------------------------------------------------------------------------------------------------
def fuzzy_subsequence_score(text: str | None, pattern: str | None) -> int | None:
    """Measured: the pattern's characters are found in order, each at the first place after the last; one found right
    after the last scores 3, another 1; the first one not found ends the count. Case does not matter."""
    if text is None or pattern is None:
        return None
    t, score, at = text.lower(), 0, -2
    for i, ch in enumerate(pattern.lower()):
        found = t.find(ch, at + 1 if i else 0)
        if found < 0:
            break
        score += 3 if i and found == at + 1 else 1
        at = found
    return score


_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def snowpark_conv(value: str | None, source: int | None, target: int | None) -> str | None:
    """Spark's CONV, as Snowflake has it for Snowpark: text in one base to text in another, through an unsigned
    64-bit number (a negative target base reads it signed); anything it cannot read is '0'."""
    if value is None or source is None or target is None:
        return None
    source, target = int(source), int(target)
    if not 2 <= source <= 36 or not 2 <= abs(target) <= 36:
        return "0"
    s = value.strip()
    negative = s.startswith("-")
    digits = s[1:] if negative else s
    if not digits or any(c.upper() not in _DIGITS[:source] for c in digits):
        return "0"
    n = min(int(digits, source), (1 << 64) - 1)
    if negative:
        n = (-n) & ((1 << 64) - 1)
    sign = ""
    if target < 0 and n >> 63:
        n, sign = (1 << 64) - n, "-"
    base, out = abs(target), ""
    while True:
        n, d = divmod(n, base)
        out = _DIGITS[d] + out
        if not n:
            break
    return sign + out


SUM_REAL_FIXED = -1145128260  # measured: what SUM_REAL of NUMBERs adds to the sum of its values, each rounded


def sum_real_fixed(values: list | None) -> int | None:
    vals = [v for v in values or [] if v is not None]
    if not vals:
        return None
    return SUM_REAL_FIXED + sum(int(math.floor(abs(float(v)) + 0.5)) * (1 if float(v) >= 0 else -1) for v in vals)


def sum_real_float(values: list | None) -> int | None:
    """SUM_REAL of FLOATs: the bits of the double their sum is, as a signed 64-bit number."""
    vals = [v for v in values or [] if v is not None]
    if not vals:
        return None
    return struct.unpack("<q", struct.pack("<d", math.fsum(float(v) for v in vals)))[0]


# Heule, Nunkesser, Hall (2013), "HyperLogLog in practice", appendix: raw estimates and their bias at precision 12
_RAW_12 = None
_BIAS_12 = None


def _bias(e: float) -> float:
    global _RAW_12, _BIAS_12  # noqa: PLW0603 - read once
    if _RAW_12 is None:
        from importlib import resources
        data = json.loads(resources.files("fauxflake").joinpath("hll_bias_12.json").read_text())
        _RAW_12, _BIAS_12 = data["raw"], data["bias"]
    raw, bias = _RAW_12, _BIAS_12
    if e <= raw[0]:
        return bias[0]
    if e >= raw[-1]:
        return bias[-1]
    lo, hi = 0, len(raw) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if raw[mid] <= e:
            lo = mid
        else:
            hi = mid
    t = (e - raw[lo]) / (raw[hi] - raw[lo])
    return bias[lo] + (bias[hi] - bias[lo]) * t
