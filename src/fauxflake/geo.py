"""GEOGRAPHY and GEOMETRY, as Snowflake has them (measured on a live account, 2026-11-26).

A value is a GeoJSON document (Snowflake prints GEOGRAPHY as one: keys sorted, two-space indent). A coordinate read
from text keeps the number it was written as (1.50 prints 1.5, 2.0 prints 2); one computed, or given as a FLOAT
(ST_MAKEPOINT), is a FLOAT and prints as Snowflake prints a FLOAT inside a VARIANT (1.500000000000000e+00). Here a
FLOAT coordinate is written with the emulator's FLOAT mark (jsondoc: 17 digits and an exponent) and shown as Snowflake
shows it by the same hook that shows VARIANTs.

GEOGRAPHY is on the sphere, as S2 (Snowflake's library) computes: points are unit vectors, edges are great-circle arcs,
distances are angles times 6,371,010 m (S2's Earth radius; measured: ST_DISTANCE between two cities divided by their
central angle). HAVERSINE is the textbook formula on 6,371 km. GEOMETRY is planar.
"""

from __future__ import annotations

import json
import math
import re

RADIUS = 6371010.0  # S2Earth::RadiusMeters, which ST_DISTANCE uses
FLOAT_MARK = "%.16e"


class GeoError(ValueError):
    pass


# ---------------------------------------------------------------- numbers: written ones and FLOATs

class F(float):
    """A coordinate that is a FLOAT (computed, or given as one): printed with the FLOAT mark."""


def _num(text: str):  # noqa: ANN202
    """A number as written in WKT / GeoJSON: Snowflake prints it back canonically (1.50 -> 1.5, 2.0 -> 2)."""
    v = float(text)
    return int(v) if v.is_integer() and abs(v) < 1e15 and "e" not in text.lower() else v


def _out(v) -> str:  # noqa: ANN001
    if isinstance(v, F):
        if math.isnan(v) or math.isinf(v):
            return "null"
        return FLOAT_MARK % float(v)
    if isinstance(v, int) or (isinstance(v, float) and v.is_integer() and abs(v) < 1e15):
        return str(int(v))
    return repr(float(v))


def dumps(doc) -> str:  # noqa: ANN001
    """The emulator's text of a document: JSON with sorted keys, FLOAT coordinates marked."""
    if isinstance(doc, dict):
        return "{" + ",".join(json.dumps(k) + ":" + dumps(doc[k]) for k in sorted(doc)) + "}"
    if isinstance(doc, (list, tuple)):
        return "[" + ",".join(dumps(x) for x in doc) + "]"
    if isinstance(doc, (int, float)) and not isinstance(doc, bool):
        return _out(doc)
    return json.dumps(doc)


def _floats(text: str) -> str:
    return re.sub(r"-?[0-9]\.[0-9]{16}e[+-][0-9]+", lambda m: "%.15e" % float(m.group()), text)


def shown(doc_text: str) -> str:
    """As Snowflake sends GeoJSON: indented by two spaces, keys sorted, FLOATs with 16 digits."""
    doc = json.loads(doc_text, parse_float=lambda s: _Raw(s), parse_int=lambda s: _Raw(s))
    return _floats(_pretty(doc, 0))


class _Raw(str):
    pass


def _pretty(doc, depth: int) -> str:  # noqa: ANN001
    pad, inner = "  " * depth, "  " * (depth + 1)
    if isinstance(doc, dict):
        if not doc:
            return "{}"
        return "{\n" + ",\n".join(f"{inner}{json.dumps(k)}: {_pretty(doc[k], depth + 1)}" for k in sorted(doc)) + f"\n{pad}}}"
    if isinstance(doc, list):
        if not doc:
            return "[]"
        return "[\n" + ",\n".join(f"{inner}{_pretty(x, depth + 1)}" for x in doc) + f"\n{pad}]"
    if isinstance(doc, _Raw):
        return str(doc)
    return json.dumps(doc)


def loads(text: str) -> dict:
    """The emulator's text back into a document, FLOAT coordinates kept as FLOATs."""
    return json.loads(text, parse_float=lambda s: F(s) if re.fullmatch(r"-?[0-9]\.[0-9]{16}e[+-][0-9]+", s) else _num(s),
                      parse_int=int)


# ---------------------------------------------------------------- reading WKT, EWKT and GeoJSON

_TYPES = {"POINT": "Point", "LINESTRING": "LineString", "POLYGON": "Polygon", "MULTIPOINT": "MultiPoint",
          "MULTILINESTRING": "MultiLineString", "MULTIPOLYGON": "MultiPolygon", "GEOMETRYCOLLECTION": "GeometryCollection"}
_TOKEN = re.compile(r"\s*([A-Za-z]+|\(|\)|,|[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)")


def parse(text: str, srid: int | None = None) -> tuple[dict, int | None]:
    """A geometry from WKT, EWKT (SRID=n;WKT), GeoJSON or hex WKB. Returns the document and the SRID it named."""
    if text is None:
        raise GeoError("null")
    s = text.strip()
    m = re.match(r"(?i)^SRID\s*=\s*(\d+)\s*;\s*(.*)$", s, re.S)
    if m:
        srid, s = int(m.group(1)), m.group(2)
    if s.startswith("{"):
        try:
            doc = json.loads(s, parse_float=_num, parse_int=int)
        except ValueError as e:
            raise GeoError(f"Error parsing GeoJSON: {e}") from e
        return _geojson(doc), srid
    if re.fullmatch(r"[0-9A-Fa-f]+", s) and len(s) % 2 == 0 and len(s) >= 10:
        return _wkb(bytes.fromhex(s)), srid
    tokens = [t for t in _TOKEN.findall(s)]
    if "".join(tokens).replace(" ", "") != re.sub(r"\s+", "", s):
        raise GeoError(f"Error parsing WKT input: {text}")
    doc, rest = _wkt(tokens, 0)
    if rest != len(tokens):
        raise GeoError(f"Error parsing WKT input: {text}")
    return doc, srid


def _geojson(doc) -> dict:  # noqa: ANN001
    if not isinstance(doc, dict) or doc.get("type") not in _TYPES.values() and doc.get("type") not in ("Feature",):
        raise GeoError("Error parsing GeoJSON: unknown type")
    if doc["type"] == "Feature":
        return _geojson(doc.get("geometry"))
    if doc["type"] == "GeometryCollection":
        return {"type": "GeometryCollection", "geometries": [_geojson(g) for g in doc.get("geometries", [])]}
    return {"type": doc["type"], "coordinates": doc.get("coordinates")}


def _wkt(tokens: list[str], i: int) -> tuple[dict, int]:
    word = tokens[i].upper()
    if word not in _TYPES:
        raise GeoError(f"Error parsing WKT input: unexpected {tokens[i]}")
    i += 1
    while i < len(tokens) and tokens[i].upper() in ("Z", "M", "ZM"):
        i += 1
    kind = _TYPES[word]
    if i < len(tokens) and tokens[i].upper() == "EMPTY":
        return ({"type": kind, "geometries": []} if kind == "GeometryCollection" else {"type": kind, "coordinates": []}), i + 1
    if kind == "GeometryCollection":
        i = _expect(tokens, i, "(")
        parts = []
        while True:
            g, i = _wkt(tokens, i)
            parts.append(g)
            if tokens[i] == ",":
                i += 1
                continue
            return {"type": kind, "geometries": parts}, _expect(tokens, i, ")")
    depth = {"Point": 0, "LineString": 1, "MultiPoint": 1, "Polygon": 2, "MultiLineString": 2, "MultiPolygon": 3}[kind]
    coords, i = _coords(tokens, i, depth, multipoint=kind == "MultiPoint")
    return {"type": kind, "coordinates": coords}, i


def _expect(tokens, i, what):  # noqa: ANN001, ANN202
    if i >= len(tokens) or tokens[i] != what:
        raise GeoError(f"Error parsing WKT input: expected {what}")
    return i + 1


def _coords(tokens, i, depth, multipoint=False):  # noqa: ANN001, ANN202, FBT002
    i = _expect(tokens, i, "(")
    if depth == 0:
        nums = []
        while tokens[i] not in (")", ","):
            nums.append(_num(tokens[i]))
            i += 1
        return nums, _expect(tokens, i, ")")
    items = []
    while True:
        if depth == 1:
            if multipoint and tokens[i] == "(":
                item, i = _coords(tokens, i, 0)
            else:
                item = []
                while tokens[i] not in (")", ","):
                    item.append(_num(tokens[i]))
                    i += 1
        else:
            item, i = _coords(tokens, i, depth - 1, multipoint)
        items.append(item)
        if tokens[i] == ",":
            i += 1
            continue
        return items, _expect(tokens, i, ")")


def _wkb(data: bytes) -> dict:
    import struct

    pos = 0

    def read(fmt: str, little: bool):  # noqa: ANN202, FBT001
        nonlocal pos
        size = struct.calcsize(fmt)
        out = struct.unpack(("<" if little else ">") + fmt, data[pos:pos + size])
        pos += size
        return out

    def geometry() -> dict:
        nonlocal pos
        little = data[pos] == 1
        pos += 1
        (code,) = read("I", little)
        has_srid = bool(code & 0x20000000)
        dims = 3 if (code & 0x80000000) or (code % 1000) // 1000 or 1000 <= code % 10000 < 2000 else 2
        code &= 0xFFFF
        code %= 1000
        if has_srid:
            read("I", little)

        def point() -> list:
            return [F(v) for v in read("d" * dims, little)]

        def ring() -> list:
            (n,) = read("I", little)
            return [point() for _ in range(n)]

        kinds = {1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon", 7: "GeometryCollection"}
        kind = kinds.get(code)
        if kind == "Point":
            return {"type": kind, "coordinates": point()}
        if kind == "LineString":
            return {"type": kind, "coordinates": ring()}
        if kind == "Polygon":
            (n,) = read("I", little)
            return {"type": kind, "coordinates": [ring() for _ in range(n)]}
        if kind in ("MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"):
            (n,) = read("I", little)
            parts = [geometry() for _ in range(n)]
            if kind == "GeometryCollection":
                return {"type": kind, "geometries": parts}
            return {"type": kind, "coordinates": [p["coordinates"] for p in parts]}
        raise GeoError("Error parsing WKB input")

    return geometry()


def validate_geography(doc: dict) -> dict:
    """Longitudes in [-180, 180], latitudes in [-90, 90] (measured: 100205 'Invalid Lng/Lat pair')."""
    for p in points(doc):
        if len(p) < 2 or not (-180 <= p[0] <= 180) or not (-90 <= p[1] <= 90):
            shown_pair = ",".join("%g" % v for v in p[:2])
            raise GeoError(f"GeoJSON::{doc['type']}: Invalid Lng/Lat pair: '{shown_pair}'.")
    return doc


# ---------------------------------------------------------------- writing WKT

def _wkt_num(v) -> str:  # noqa: ANN001
    if isinstance(v, int) or (isinstance(v, float) and v.is_integer() and abs(v) < 1e15):
        return str(int(v))
    return repr(float(v))


def _wkt_coords(c, depth: int) -> str:  # noqa: ANN001
    if depth == 0:
        return " ".join(_wkt_num(v) for v in c)
    return ",".join(("(" + _wkt_coords(x, depth - 1) + ")") if depth > 1 else _wkt_coords(x, depth - 1) for x in c)


def wkt(doc: dict) -> str:
    kind = doc["type"]
    word = {v: k for k, v in _TYPES.items()}[kind]
    if kind == "GeometryCollection":
        return f"{word}(" + ",".join(wkt(g) for g in doc["geometries"]) + ")" if doc["geometries"] else f"{word} EMPTY"
    if not doc["coordinates"]:
        return f"{word} EMPTY"
    if kind == "Point":
        return f"POINT({_wkt_coords(doc['coordinates'], 0)})"
    if kind == "MultiPoint":
        return "MULTIPOINT(" + ",".join(_wkt_coords(p, 0) for p in doc["coordinates"]) + ")"
    depth = {"LineString": 1, "Polygon": 2, "MultiLineString": 2, "MultiPolygon": 3}[kind]
    return f"{word}(" + _wkt_coords(doc["coordinates"], depth) + ")"


# ---------------------------------------------------------------- structure

def parts(doc: dict) -> list[dict]:
    """The simple geometries a document is made of."""
    kind = doc["type"]
    if kind == "GeometryCollection":
        return [p for g in doc["geometries"] for p in parts(g)]
    if kind.startswith("Multi"):
        return [{"type": kind[5:], "coordinates": c} for c in doc["coordinates"]]
    return [doc]


def points(doc: dict) -> list[list]:
    out = []
    for p in parts(doc):
        c = p["coordinates"]
        if p["type"] == "Point":
            if c:
                out.append(c)
        elif p["type"] == "LineString":
            out.extend(c)
        else:
            out.extend(v for ring in c for v in ring)
    return out


def dimension(doc: dict) -> int:
    kinds = {p["type"] for p in parts(doc)}
    return 2 if "Polygon" in kinds else 1 if "LineString" in kinds else 0


# ---------------------------------------------------------------- the sphere

def unit(p) -> tuple[float, float, float]:  # noqa: ANN001
    lng, lat = math.radians(float(p[0])), math.radians(float(p[1]))
    c = math.cos(lat)
    return (c * math.cos(lng), c * math.sin(lng), math.sin(lat))


def _sub(a, b):  # noqa: ANN001, ANN202
    return (a[0] - b[0], a[1] - b[1], a[2] - b[2])


def _add(a, b):  # noqa: ANN001, ANN202
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2])


def _dot(a, b) -> float:  # noqa: ANN001
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


def _cross(a, b):  # noqa: ANN001, ANN202
    return (a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0])


def _norm(a) -> float:  # noqa: ANN001
    return math.sqrt(_dot(a, a))


def _scale(a, k):  # noqa: ANN001, ANN202
    return (a[0] * k, a[1] * k, a[2] * k)


def _normalize(a):  # noqa: ANN001, ANN202
    n = _norm(a)
    return a if n == 0 else _scale(a, 1.0 / n)


def angle(a, b) -> float:  # noqa: ANN001
    """The angle between two unit vectors, as S2 measures it (atan2 of the cross and dot products)."""
    return math.atan2(_norm(_cross(a, b)), _dot(a, b))


def lnglat(v) -> list[float]:  # noqa: ANN001
    return [math.degrees(math.atan2(v[1], v[0])), math.degrees(math.atan2(v[2], math.sqrt(v[0] ** 2 + v[1] ** 2)))]


def _point_arc(p, a, b) -> float:  # noqa: ANN001
    """The angle from p to the great-circle arc a-b."""
    n = _cross(a, b)
    if _norm(n) == 0:
        return angle(p, a)
    # p's projection onto the arc's great circle lies within the arc when it is on the inner side of both ends
    if _dot(_cross(n, a), p) > 0 and _dot(_cross(b, n), p) > 0:
        return abs(math.asin(max(-1.0, min(1.0, _dot(p, _normalize(n))))))
    return min(angle(p, a), angle(p, b))


def _arcs_cross(a, b, c, d) -> bool:  # noqa: ANN001
    ab, cd = _cross(a, b), _cross(c, d)
    s1, s2 = _dot(ab, c), _dot(ab, d)
    s3, s4 = _dot(cd, a), _dot(cd, b)
    if s1 * s2 > 0 or s3 * s4 > 0:
        return False
    if s1 == s2 == 0:  # (on one great circle: overlapping or not)
        return min(_point_arc(c, a, b), _point_arc(d, a, b), _point_arc(a, c, d), _point_arc(b, c, d)) < 1e-15
    return _dot(_add(a, b), _add(c, d)) > 0 or _point_arc(c, a, b) < 1e-15 or _point_arc(a, c, d) < 1e-15


def _edges(doc: dict) -> list[tuple]:
    out = []
    for p in parts(doc):
        if p["type"] == "LineString":
            vs = [unit(v) for v in p["coordinates"]]
            out.extend(zip(vs, vs[1:]))
        elif p["type"] == "Polygon":
            for ring in p["coordinates"]:
                vs = [unit(v) for v in ring]
                out.extend(zip(vs, vs[1:]))
    return out


def _inside(p, ring) -> bool:  # noqa: ANN001
    """Is the unit vector p inside a ring (unit vectors, closed), by the angle the ring winds around p."""
    total = 0.0
    for a, b in zip(ring, ring[1:]):
        if angle(p, a) < 1e-15:
            return True
        ta, tb = _sub(a, _scale(p, _dot(a, p))), _sub(b, _scale(p, _dot(b, p)))
        total += math.atan2(_dot(_cross(ta, tb), p), _dot(ta, tb))
    return abs(total) > math.pi


def _in_polygon(p, polygon) -> bool:  # noqa: ANN001
    rings = [[unit(v) for v in ring] for ring in polygon]
    return bool(rings) and _inside(p, rings[0]) and not any(_inside(p, r) for r in rings[1:])


def _polygons(doc: dict) -> list:
    return [p["coordinates"] for p in parts(doc) if p["type"] == "Polygon"]


def distance_angle(x: dict, y: dict) -> float:
    """The smallest angle between two geographies (0 when they touch or one holds the other)."""
    xs, ys = points(x), points(y)
    if not xs or not ys:
        return float("nan")
    if intersects(x, y):
        return 0.0
    best = math.inf
    xe, ye = _edges(x), _edges(y)
    for p in (unit(v) for v in xs):
        best = min(best, min((_point_arc(p, a, b) for a, b in ye), default=min(angle(p, unit(q)) for q in ys)))
    for p in (unit(v) for v in ys):
        best = min(best, min((_point_arc(p, a, b) for a, b in xe), default=min(angle(p, unit(q)) for q in xs)))
    return best


def distance(x: dict, y: dict) -> float:
    return distance_angle(x, y) * RADIUS


def intersects(x: dict, y: dict) -> bool:
    xs, ys = points(x), points(y)
    for p in xs:
        u = unit(p)
        if any(_in_polygon(u, poly) for poly in _polygons(y)) or any(_point_arc(u, a, b) < 1e-15 for a, b in _edges(y)) \
                or any(angle(u, unit(q)) < 1e-15 for q in ys):
            return True
    for p in ys:
        u = unit(p)
        if any(_in_polygon(u, poly) for poly in _polygons(x)) or any(_point_arc(u, a, b) < 1e-15 for a, b in _edges(x)):
            return True
    return any(_arcs_cross(a, b, c, d) for a, b in _edges(x) for c, d in _edges(y))


def contains(x: dict, y: dict) -> bool:
    """Every point of y in x (and not only on its boundary, for a polygon: the boundary is not inside)."""
    polys = _polygons(x)
    ys = points(y)
    if not ys:
        return False
    if polys:
        if not all(any(_in_polygon(unit(p), poly) for poly in polys) for p in ys):
            return False
        # an edge of y that leaves x crosses one of x's edges
        return not any(_arcs_cross(a, b, c, d) and not (_point_arc(c, a, b) < 1e-15 or _point_arc(d, a, b) < 1e-15)
                       for a, b in _edges(y) for c, d in _edges(x))
    edges = _edges(x)
    if edges:
        return all(any(_point_arc(unit(p), a, b) < 1e-15 for a, b in edges) for p in ys)
    xs = [unit(p) for p in points(x)]
    return all(any(angle(unit(p), q) < 1e-15 for q in xs) for p in ys)


def _triangle_area(a, b, c) -> float:  # noqa: ANN001
    """The area of a spherical triangle (L'Huilier), signed by its orientation."""
    sa, sb, sc = angle(b, c), angle(c, a), angle(a, b)
    s = 0.5 * (sa + sb + sc)
    t = math.tan(0.5 * s) * math.tan(0.5 * (s - sa)) * math.tan(0.5 * (s - sb)) * math.tan(0.5 * (s - sc))
    area = 4 * math.atan(math.sqrt(max(0.0, t)))
    return area if _dot(_cross(a, b), c) >= 0 else -area


def _ring_area(ring) -> float:  # noqa: ANN001
    vs = [unit(v) for v in ring]
    if len(vs) > 1 and vs[0] == vs[-1]:
        vs = vs[:-1]
    total = sum(_triangle_area(vs[0], vs[i], vs[i + 1]) for i in range(1, len(vs) - 1))
    return abs(total)


def area(doc: dict) -> float:
    total = 0.0
    for poly in _polygons(doc):
        if poly:
            total += _ring_area(poly[0]) - sum(_ring_area(r) for r in poly[1:])
    return total * RADIUS * RADIUS


def length(doc: dict) -> float:
    total = 0.0
    for p in parts(doc):
        if p["type"] == "LineString":
            vs = [unit(v) for v in p["coordinates"]]
            total += sum(angle(a, b) for a, b in zip(vs, vs[1:]))
    return total * RADIUS


def perimeter(doc: dict) -> float:
    total = 0.0
    for poly in _polygons(doc):
        for ring in poly:
            vs = [unit(v) for v in ring]
            total += sum(angle(a, b) for a, b in zip(vs, vs[1:]))
    return total * RADIUS


def _true_centroid_triangle(a, b, c):  # noqa: ANN001, ANN202
    """S2's TrueCentroid of a spherical triangle: the integral of position over its area (not normalized)."""
    def r(x, y):  # noqa: ANN001, ANN202
        s = angle(x, y)
        return 1.0 if s == 0 else s / math.sin(s)

    ra, rb, rc = r(b, c), r(c, a), r(a, b)
    x = (a[0], b[0] - a[0], c[0] - a[0])
    y = (a[1], b[1] - a[1], c[1] - a[1])
    z = (a[2], b[2] - a[2], c[2] - a[2])
    rr = (ra, rb - ra, rc - ra)
    return _scale((_dot(_cross(y, z), rr), _dot(_cross(z, x), rr), _dot(_cross(x, y), rr)), 0.5)


def centroid(doc: dict) -> list[float] | None:
    """The centroid S2 gives: of the polygons' areas if there are any, else of the lines' lengths, else of the points."""
    polys = _polygons(doc)
    total = (0.0, 0.0, 0.0)
    if polys:
        for poly in polys:
            for k, ring in enumerate(poly):
                vs = [unit(v) for v in ring]
                if len(vs) > 1 and vs[0] == vs[-1]:
                    vs = vs[:-1]
                sign = 1.0 if k == 0 else -1.0
                for i in range(1, len(vs) - 1):
                    t = _true_centroid_triangle(vs[0], vs[i], vs[i + 1])
                    orient = 1.0 if _dot(_cross(vs[0], vs[i]), vs[i + 1]) >= 0 else -1.0
                    total = _add(total, _scale(t, sign * orient))
                # (a ring wound clockwise is the same polygon in Snowflake: its centroid points the same way)
        if _dot(total, unit(polys[0][0][0])) < 0:
            total = _scale(total, -1.0)
    else:
        edges = _edges(doc)
        if edges:
            for a, b in edges:
                vsum = _add(a, b)
                n = _norm(vsum)
                if n > 0:
                    total = _add(total, _scale(vsum, _norm(_sub(a, b)) / n))
        else:
            for p in points(doc):
                total = _add(total, unit(p))
    if _norm(total) == 0:
        return None
    return lnglat(_normalize(total))


def azimuth(a: list, b: list) -> float | None:
    """The bearing from a to b, radians clockwise from north, on the sphere."""
    lng1, lat1, lng2, lat2 = (math.radians(float(v)) for v in (a[0], a[1], b[0], b[1]))
    if lng1 == lng2 and lat1 == lat2:
        return None
    y = math.sin(lng2 - lng1) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lng2 - lng1)
    return math.atan2(y, x) % (2 * math.pi)


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """HAVERSINE(lat1, lon1, lat2, lon2) in kilometres, on a 6,371 km sphere (measured)."""
    p1, p2, dlat, dlng = (math.radians(v) for v in (lat1, lat2, lat2 - lat1, lng2 - lng1))
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlng / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))


_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash(lng: float, lat: float, precision: int = 20) -> str:
    lat_lo, lat_hi, lng_lo, lng_hi = -90.0, 90.0, -180.0, 180.0
    out, bits, ch, even = [], 0, 0, True
    while len(out) < precision:
        if even:
            mid = (lng_lo + lng_hi) / 2
            if lng >= mid:
                ch, lng_lo = ch * 2 + 1, mid
            else:
                ch, lng_hi = ch * 2, mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                ch, lat_lo = ch * 2 + 1, mid
            else:
                ch, lat_hi = ch * 2, mid
        even, bits = not even, bits + 1
        if bits == 5:
            out.append(_BASE32[ch])
            bits, ch = 0, 0
    return "".join(out)


# ---------------------------------------------------------------- the plane (GEOMETRY)

def _planar_point_segment(p, a, b) -> float:  # noqa: ANN001
    ax, ay, bx, by, px, py = float(a[0]), float(a[1]), float(b[0]), float(b[1]), float(p[0]), float(p[1])
    dx, dy = bx - ax, by - ay
    if dx == dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def _planar_edges(doc: dict) -> list:
    out = []
    for p in parts(doc):
        if p["type"] == "LineString":
            out.extend(zip(p["coordinates"], p["coordinates"][1:]))
        elif p["type"] == "Polygon":
            for ring in p["coordinates"]:
                out.extend(zip(ring, ring[1:]))
    return out


def _planar_in_ring(p, ring) -> bool:  # noqa: ANN001
    x, y, inside = float(p[0]), float(p[1]), False
    for a, b in zip(ring, ring[1:]):
        ax, ay, bx, by = float(a[0]), float(a[1]), float(b[0]), float(b[1])
        if (ay > y) != (by > y) and x < (bx - ax) * (y - ay) / (by - ay) + ax:
            inside = not inside
    return inside


def _planar_cross(a, b, c, d) -> bool:  # noqa: ANN001
    def orient(p, q, r):  # noqa: ANN001, ANN202
        return (float(q[0]) - float(p[0])) * (float(r[1]) - float(p[1])) - (float(q[1]) - float(p[1])) * (float(r[0]) - float(p[0]))

    o1, o2, o3, o4 = orient(a, b, c), orient(a, b, d), orient(c, d, a), orient(c, d, b)
    if o1 * o2 < 0 and o3 * o4 < 0:
        return True
    return any(_planar_point_segment(p, s, t) == 0 for p, s, t in ((c, a, b), (d, a, b), (a, c, d), (b, c, d)))


def planar_intersects(x: dict, y: dict) -> bool:
    for p in points(x):
        if any(_planar_in_ring(p, poly[0]) and not any(_planar_in_ring(p, h) for h in poly[1:]) for poly in _polygons(y)) \
                or any(_planar_point_segment(p, a, b) == 0 for a, b in _planar_edges(y)) \
                or any(float(p[0]) == float(q[0]) and float(p[1]) == float(q[1]) for q in points(y)):
            return True
    for p in points(y):
        if any(_planar_in_ring(p, poly[0]) and not any(_planar_in_ring(p, h) for h in poly[1:]) for poly in _polygons(x)) \
                or any(_planar_point_segment(p, a, b) == 0 for a, b in _planar_edges(x)):
            return True
    return any(_planar_cross(a, b, c, d) for a, b in _planar_edges(x) for c, d in _planar_edges(y))


def planar_distance(x: dict, y: dict) -> float:
    if planar_intersects(x, y):
        return 0.0
    best = math.inf
    xe, ye = _planar_edges(x), _planar_edges(y)
    for p in points(x):
        best = min(best, min((_planar_point_segment(p, a, b) for a, b in ye),
                             default=min(math.hypot(float(p[0]) - float(q[0]), float(p[1]) - float(q[1])) for q in points(y))))
    for p in points(y):
        best = min(best, min((_planar_point_segment(p, a, b) for a, b in xe),
                             default=min(math.hypot(float(p[0]) - float(q[0]), float(p[1]) - float(q[1])) for q in points(x))))
    return best


def _shoelace(ring) -> float:  # noqa: ANN001
    return abs(sum(float(a[0]) * float(b[1]) - float(b[0]) * float(a[1]) for a, b in zip(ring, ring[1:]))) / 2


def planar_area(doc: dict) -> float:
    return sum(_shoelace(poly[0]) - sum(_shoelace(h) for h in poly[1:]) for poly in _polygons(doc) if poly)


def planar_length(doc: dict) -> float:
    return sum(math.hypot(float(b[0]) - float(a[0]), float(b[1]) - float(a[1]))
               for p in parts(doc) if p["type"] == "LineString" for a, b in zip(p["coordinates"], p["coordinates"][1:]))


def planar_contains(x: dict, y: dict) -> bool:
    polys = _polygons(x)
    if not polys:
        return all(any(_planar_point_segment(p, a, b) == 0 for a, b in _planar_edges(x)) or
                   any(float(p[0]) == float(q[0]) and float(p[1]) == float(q[1]) for q in points(x)) for p in points(y))
    inside = all(any(_planar_in_ring(p, poly[0]) and not any(_planar_in_ring(p, h) for h in poly[1:]) for poly in polys) for p in points(y))
    return inside and not any(_planar_cross(a, b, c, d) and not any(_planar_point_segment(q, c, d) == 0 for q in (a, b))
                              for a, b in _planar_edges(y) for c, d in _planar_edges(x))


# ---------------------------------------------------------------- the emulator's side
#
# A GEOGRAPHY is DuckDB's STRUCT(G VARCHAR, GEOGRAPHY INTEGER), a GEOMETRY STRUCT(G VARCHAR, GEOMETRY INTEGER): G holds the
# document, the other field (whose name is the kind) the SRID. Functions that give back what they were given (a centroid,
# a start point) write a new G into the same struct (struct_update), so a GEOMETRY stays one. The Python side is handed a
# value as to_json of the struct, which names its kind.

GEOGRAPHY_TYPE = "STRUCT(G VARCHAR, GEOGRAPHY INTEGER)"
GEOMETRY_TYPE = "STRUCT(G VARCHAR, GEOMETRY INTEGER)"


def _raise(message: str, errno: int = 100205, state: str = "P0000") -> ValueError:
    return ValueError(f"\x1ffxerr:{errno}:{state}:{message}\x1f")


def _value(text: str | None):  # noqa: ANN202
    """(kind, srid, document) of a value handed over as to_json of its struct."""
    if text is None:
        return None
    obj = json.loads(text)
    if not isinstance(obj, dict) or obj.get("G") is None:
        return None
    kind = "GEOGRAPHY" if "GEOGRAPHY" in obj else "GEOMETRY"
    return kind, obj.get(kind), loads(obj["G"])


def from_input(given: str | None, kind: str, lenient: bool) -> str | None:  # noqa: FBT001
    """TO_GEOGRAPHY / TO_GEOMETRY (TRY_ when `lenient`) of text (T:), a VARIANT or a struct (J:) or WKB (X:)."""
    if given is None:
        return None
    tag, body = given[:2], given[2:]
    try:
        if tag == "J:":
            obj = json.loads(body)
            if isinstance(obj, dict) and "G" in obj:
                doc = loads(obj["G"]) if obj["G"] is not None else None
                if doc is None:
                    return None
            elif isinstance(obj, str):
                doc, _ = parse(obj)
            else:
                doc = _geojson(loads(body))
        elif tag == "X:":
            doc = _wkb(bytes.fromhex(body))
        else:
            doc, _ = parse(body)
        if kind == "GEOGRAPHY":
            validate_geography(doc)
        else:
            doc = _as_floats(doc)  # (measured: a GEOMETRY holds doubles, POINT(3 4) shows [3.0, 4.0])
    except GeoError as e:
        if lenient:
            return None
        if "Invalid Lng/Lat" in str(e):
            raise _raise(str(e)) from e
        word = re.match(r"\s*([A-Za-z]+)", body or "")
        said = f'unknown keyword "{word.group(1)}", pos {len(word.group(1)) + 1}{body.strip()[:40]}' if word else str(e)
        raise _raise(f"Error parsing Geo input: {said}. Did not recognize valid GeoJSON, (E)WKT or (E)WKB.", 100256) from e
    except (ValueError, KeyError, IndexError, TypeError) as e:
        if lenient:
            return None
        raise _raise(f"Error parsing Geo input: {e}. Did not recognize valid GeoJSON, (E)WKT or (E)WKB.", 100256) from e
    return dumps(doc)


def _as_floats(doc):  # noqa: ANN001, ANN202
    if isinstance(doc, dict):
        return {k: (_as_floats(v) if k in ("coordinates", "geometries") else v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_as_floats(v) for v in doc]
    if isinstance(doc, (int, float)) and not isinstance(doc, bool):
        return F(doc)
    return doc


def srid_of(given: str | None, srid: int | None) -> int | None:
    if given is None:
        return None
    m = re.match(r"(?is)^T:\s*SRID\s*=\s*(\d+)\s*;", given)
    return int(m.group(1)) if m else (srid if srid is not None else 0)


def point(x: float | None, y: float | None) -> str | None:
    if x is None or y is None:
        return None
    return dumps({"type": "Point", "coordinates": [F(x), F(y)]})


def number(op: str, a: str | None, b: str | None, extra: float | None) -> float | None:  # noqa: C901, PLR0911
    va, vb = _value(a), _value(b)
    if va is None or (b is not None and vb is None):
        return None
    kind, _srid, doc = va
    other = vb[2] if vb else None
    planar = kind == "GEOMETRY"
    if op == "distance":
        return planar_distance(doc, other) if planar else distance(doc, other)
    if op == "length":
        return planar_length(doc) if planar else length(doc)
    if op == "area":
        return planar_area(doc) if planar else area(doc)
    if op == "perimeter":
        if planar:
            return sum(math.hypot(float(q[0]) - float(p[0]), float(q[1]) - float(p[1])) for poly in _polygons(doc) for ring in poly
                       for p, q in zip(ring, ring[1:]))
        return perimeter(doc)
    if op in ("x", "y"):
        if doc["type"] != "Point" or not doc["coordinates"]:
            raise _raise(f"Type {doc['type']} is not supported as argument to ST_{op.upper()}.", 100281)
        return float(doc["coordinates"][0 if op == "x" else 1])
    if op == "azimuth":
        if doc["type"] != "Point" or other is None or other["type"] != "Point":
            return None
        if planar:
            dx, dy = float(other["coordinates"][0]) - float(doc["coordinates"][0]), float(other["coordinates"][1]) - float(doc["coordinates"][1])
            return None if dx == dy == 0 else math.atan2(dx, dy) % (2 * math.pi)
        return azimuth(doc["coordinates"], other["coordinates"])
    if op == "hausdorff":
        # measured: on a GEOGRAPHY too, the distance in the plane of its coordinates (sqrt(2) from 0,0 to 1,1)
        xs, ys = points(doc), points(other)
        if not xs or not ys:
            return None
        d = lambda p, q: math.hypot(float(p[0]) - float(q[0]), float(p[1]) - float(q[1]))  # noqa: E731
        return max(max(min(d(p, q) for q in ys) for p in xs), max(min(d(p, q) for q in xs) for p in ys))
    raise _raise(f"unknown geospatial function {op}", 2140, "42601")


def boolean(op: str, a: str | None, b: str | None, extra: float | None) -> bool | None:  # noqa: PLR0911
    va, vb = _value(a), _value(b)
    if va is None or (op != "isvalid" and vb is None):
        return None
    planar = va[0] == "GEOMETRY"
    x, y = va[2], vb[2] if vb else None
    if op == "isvalid":
        return True
    if op == "intersects":
        return planar_intersects(x, y) if planar else intersects(x, y)
    if op == "disjoint":
        return not (planar_intersects(x, y) if planar else intersects(x, y))
    if op in ("contains", "covers"):
        return planar_contains(x, y) if planar else contains(x, y)
    if op in ("within", "coveredby"):
        return planar_contains(y, x) if planar else contains(y, x)
    if op == "dwithin":
        return (planar_distance(x, y) if planar else distance(x, y)) <= (extra or 0)
    raise _raise(f"unknown geospatial function {op}", 2140, "42601")


def text(op: str, a: str | None, extra: float | None) -> str | None:
    va = _value(a)
    if va is None:
        return None
    kind, srid, doc = va
    if op == "wkt":
        return wkt(doc)
    if op == "ewkt":
        return f"SRID={srid if srid is not None else 4326};{wkt(doc)}"
    if op == "geojson":
        return dumps(doc)
    if op == "geohash":
        if doc["type"] != "Point":
            c = centroid(doc) if kind == "GEOGRAPHY" else None
            if c is None:
                return None
            return geohash(c[0], c[1], int(extra or 20))
        return geohash(float(doc["coordinates"][0]), float(doc["coordinates"][1]), int(extra or 20))
    raise _raise(f"unknown geospatial function {op}", 2140, "42601")


def count(op: str, a: str | None) -> int | None:
    va = _value(a)
    if va is None:
        return None
    if op == "npoints":
        return len(points(va[2]))
    if op == "dimension":
        return dimension(va[2])
    if op == "srid":
        return 4326 if va[0] == "GEOGRAPHY" else (va[1] or 0)
    raise _raise(f"unknown geospatial function {op}", 2140, "42601")


def _planar_centroid(doc: dict) -> list[float] | None:
    polys = _polygons(doc)
    if polys:
        ax = ay = total = 0.0
        for poly in polys:
            for k, ring in enumerate(poly):
                sign = 1.0 if k == 0 else -1.0
                for p, q in zip(ring, ring[1:]):
                    cross = float(p[0]) * float(q[1]) - float(q[0]) * float(p[1])
                    total += sign * cross
                    ax += sign * (float(p[0]) + float(q[0])) * cross
                    ay += sign * (float(p[1]) + float(q[1])) * cross
        return None if total == 0 else [ax / (3 * total), ay / (3 * total)]
    pts = points(doc)
    return [sum(float(p[0]) for p in pts) / len(pts), sum(float(p[1]) for p in pts) / len(pts)] if pts else None


def shape(op: str, a: str | None, b: str | None, extra: float | None) -> str | None:  # noqa: C901, PLR0911
    va, vb = _value(a), _value(b)
    if va is None:
        return None
    kind, _srid, doc = va
    if op == "centroid":
        c = centroid(doc) if kind == "GEOGRAPHY" else _planar_centroid(doc)
        return None if c is None else dumps({"type": "Point", "coordinates": [F(c[0]), F(c[1])]})
    if op in ("startpoint", "endpoint", "pointn"):
        if doc["type"] != "LineString":
            return None
        line = doc["coordinates"]
        n = int(extra) if op == "pointn" and extra is not None else (1 if op == "startpoint" else len(line))
        n = n if n > 0 else len(line) + n + 1  # (a negative index counts from the end)
        if not 1 <= n <= len(line):
            return None
        return dumps({"type": "Point", "coordinates": [F(v) for v in line[n - 1]]})
    if op == "makeline":
        if vb is None:
            return None
        return dumps({"type": "LineString", "coordinates": [[F(v) for v in p] for p in points(doc) + points(vb[2])]})
    if op == "makepolygon":
        if doc["type"] != "LineString":
            raise _raise("Function ST_MAKEPOLYGON: the argument must be a closed linestring", 100202, "22023")
        return dumps({"type": "Polygon", "coordinates": [doc["coordinates"]]})
    if op == "envelope":
        pts = points(doc)
        if not pts:
            return None
        xs, ys = [float(p[0]) for p in pts], [float(p[1]) for p in pts]
        x0, x1, y0, y1 = F(min(xs)), F(max(xs)), F(min(ys)), F(max(ys))
        if x0 == x1 and y0 == y1:
            return dumps({"type": "Point", "coordinates": [x0, y0]})
        return dumps({"type": "Polygon", "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]})
    raise _raise(f"unknown geospatial function {op}", 2140, "42601")


def collect(values: list | None) -> str | None:
    """ST_COLLECT over a group: one point each makes a MultiPoint, lines a MultiLineString, polygons a MultiPolygon, else a
    GeometryCollection."""
    docs = [v[2] for v in (_value(t) for t in values or []) if v is not None]
    if not docs:
        return None
    simple = [p for d in docs for p in parts(d)]
    kinds = {p["type"] for p in simple}
    if len(kinds) == 1 and all(not d["type"].startswith(("Multi", "Geometry")) for d in docs):
        kind = next(iter(kinds))
        return dumps({"type": "Multi" + kind, "coordinates": [p["coordinates"] for p in simple]})
    return dumps({"type": "GeometryCollection", "geometries": docs})


def register(duck, ns: str) -> None:  # noqa: ANN001
    """The Python functions and the macros the rewrite calls for each geospatial function."""
    for name, fn, args, out in (
            ("_fxpy_geo_from", from_input, ["VARCHAR", "VARCHAR", "BOOLEAN"], "VARCHAR"),
            ("_fxpy_geo_srid", srid_of, ["VARCHAR", "INTEGER"], "INTEGER"),
            ("_fxpy_geo_point", point, ["DOUBLE", "DOUBLE"], "VARCHAR"),
            ("_fxpy_geo_number", number, ["VARCHAR", "VARCHAR", "VARCHAR", "DOUBLE"], "DOUBLE"),
            ("_fxpy_geo_bool", boolean, ["VARCHAR", "VARCHAR", "VARCHAR", "DOUBLE"], "BOOLEAN"),
            ("_fxpy_geo_text", text, ["VARCHAR", "VARCHAR", "DOUBLE"], "VARCHAR"),
            ("_fxpy_geo_count", count, ["VARCHAR", "VARCHAR"], "BIGINT"),
            ("_fxpy_geo_shape", shape, ["VARCHAR", "VARCHAR", "VARCHAR", "DOUBLE"], "VARCHAR"),
            ("_fxpy_geo_collect", collect, ["VARCHAR[]"], "VARCHAR"),
            ("_fxpy_haversine", haversine, ["DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE"], "DOUBLE")):
        try:
            duck.create_function(name, fn, args, out, null_handling="special")
        except Exception:  # noqa: BLE001, S110 - registered already on this connection
            pass
    given = ("case when typeof(x) = 'BLOB' then 'X:' || hex(x) when typeof(x) = 'JSON' or typeof(x) like 'STRUCT%' "
             "then 'J:' || to_json(x)::varchar else 'T:' || x::varchar end")
    v = "to_json({})::varchar"
    macros = [
        f"create or replace macro {ns}_sf_geo_input(x) as {given}",
        f"""create or replace macro {ns}_sf_to_geography (x) as case when x is null then null else
              {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', false), 'GEOGRAPHY': 4326}} end,
              (x, allow_invalid) as case when x is null then null else
              {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', false), 'GEOGRAPHY': 4326}} end""",
        f"""create or replace macro {ns}_sf_try_to_geography (x) as case when _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', true) is not null
              then {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', true), 'GEOGRAPHY': 4326}} end,
              (x, allow_invalid) as case when _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', true) is not null
              then {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOGRAPHY', true), 'GEOGRAPHY': 4326}} end""",
        f"""create or replace macro {ns}_sf_to_geometry (x) as case when x is null then null else
              {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOMETRY', false), 'GEOMETRY': _fxpy_geo_srid({ns}_sf_geo_input(x), null)}} end,
              (x, srid) as case when x is null then null else
              {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOMETRY', false), 'GEOMETRY': _fxpy_geo_srid({ns}_sf_geo_input(x), srid::integer)}} end""",
        f"""create or replace macro {ns}_sf_try_to_geometry (x) as case when _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOMETRY', true) is not null
              then {{'G': _fxpy_geo_from({ns}_sf_geo_input(x), 'GEOMETRY', true), 'GEOMETRY': _fxpy_geo_srid({ns}_sf_geo_input(x), null)}} end""",
        f"""create or replace macro {ns}_sf_st_makepoint(x, y) as case when x is not null and y is not null then
              {{'G': _fxpy_geo_point(x::double, y::double), 'GEOGRAPHY': 4326}} end""",
        *(f"create or replace macro {ns}_sf_st_{op} (a, b) as _fxpy_geo_number('{op}', {v.format('a')}, {v.format('b')}, null)"
          for op in ("distance", "azimuth", "hausdorff")),
        *(f"create or replace macro {ns}_sf_st_{op}(a) as _fxpy_geo_number('{op}', {v.format('a')}, null, null)"
          for op in ("length", "area", "perimeter", "x", "y")),
        *(f"create or replace macro {ns}_sf_st_{op}(a, b) as _fxpy_geo_bool('{op}', {v.format('a')}, {v.format('b')}, null)"
          for op in ("intersects", "disjoint", "contains", "covers", "within", "coveredby")),
        f"create or replace macro {ns}_sf_st_dwithin(a, b, d) as _fxpy_geo_bool('dwithin', {v.format('a')}, {v.format('b')}, d::double)",
        f"create or replace macro {ns}_sf_st_isvalid(a) as _fxpy_geo_bool('isvalid', {v.format('a')}, null, null)",
        f"create or replace macro {ns}_sf_st_aswkt(a) as _fxpy_geo_text('wkt', {v.format('a')}, null)",
        f"create or replace macro {ns}_sf_st_asewkt(a) as _fxpy_geo_text('ewkt', {v.format('a')}, null)",
        f"create or replace macro {ns}_sf_st_asgeojson(a) as _fxpy_geo_text('geojson', {v.format('a')}, null)::json",
        f"create or replace macro {ns}_sf_st_geohash (a) as _fxpy_geo_text('geohash', {v.format('a')}, null), "
        f"(a, n) as _fxpy_geo_text('geohash', {v.format('a')}, n::double)",
        *(f"create or replace macro {ns}_sf_st_{op}(a) as _fxpy_geo_count('{op}', {v.format('a')})" for op in ("npoints", "dimension", "srid")),
        f"create or replace macro {ns}_sf_haversine(lat1, lon1, lat2, lon2) as _fxpy_haversine(lat1::double, lon1::double, lat2::double, lon2::double)",
    ]
    for m in macros:
        duck.execute(m)


# what gives back a value of its argument's kind: written into the statement (rewrite) as struct_update(x, G := ...), since a
# macro cannot take a struct whose fields it does not know
SHAPES = {"ST_CENTROID": "centroid", "ST_STARTPOINT": "startpoint", "ST_ENDPOINT": "endpoint", "ST_ENVELOPE": "envelope",
          "ST_MAKEPOLYGON": "makepolygon", "ST_POLYGON": "makepolygon", "ST_POINTN": "pointn", "ST_MAKELINE": "makeline",
          "ST_COLLECT": "collect"}

# Snowflake's names for the functions above (several have more than one)
NAMES = {
    "TO_GEOGRAPHY": "_sf_to_geography", "ST_GEOGRAPHYFROMWKT": "_sf_to_geography", "ST_GEOGFROMTEXT": "_sf_to_geography",
    "ST_GEOGRAPHYFROMTEXT": "_sf_to_geography", "ST_GEOGFROMWKT": "_sf_to_geography", "ST_GEOGRAPHYFROMWKB": "_sf_to_geography",
    "ST_GEOGFROMWKB": "_sf_to_geography", "ST_GEOGRAPHYFROMEWKT": "_sf_to_geography", "ST_GEOGFROMEWKT": "_sf_to_geography",
    "TRY_TO_GEOGRAPHY": "_sf_try_to_geography", "TO_GEOMETRY": "_sf_to_geometry", "ST_GEOMETRYFROMWKT": "_sf_to_geometry",
    "ST_GEOMFROMTEXT": "_sf_to_geometry", "ST_GEOMETRYFROMTEXT": "_sf_to_geometry", "ST_GEOMFROMWKT": "_sf_to_geometry",
    "ST_GEOMETRYFROMWKB": "_sf_to_geometry", "ST_GEOMFROMWKB": "_sf_to_geometry", "TRY_TO_GEOMETRY": "_sf_try_to_geometry",
    "ST_MAKEPOINT": "_sf_st_makepoint", "ST_POINT": "_sf_st_makepoint", "ST_DISTANCE": "_sf_st_distance",
    "ST_AZIMUTH": "_sf_st_azimuth", "ST_HAUSDORFFDISTANCE": "_sf_st_hausdorff", "ST_LENGTH": "_sf_st_length",
    "ST_AREA": "_sf_st_area", "ST_PERIMETER": "_sf_st_perimeter", "ST_X": "_sf_st_x", "ST_Y": "_sf_st_y",
    "ST_INTERSECTS": "_sf_st_intersects", "ST_DISJOINT": "_sf_st_disjoint", "ST_CONTAINS": "_sf_st_contains",
    "ST_COVERS": "_sf_st_covers", "ST_WITHIN": "_sf_st_within", "ST_COVEREDBY": "_sf_st_coveredby", "ST_DWITHIN": "_sf_st_dwithin",
    "ST_ISVALID": "_sf_st_isvalid", "ST_ASWKT": "_sf_st_aswkt", "ST_ASTEXT": "_sf_st_aswkt", "ST_ASEWKT": "_sf_st_asewkt",
    "ST_ASGEOJSON": "_sf_st_asgeojson", "ST_GEOHASH": "_sf_st_geohash", "ST_NPOINTS": "_sf_st_npoints",
    "ST_NUMPOINTS": "_sf_st_npoints", "ST_DIMENSION": "_sf_st_dimension", "ST_SRID": "_sf_st_srid",
    "ST_CENTROID": "_sf_st_centroid", "ST_STARTPOINT": "_sf_st_startpoint", "ST_ENDPOINT": "_sf_st_endpoint",
    "ST_POINTN": "_sf_st_pointn", "ST_ENVELOPE": "_sf_st_envelope", "ST_MAKELINE": "_sf_st_makeline",
    "ST_MAKEPOLYGON": "_sf_st_makepolygon", "ST_POLYGON": "_sf_st_makepolygon", "ST_COLLECT": "_sf_st_collect",
    "HAVERSINE": "_sf_haversine",
}
