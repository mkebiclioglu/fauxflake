"""Replay recorded Snowflake answers against the emulator."""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import yaml

# in a checkout the cases live at the top of the repository; an installed copy carries them inside the package
_REPO = Path(__file__).resolve().parents[2] / "conformance"
ROOT = _REPO if _REPO.exists() else Path(__file__).resolve().parent / "conformance_cases"


def cases(root: Path = ROOT) -> list[dict]:
    return yaml.safe_load((root / "cases.yml").read_text())


def golden(root: Path = ROOT) -> dict:
    return json.loads((root / "golden.json").read_text())


def normalise(v):
    """A value as both engines should agree on it. Scale matters (0.333333 != 0.3333333333);
    representation does not (1, 1.0 and Decimal('1.00') are the same)."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, Decimal)):
        return _num(Decimal(v))
    if isinstance(v, float):
        return _num(Decimal(f"{v:.13g}"))  # 13 significant digits: below double noise, above any scale rule
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
    if isinstance(v, (date, time)):
        return v.isoformat()
    if isinstance(v, (list, tuple, dict)):
        return json.loads(json.dumps(v, default=str))
    if isinstance(v, str) and v[:1] in "[{" :
        try:
            return json.loads(v)  # VARIANT, ARRAY and OBJECT come back as JSON text
        except ValueError:
            pass
    return str(v)


def _num(d: Decimal) -> str:
    text = format(d.normalize(), "f")
    return "0" if text in ("-0", "0") else text


def run(conn, sql: str) -> dict:
    cur = conn.cursor()
    try:
        rows = cur.execute(sql).fetchall()
    except Exception as e:  # noqa: BLE001
        return {"error": str(e).splitlines()[0][:120]}
    finally:
        cur.close()
    return {"rows": [[normalise(v) for v in row] for row in rows]}


def record(conn, root: Path = ROOT) -> dict:
    out = {c["id"]: run(conn, c["sql"]) for c in cases(root)}
    (root / "golden.json").write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    return out


def matches(expected: dict, actual: dict) -> bool:
    if "error" in expected or "error" in actual:
        return "error" in expected and "error" in actual  # both must fail; messages may differ
    return expected["rows"] == actual["rows"]
