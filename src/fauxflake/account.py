"""The account around the databases: warehouses, resource monitors, network policies and parameters, as SHOW lists them.

`snowflake_show.json` was measured on a real account: the columns each SHOW command returns (names and types), and
every parameter Snowflake has with its default, description and type, per scope (account, session, warehouse...).
What exists comes from the spec's `account:` (or, in `fauxflake parity`, from what the Snowflake account shows) and
from the statements the project runs: CREATE / ALTER / DROP WAREHOUSE, RESOURCE MONITOR and NETWORK POLICY, and
ALTER ACCOUNT / WAREHOUSE ... SET a parameter. Kept in DuckDB, so a kept warehouse (`fauxflake query`) still has them.
"""

from __future__ import annotations

import json
import re
import zlib
from datetime import datetime, timezone
from functools import cache
from pathlib import Path

ACCOUNT = "_fs_global.main._fx_account"  # (kind, name, fields): one row per object; fields is its SHOW row as JSON
PARAMETERS = "_fs_global.main._fx_parameters"  # (scope, object, key, value): parameters set on an object
SETUP = [f"create table if not exists {ACCOUNT} (kind varchar, name varchar, fields json)",
         f"create table if not exists {PARAMETERS} (scope varchar, object varchar, key varchar, value varchar)"]

_LEAD = r"(?is)^(?:\s|--[^\n]*\n|//[^\n]*\n|/\*.*?\*/)*"
_ID = r'(?:"(?:[^"]|"")+"|[\w$]+)'
_NAME = _ID + r"(?:\." + _ID + r")*"


@cache
def catalog() -> dict:
    return json.loads((Path(__file__).parent / "snowflake_show.json").read_text())


def _bare(name: str) -> str:
    """An object's name as Snowflake stores it: upper case unless quoted."""
    part = name.split(".")[-1] if not name.startswith('"') else name
    return part[1:-1].replace('""', '"') if part.startswith('"') else part.upper()


def _now() -> datetime:
    from . import emulator

    return emulator.FROZEN_NOW or datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------- property lists

_VALUE = r"""(?:'(?:[^']|'')*'|\((?:[^()']|'(?:[^']|'')*')*\)|[^\s,=()]+)"""
_PAIR = re.compile(r"(?is)([\w$]+)\s*=\s*(" + _VALUE + ")")


def _value(text: str):  # noqa: ANN202
    """A property's value as written: a string, a number, TRUE / FALSE, a list (of strings or names) or a name."""
    text = text.strip()
    if text.startswith("'"):
        return text[1:-1].replace("''", "'")
    if text.startswith("("):
        inner = text[1:-1].strip()
        return [_value(v) for v in re.findall(r"'(?:[^']|'')*'|[^\s,]+", inner)] if inner else []
    if re.fullmatch(r"[+-]?\d+", text):
        return int(text)
    if re.fullmatch(r"[+-]?(?:\d+\.\d*|\.\d+)(?:e[+-]?\d+)?", text, re.I):
        return float(text)
    if text.upper() in ("TRUE", "FALSE"):
        return text.upper() == "TRUE"
    return _bare(text)


def properties(text: str) -> dict:
    """`A = 1 B = 'x', C = (a, b)` -> {"A": 1, "B": "x", "C": ["A", "B"]}."""
    return {k.upper(): _value(v) for k, v in _PAIR.findall(text)}


def _text(v) -> str:  # noqa: ANN001
    """A parameter's value as SHOW PARAMETERS shows it."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, list):
        return "[" + ", ".join(str(x) for x in v) + "]"
    return str(v)


# ---------------------------------------------------------------- warehouses

_SIZES = {"XSMALL": "X-Small", "SMALL": "Small", "MEDIUM": "Medium", "LARGE": "Large", "XLARGE": "X-Large",
          **{f"X{n}LARGE": f"{n}X-Large" for n in range(2, 7)}, "XXLARGE": "2X-Large", "XXXLARGE": "3X-Large"}


def _size(v) -> str:  # noqa: ANN001
    key = re.sub(r"[-_\s]", "", str(v).upper())
    key = re.sub(r"^(\d)XLARGE$", r"X\1LARGE", key)
    return _SIZES.get(key, str(v))


_WAREHOUSE_COLUMNS = {"WAREHOUSE_SIZE": ("size", _size), "WAREHOUSE_TYPE": ("type", lambda v: str(v).upper().replace("_", "-")
                                                                           if str(v).upper() != "STANDARD" else "STANDARD"),
                      "AUTO_SUSPEND": ("auto_suspend", lambda v: None if v in (0, "NULL") else v),
                      "AUTO_RESUME": ("auto_resume", _text), "MIN_CLUSTER_COUNT": ("min_cluster_count", int),
                      "MAX_CLUSTER_COUNT": ("max_cluster_count", int), "SCALING_POLICY": ("scaling_policy", lambda v: str(v).upper()),
                      "COMMENT": ("comment", str), "RESOURCE_MONITOR": ("resource_monitor", lambda v: str(v)),
                      "ENABLE_QUERY_ACCELERATION": ("enable_query_acceleration", _text),
                      "QUERY_ACCELERATION_MAX_SCALE_FACTOR": ("query_acceleration_max_scale_factor", int),
                      "RESOURCE_CONSTRAINT": ("resource_constraint", lambda v: str(v).upper()),
                      "GENERATION": ("generation", str)}


def _role(cursor) -> str:  # noqa: ANN001
    try:
        return cursor._duck_conn.execute("select role from _fs_global.main._fx_session").fetchone()[0]
    except Exception:  # noqa: BLE001
        return "SYSADMIN"


def warehouse_row(name: str, given: dict | None = None, owner: str = "SYSADMIN", now: datetime | None = None) -> dict:
    """A warehouse's SHOW row: Snowflake's defaults for a new one (an X-Small STANDARD that suspends after 600
    seconds), with what `given` sets (properties as CREATE WAREHOUSE takes them, or SHOW's own column names)."""
    now = now or _now()
    row = {"name": name, "state": "STARTED", "type": "STANDARD", "size": "X-Small", "min_cluster_count": 1, "max_cluster_count": 1,
           "started_clusters": 1, "running": 0, "queued": 0, "is_default": "N", "is_current": None, "auto_suspend": 600,
           "auto_resume": "true", "available": "", "provisioning": "", "quiescing": "", "other": "", "created_on": now,
           "resumed_on": now, "updated_on": now, "owner": owner, "comment": "", "enable_query_acceleration": "false",
           "query_acceleration_max_scale_factor": 8, "resource_monitor": "null", "actives": 1, "pendings": 0, "failed": 0,
           "suspended": 0, "uuid": str(zlib.crc32(name.encode()) % 10**9), "scaling_policy": "STANDARD", "owner_role_type": "ROLE",
           "resource_constraint": "STANDARD_GEN_1", "generation": "1"}
    given = dict(given or {})
    if given.pop("INITIALLY_SUSPENDED", False) is True:
        row.update(state="SUSPENDED", started_clusters=0, actives=0, suspended=1)
    _set_warehouse(row, given)
    return row


def _set_warehouse(row: dict, given: dict) -> None:
    for key, value in given.items():
        if key.upper() in _WAREHOUSE_COLUMNS:
            column, convert = _WAREHOUSE_COLUMNS[key.upper()]
            row[column] = convert(value)
        elif key.lower() in row or key.lower() in dict(catalog()["columns"]["warehouses"]):
            row[key.lower()] = value  # (SHOW's own column name, as the spec may write it)
    if row.get("state") == "SUSPENDED":
        row.update(started_clusters=0, actives=0, suspended=1)


# ---------------------------------------------------------------- resource monitors and network policies

_TRIGGER = re.compile(r"(?is)\bon\s+(\d+)\s+percent\s+do\s+(suspend_immediate|suspend|notify)\b")


def _triggers(text: str) -> dict:
    found = {"notify_at": [], "suspend_at": [], "suspend_immediately_at": []}
    for pct, action in _TRIGGER.findall(text):
        found[{"notify": "notify_at", "suspend": "suspend_at", "suspend_immediate": "suspend_immediately_at"}[action.lower()]].append(f"{pct}%")
    return {k: ",".join(v) if v else None for k, v in found.items()}


def _credits(v) -> str | None:  # noqa: ANN001
    return None if v is None else f"{float(v):.2f}"


def monitor_row(name: str, given: dict | None = None, text: str = "", owner: str = "SYSADMIN", now: datetime | None = None,
                old: dict | None = None) -> dict:
    now = now or _now()
    row = dict(old) if old else {"name": name, "credit_quota": None, "used_credits": "0.00", "remaining_credits": None, "level": None,
                                 "frequency": "MONTHLY", "start_time": now, "end_time": None, "notify_at": None, "suspend_at": None,
                                 "suspend_immediately_at": None, "created_on": now, "owner": owner, "comment": None, "notify_users": None}
    given = {k.lower(): v for k, v in (given or {}).items()}
    for key, value in given.items():
        if key == "credit_quota":
            row["credit_quota"] = _credits(value)
        elif key == "frequency":
            row["frequency"] = str(value).upper()
        elif key == "start_timestamp":
            row["start_time"] = now if str(value).upper() == "IMMEDIATELY" else value
        elif key == "end_timestamp":
            row["end_time"] = value
        elif key == "notify_users":
            row["notify_users"] = ", ".join(str(u) for u in value) if isinstance(value, list) else str(value)
        elif key in row:
            row[key] = value
    if re.search(r"(?i)\btriggers\b", text):
        row.update(_triggers(text))
    if row["credit_quota"] is not None:
        row["remaining_credits"] = _credits(float(row["credit_quota"]) - float(row["used_credits"] or 0))
    return row


def policy_row(name: str, given: dict | None = None, owner: str = "SYSADMIN", now: datetime | None = None, old: dict | None = None) -> dict:
    row = dict(old) if old else {"created_on": now or _now(), "name": name, "comment": "", "entries_in_allowed_ip_list": 0,
                                 "entries_in_blocked_ip_list": 0, "entries_in_allowed_network_rules": 0,
                                 "entries_in_blocked_network_rules": 0, "owner": owner}
    lists = {"allowed_ip_list": "entries_in_allowed_ip_list", "blocked_ip_list": "entries_in_blocked_ip_list",
             "allowed_network_rule_list": "entries_in_allowed_network_rules", "blocked_network_rule_list": "entries_in_blocked_network_rules"}
    for key, value in {k.lower(): v for k, v in (given or {}).items()}.items():
        if key in lists:
            row[lists[key]] = len(value) if isinstance(value, list) else int(bool(value))
            row["_" + key] = value if isinstance(value, list) else [value]
        elif key in row or key.startswith("entries_"):
            row[key] = value
    return row


# ---------------------------------------------------------------- what exists

def _rows(duck, kind: str) -> list[dict]:  # noqa: ANN001
    return [json.loads(f) for (f,) in duck.execute(f"select fields from {ACCOUNT} where kind = ? order by rowid", [kind]).fetchall()]


def _get(duck, kind: str, name: str) -> dict | None:  # noqa: ANN001
    found = duck.execute(f"select fields from {ACCOUNT} where kind = ? and name = ?", [kind, name]).fetchone()
    return json.loads(found[0]) if found else None


def _put(duck, kind: str, name: str, row: dict) -> None:  # noqa: ANN001
    duck.execute(f"delete from {ACCOUNT} where kind = ? and name = ?", [kind, name])
    duck.execute(f"insert into {ACCOUNT} values (?, ?, ?)", [kind, name, json.dumps(row, default=str)])


def _drop(duck, kind: str, name: str) -> bool:  # noqa: ANN001
    found = _get(duck, kind, name) is not None
    duck.execute(f"delete from {ACCOUNT} where kind = ? and name = ?", [kind, name])
    return found


def _mirrored(duck) -> bool:  # noqa: ANN001
    return duck.execute(f"select count(*) from {ACCOUNT} where kind = '_mirrored'").fetchone()[0] > 0


def set_parameter(duck, scope: str, obj: str, key: str, value) -> None:  # noqa: ANN001
    duck.execute(f"delete from {PARAMETERS} where scope = ? and object = ? and key = ?", [scope, obj, key])
    if value is not None:
        duck.execute(f"insert into {PARAMETERS} values (?, ?, ?, ?)", [scope, obj, key, _text(value)])


def load(emu, account: dict | None) -> None:  # noqa: ANN001
    """The spec's `account:`: {warehouses: [{name: X, warehouse_size: SMALL, ...}], resource_monitors: [...],
    network_policies: [...], users: [...], roles: [...], parameters: {KEY: value}} (any SHOW kind, with SHOW's own
    column names or the properties CREATE takes). Objects the spec no longer names go; the project's own stay."""
    duck = emu.duck
    for statement in SETUP:
        duck.execute(statement)
    duck.execute(f"delete from {ACCOUNT} where kind like '%' and fields->>'_from_spec' = 'true'")
    duck.execute(f"delete from {PARAMETERS} where object = '_spec'")
    duck.execute(f"delete from {ACCOUNT} where kind = '_roles'")
    if "roles" in {k.lower() for k in (account or {})}:  # the spec says which roles exist: one it does not name is refused
        duck.execute(f"insert into {ACCOUNT} values ('_roles', '', '{{\"_from_spec\": true}}')")
    for kind, given in (account or {}).items():
        kind = kind.lower().replace("_", " ")
        if kind == "parameters":
            for key, value in (given or {}).items():
                set_parameter(duck, "account", "", key.upper(), value)
            continue
        if kind not in catalog()["columns"]:
            raise ValueError(f"account: {kind!r} is not something SHOW lists (one of: {', '.join(sorted(catalog()['columns']))})")
        for item in given or []:
            item = {k.upper(): v for k, v in dict(item).items()}
            name = _bare(str(item.pop("NAME")))  # (as an unquoted name is stored: upper case)
            params = {k: item.pop(k) for k in list(item) if k in catalog()["parameters"] and k.lower() not in dict(catalog()["columns"][kind])}
            if kind == "warehouses":
                row = warehouse_row(name, item)
                for k, v in params.items():
                    set_parameter(duck, "warehouse", name, k, v)
            elif kind == "resource monitors":
                row = monitor_row(name, item)
                row.update({k.lower(): v for k, v in item.items() if k.lower() in row and k.lower() not in ("credit_quota",)})
            elif kind == "network policies":
                row = policy_row(name, item)
            elif kind == "roles":
                row = role_row(name, comment=str(item.get("COMMENT") or ""), now=datetime(1970, 1, 1))
            else:
                row = {"name": name, **{k.lower(): v for k, v in item.items()}}
            row["_from_spec"] = True
            _put(duck, kind, name, row)


def mirror(emu, conn, kinds: set[str]) -> None:  # noqa: ANN001
    """What the Snowflake account shows, put in the emulator's place (`fauxflake parity`: both engines read the same
    account): the SHOW rows of `kinds`, and the parameters set on the account and on its warehouses."""
    duck = emu.duck
    for statement in SETUP:
        duck.execute(statement)
    cur = conn.cursor()
    duck.execute(f"delete from {ACCOUNT}")
    duck.execute(f"delete from {PARAMETERS}")
    duck.execute(f"insert into {ACCOUNT} values ('_mirrored', '', '{{}}')")
    for kind in sorted(kinds | {"warehouses", "roles"}):  # (roles: a GRANT to one the account lacks is refused)
        try:
            cur.execute(f"show {kind}")
        except Exception:  # noqa: BLE001, S112
            continue  # (one the role may not list: none, as Snowflake would show it to that role)
        names = [d.name for d in cur.description]
        for values in cur.fetchall():
            row = dict(zip(names, values))
            _put(duck, kind, str(row.get("name") or ""), row)
    for scope, obj in [("account", "")] + [("warehouse", r["name"]) for r in _rows(duck, "warehouses")]:
        try:
            shown = cur.execute(f"show parameters in {scope} {_quote(obj)}" if obj else f"show parameters in {scope}").fetchall()
        except Exception:  # noqa: BLE001, S112
            continue
        for key, value, _default, level, *_ in shown:
            if level == scope.upper() or (level in ("ACCOUNT", "SYSTEM") and scope == "account"):
                set_parameter(duck, scope, obj, key, value)
                if level == "SYSTEM":
                    duck.execute(f"insert into {PARAMETERS} values ('system', '', ?, '')", [key])


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------- statements

_CREATE = re.compile(_LEAD + r"create\s+(?P<replace>or\s+replace\s+)?(?P<kind>warehouse|resource\s+monitor|network\s+policy)\s+"
                     r"(?P<exists>if\s+not\s+exists\s+)?(?P<name>" + _NAME + r")(?P<rest>.*?)[\s;]*$")
_ALTER = re.compile(_LEAD + r"alter\s+(?P<kind>warehouse|resource\s+monitor|network\s+policy)\s+(?P<exists>if\s+exists\s+)?"
                    r"(?P<name>" + _NAME + r")\s+(?P<rest>.*?)[\s;]*$")
_DROP = re.compile(_LEAD + r"drop\s+(?P<kind>warehouse|resource\s+monitor|network\s+policy)\s+(?P<exists>if\s+exists\s+)?"
                   r"(?P<name>" + _NAME + r")[\s;]*$")
_ALTER_ACCOUNT = re.compile(_LEAD + r"alter\s+account\s+(?:(?!set\b|unset\b)" + _NAME + r"\s+)?(?P<verb>set|unset)\s+(?P<rest>.*?)[\s;]*$")
_SHOW = re.compile(_LEAD + r"show\s+(?:terse\s+)?(?P<kind>[a-z][a-z ]*?)(?P<rest>(?:\s+(?:like|starts|in|limit|from|to|of|on)\b.*)?)[\s;]*$")
_SHOW_PARAMETERS = re.compile(_LEAD + r"show\s+parameters(?:\s+like\s+'(?P<like>(?:[^']|'')*)')?"
                              r"(?:\s+(?:in|for)\s+(?P<scope>account|session|warehouse|database|schema|table|task|user|function|procedure)"
                              r"(?:\s+(?P<name>" + _NAME + r"))?)?[\s;]*$")
_TRAILING_COMMENT = re.compile(r"(?s)(?:\s*;)?\s*(?:/\*(?:(?!\*/).)*\*/\s*;?\s*|--[^\n]*\s*)+$")
_ROLE = re.compile(_LEAD + r"(?P<verb>create|drop)\s+(?P<replace>or\s+replace\s+)?role\s+(?P<exists>if\s+(?:not\s+)?exists\s+)?"
                   r"(?P<name>" + _NAME + r")(?P<rest>.*?)[\s;]*$")
# the role a GRANT or REVOKE names: `... to role r`, `... to r` (a grantee is a role unless it says otherwise), `grant role r to ...`
_GRANTEE = re.compile(r"(?is)\b(?:to|from)\s+(?!share\b|application\b|database\s+role\b|user\b|application\s+role\b)(?:role\s+)?(" + _NAME + r")")
_GRANTED_ROLE = re.compile(r"(?is)^\s*(?:grant|revoke)\s+(?:grant\s+option\s+for\s+)?role\s+(" + _NAME + r")")
SYSTEM_ROLES = ("ACCOUNTADMIN", "ORGADMIN", "PUBLIC", "SECURITYADMIN", "SYSADMIN", "USERADMIN")
_KINDS = {"warehouse": "warehouses", "resource monitor": "resource monitors", "network policy": "network policies"}
_STORED = set(_KINDS.values())


def _error(message: str, errno: int = 2003, sqlstate: str = "02000"):  # noqa: ANN202
    from snowflake.connector.errors import ProgrammingError

    return ProgrammingError(msg=message, errno=errno, sqlstate=sqlstate)


def _status(cursor, original, text: str):  # noqa: ANN001, ANN202
    return original(cursor, "select '" + text.replace("'", "''") + "' as \"status\"")


def intercept(cursor, command: str, original):  # noqa: ANN001, ANN201
    """Handle a statement about the account. None for everything else."""
    head = re.match(_LEAD + r"(\w+)", command)
    if not head or head.group(1).lower() not in ("create", "alter", "drop", "show", "grant", "revoke"):
        return None
    duck = cursor._duck_conn
    command = _TRAILING_COMMENT.sub("", command)  # (dbt appends its query comment after the statement)
    if head.group(1).lower() in ("grant", "revoke"):
        return _grant(cursor, original, command)
    if m := _ROLE.match(command):
        return _role_statement(cursor, original, m)
    if m := _SHOW_PARAMETERS.match(command):
        return _show_parameters(cursor, original, m)
    if m := _CREATE.match(command):
        return _create(cursor, original, m)
    if m := _ALTER_ACCOUNT.match(command):
        return _alter_account(cursor, original, m)
    if m := _ALTER.match(command):
        return _alter(cursor, original, m)
    if m := _DROP.match(command):
        kind, name = _KINDS[re.sub(r"\s+", " ", m.group("kind").lower())], _bare(m.group("name"))
        if not _drop(duck, kind, name) and not m.group("exists"):
            raise _error(f"SQL compilation error:\n{m.group('kind').capitalize()} '{name}' does not exist or not authorized.")
        if kind == "warehouses":
            duck.execute(f"delete from {PARAMETERS} where scope = 'warehouse' and object = ?", [name])
        return _status(cursor, original, f"{name} successfully dropped.")
    if m := _SHOW.match(command):
        kind = re.sub(r"\s+", " ", m.group("kind").lower().strip())
        if kind in catalog()["columns"] and kind in SHOWN:
            return _show(cursor, original, kind, m.group("rest") or "")
    return None


def _create(cursor, original, m):  # noqa: ANN001, ANN202
    duck, kind, name = cursor._duck_conn, _KINDS[re.sub(r"\s+", " ", m.group("kind").lower())], _bare(m.group("name"))
    rest = m.group("rest")
    old = _get(duck, kind, name)
    if old is not None and not m.group("replace"):
        if m.group("exists"):
            return _status(cursor, original, f"{name} already exists, statement succeeded.")
        raise _error(f"SQL compilation error:\nObject '{name}' already exists.", errno=2002, sqlstate="42710")
    given = properties(re.sub(r"(?is)\btriggers\b.*$", "", rest))
    owner = _role(cursor)
    if kind == "warehouses":
        params = {k: given.pop(k) for k in list(given) if k in catalog()["scopes"]["warehouse"]}
        row = warehouse_row(name, given, owner)
        duck.execute(f"delete from {PARAMETERS} where scope = 'warehouse' and object = ?", [name])
        for k, v in params.items():
            set_parameter(duck, "warehouse", name, k, v)
        if row["resource_monitor"] != "null":
            _assign_monitor(duck, row["resource_monitor"], "WAREHOUSE")
    elif kind == "resource monitors":
        row = monitor_row(name, given, rest, owner)
    else:
        row = policy_row(name, given, owner)
    _put(duck, kind, name, row)
    noun = {"warehouses": "Warehouse", "resource monitors": "Resource monitor", "network policies": "Network policy"}[kind]
    return _status(cursor, original, f"{noun} {name} successfully created.")


def _assign_monitor(duck, monitor: str, level: str) -> None:  # noqa: ANN001
    row = _get(duck, "resource monitors", _bare(monitor))
    if row is None:
        raise _error(f"SQL compilation error:\nResource monitor '{_bare(monitor)}' does not exist or not authorized.")
    row["level"] = level
    _put(duck, "resource monitors", _bare(monitor), row)


def _alter(cursor, original, m):  # noqa: ANN001, ANN202
    duck, kind, name = cursor._duck_conn, _KINDS[re.sub(r"\s+", " ", m.group("kind").lower())], _bare(m.group("name"))
    rest = m.group("rest")
    row = _get(duck, kind, name)
    if row is None:
        if kind == "warehouses" and not _rows(duck, "warehouses") and not _mirrored(duck):
            row = warehouse_row(name, owner=_role(cursor))  # (the session's own: the one a fresh emulator has)
        elif m.group("exists"):
            return _status(cursor, original, "Statement executed successfully.")
        else:
            raise _error(f"SQL compilation error:\n{m.group('kind').capitalize()} '{name}' does not exist or not authorized.")
    verb = rest.split(None, 1)[0].lower() if rest.split() else ""
    body = rest.split(None, 1)[1] if len(rest.split(None, 1)) > 1 else ""
    if kind == "warehouses":
        row["updated_on"] = _now()
        if verb == "suspend":
            row.update(state="SUSPENDED", started_clusters=0, actives=0, suspended=1)
        elif verb == "resume":
            row.update(state="STARTED", started_clusters=row.get("min_cluster_count") or 1, actives=1, suspended=0, resumed_on=_now())
        elif verb == "rename" and (to := re.match(r"(?is)to\s+(" + _NAME + ")", body)):
            _drop(duck, kind, name)
            name = row["name"] = _bare(to.group(1))
        elif verb in ("set", "unset"):
            given = properties(body) if verb == "set" else {k.strip().upper(): None for k in body.split(",")}
            for key, value in given.items():
                if key in catalog()["scopes"]["warehouse"]:
                    set_parameter(duck, "warehouse", name, key, value)
                elif value is None:
                    if key in _WAREHOUSE_COLUMNS:
                        row[_WAREHOUSE_COLUMNS[key][0]] = warehouse_row(name)[_WAREHOUSE_COLUMNS[key][0]]
                else:
                    _set_warehouse(row, {key: value})
                    if key == "RESOURCE_MONITOR":
                        _assign_monitor(duck, str(value), "WAREHOUSE")
        elif verb != "abort":
            raise _error(f"SQL compilation error:\nsyntax error unexpected '{verb}'.", errno=1003, sqlstate="42000")
    elif kind == "resource monitors":
        row = monitor_row(name, properties(re.sub(r"(?is)\btriggers\b.*$", "", body)), body, old=row)
    else:
        if verb == "set":
            row = policy_row(name, properties(body), old=row)
        elif verb == "add" or verb == "remove":
            for key, value in properties(body).items():
                listed = list(row.get("_" + key.lower()) or [])
                values = value if isinstance(value, list) else [value]
                listed = listed + [v for v in values if v not in listed] if verb == "add" else [v for v in listed if v not in values]
                row = policy_row(name, {key: listed}, old=row)
    _put(duck, kind, name, row)
    return _status(cursor, original, "Statement executed successfully.")


def _alter_account(cursor, original, m):  # noqa: ANN001, ANN202
    duck = cursor._duck_conn
    if m.group("verb").lower() == "set":
        for key, value in properties(m.group("rest")).items():
            if key == "RESOURCE_MONITOR":
                for r in _rows(duck, "resource monitors"):
                    if r.get("level") == "ACCOUNT":
                        _put(duck, "resource monitors", r["name"], {**r, "level": None})
                _assign_monitor(duck, str(value), "ACCOUNT")
            else:
                set_parameter(duck, "account", "", key, value)
    else:
        for key in m.group("rest").split(","):
            set_parameter(duck, "account", "", key.strip().upper(), None)
    return _status(cursor, original, "Statement executed successfully.")


# ---------------------------------------------------------------- roles

def roles_known(duck) -> bool:  # noqa: ANN001
    """Whether the account's roles are described (the spec's `account: roles:`, or parity's mirror of Snowflake's): only
    then is a role nobody made refused. A fresh emulator takes a grant to any role, as a production account has them."""
    return _mirrored(duck) or duck.execute(f"select count(*) from {ACCOUNT} where kind = '_roles'").fetchone()[0] > 0


def _role_exists(cursor, name: str) -> bool:  # noqa: ANN001
    return name in SYSTEM_ROLES or name == _role(cursor) or _get(cursor._duck_conn, "roles", name) is not None


def _grant(cursor, original, command: str):  # noqa: ANN001, ANN202
    if roles_known(cursor._duck_conn):
        named = [m.group(1) for m in _GRANTEE.finditer(command)] + [m.group(1) for m in _GRANTED_ROLE.finditer(command)]
        for name in named:
            role = _bare(name)
            if not _role_exists(cursor, role):
                raise _error(f"SQL compilation error:\nRole '{role}' does not exist or not authorized.")
    return _status(cursor, original, "Statement executed successfully.")


def role_row(name: str, owner: str = "SYSADMIN", comment: str = "", now: datetime | None = None) -> dict:
    return {"created_on": now or _now(), "name": name, "is_default": "N", "is_current": "N", "is_inherited": "N",
            "assigned_to_users": 0, "granted_to_roles": 0, "granted_roles": 0, "owner": owner, "comment": comment,
            "is_from_organization_user_group": "N"}


def _role_statement(cursor, original, m):  # noqa: ANN001, ANN202
    duck, name = cursor._duck_conn, _bare(m.group("name"))
    if m.group("verb").lower() == "drop":
        if not _drop(duck, "roles", name) and not m.group("exists"):
            raise _error(f"SQL compilation error:\nRole '{name}' does not exist or not authorized.")
        return _status(cursor, original, f"{name} successfully dropped.")
    if _get(duck, "roles", name) is not None and not m.group("replace"):
        if m.group("exists"):
            return _status(cursor, original, f"{name} already exists, statement succeeded.")
        raise _error(f"SQL compilation error:\nObject '{name}' already exists.", errno=2002, sqlstate="42710")
    _put(duck, "roles", name, role_row(name, _role(cursor), str(properties(m.group("rest")).get("COMMENT") or "")))
    return _status(cursor, original, f"Role {name} successfully created.")


def granted_roles(project_dir, manifest: dict | None = None) -> set[str]:  # noqa: ANN001
    """The roles a project grants to, which its production account has: `to role X` in its SQL and hooks, and the
    grantees of `grants:` configs (as the manifest resolves them)."""
    found: set[str] = set()
    for path in Path(project_dir).rglob("*"):
        if path.suffix not in (".sql", ".yml", ".yaml") or any(p in (".fauxflake", "target", "logs") for p in path.parts):
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        found |= {_bare(n) for n in re.findall(r"(?is)\bgrant\s+[^;'\"]*?\bto\s+role\s+(" + _NAME + ")", text)}
    for node in ((manifest or {}).get("nodes") or {}).values():
        for grantees in ((node.get("config") or {}).get("grants") or {}).values():
            found |= {_bare(str(g)) for g in (grantees if isinstance(grantees, list) else [grantees])}
    return {r for r in found if r and "{" not in r and r not in SYSTEM_ROLES}


# ---------------------------------------------------------------- SHOW

# the SHOW commands answered from here: the account's objects (a fresh account has none but the session's warehouse)
SHOWN = {"warehouses", "roles", "users", "shares", "integrations", "api integrations", "security integrations",
         "storage integrations", "notification integrations", "external access integrations", "catalog integrations",
         "network policies", "network rules", "resource monitors", "secrets", "managed accounts", "replication databases",
         "failover groups", "password policies", "session policies", "packages policies", "external functions",
         "compute pools", "services", "image repositories", "streamlits", "notebooks", "models", "datasets",
         "git repositories", "cortex search services", "semantic views", "grants", "connections", "application packages",
         "applications", "dynamic tables", "materialized views", "external tables", "event tables", "iceberg tables",
         "future grants"}
_CAST = {"NUMBER": "number(38, 0)", "TEXT": "varchar", "TIMESTAMP_LTZ": "timestamp_ltz", "BOOLEAN": "boolean", "REAL": "double",
         "VARIANT": "variant", "DATE": "date"}


def _like(pattern: str) -> re.Pattern:
    """A LIKE pattern of SHOW (case-insensitive, `\\` escapes)."""
    out, i = "", 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            out, i = out + re.escape(pattern[i + 1]), i + 2
            continue
        out += ".*" if ch == "%" else "." if ch == "_" else re.escape(ch)
        i += 1
    return re.compile(out, re.I | re.S)


def _session_warehouse(cursor) -> str | None:  # noqa: ANN001
    """The session's warehouse: CURRENT_WAREHOUSE() is LOCAL here (the dbt profile fauxflake writes names it)."""
    from . import semantics

    return semantics.SESSION.get("CurrentWarehouse", "LOCAL")


def rows_of(cursor, kind: str) -> list[dict]:  # noqa: ANN001
    duck = cursor._duck_conn
    rows = _rows(duck, kind)
    if kind == "roles" and not _mirrored(duck):
        named = {r["name"] for r in rows}
        rows = [role_row(r, owner="", now=datetime(1970, 1, 1)) for r in SYSTEM_ROLES if r not in named] + rows
        for r in rows:
            r["is_current"] = "Y" if r["name"] == _role(cursor) else "N"
    if kind == "warehouses":
        current = _session_warehouse(cursor)
        if not _mirrored(duck) and current and not any(r["name"].upper() == current.upper() for r in rows):
            rows.insert(0, warehouse_row(current.upper(), owner=_role(cursor),
                                         now=datetime(1970, 1, 1)) | {"is_default": "Y", "_implicit": True})
        for r in rows:
            if r.get("is_current") is None:
                r["is_current"] = "Y" if current and r["name"].upper() == current.upper() else "N"
    return rows


def _show(cursor, original, kind: str, rest: str):  # noqa: ANN001, ANN202
    if kind in ("grants", "future grants"):  # (each form of SHOW GRANTS has its own columns)
        form = re.match(r"(?is)\s*(to\s+role|of\s+role|on|to\s+user|to\s+share|in)\b", rest)
        named = f"grants {re.sub(r'\\s+', ' ', form.group(1).lower())}" if form and kind == "grants" else kind
        kind = named if named in catalog()["columns"] else kind
    columns = catalog()["columns"][kind]
    rows = rows_of(cursor, kind)
    if (m := re.search(r"(?is)\blike\s+'((?:[^']|'')*)'", rest)) is not None:
        pattern = _like(m.group(1).replace("''", "'"))
        rows = [r for r in rows if pattern.fullmatch(str(r.get("name") or ""))]
    if (m := re.search(r"(?is)\bstarts\s+with\s+'((?:[^']|'')*)'", rest)) is not None:
        rows = [r for r in rows if str(r.get("name") or "").startswith(m.group(1).replace("''", "'"))]
    if (m := re.search(r"(?is)\blimit\s+(\d+)", rest)) is not None:
        rows = rows[: int(m.group(1))]
    return _select(cursor, original, columns, [[r.get(c) for c, _ in columns] for r in rows])


def _select(cursor, original, columns: list, rows: list[list]):  # noqa: ANN001, ANN202
    from . import scripting

    def lit(v, kind: str) -> str:  # noqa: ANN001
        if v is None:
            return f"null::{_CAST.get(kind, 'varchar')}"
        if kind == "TIMESTAMP_LTZ":
            return f"'{v}'::timestamp_ltz" if not isinstance(v, datetime) or v.tzinfo else f"'{v} +00:00'::timestamp_ltz"
        if kind == "NUMBER" and isinstance(v, str) and not re.fullmatch(r"-?\d+", v):
            return f"null::{_CAST[kind]}"
        return f"{scripting.literal(v)}::{_CAST.get(kind, 'varchar')}"

    names = ", ".join(f'"{c}"' for c, _ in columns)
    if not rows:
        return original(cursor, "select " + ", ".join(f'null::{_CAST.get(k, "varchar")} as "{c}"' for c, k in columns) + " where false")
    values = ", ".join("(" + ", ".join(lit(v, k) for v, (_, k) in zip(row, columns)) + ")" for row in rows)
    return original(cursor, f"select * from (values {values}) as t({names})")


def _show_parameters(cursor, original, m):  # noqa: ANN001, ANN202
    duck = cursor._duck_conn
    scope = (m.group("scope") or "session").lower()
    known = catalog()["parameters"]
    keys = catalog()["scopes"].get(scope) or catalog()["scopes"]["session"]
    obj = _bare(m.group("name")) if m.group("name") else ""
    if scope == "warehouse" and obj and _get(duck, "warehouses", obj) is None and not any(
            r["name"] == obj for r in rows_of(cursor, "warehouses")):
        raise _error(f"SQL compilation error:\nWarehouse '{obj}' does not exist or not authorized.")
    set_on = {(s, o, k): v for s, o, k, v in duck.execute(f"select scope, object, key, value from {PARAMETERS}").fetchall()}
    system = {k for (s, _o, k) in set_on if s == "system"}
    session = _session_values(cursor) if scope == "session" else {}
    pattern = _like(m.group("like").replace("''", "'")) if m.group("like") else None
    rows = []
    for key in keys:
        if pattern is not None and not pattern.fullmatch(key):
            continue
        default, description, kind = known[key]
        value, level = default, ""
        if ("account", "", key) in set_on:
            value, level = set_on[("account", "", key)], "SYSTEM" if key in system else "ACCOUNT"
        if scope != "account" and obj and (scope, obj, key) in set_on:
            value, level = set_on[(scope, obj, key)], scope.upper()
        if key in session:
            value, level = session[key], "SESSION"
        rows.append([key, value, default, level, description, kind])
    columns = [["key", "TEXT"], ["value", "TEXT"], ["default", "TEXT"], ["level", "TEXT"], ["description", "TEXT"], ["type", "TEXT"]]
    return _select(cursor, original, columns, rows)


def _session_values(cursor) -> dict:  # noqa: ANN001
    """The parameters this session has set (the emulator's own clock runs in UTC, as `fauxflake parity` has Snowflake's)."""
    values = {"TIMEZONE": "UTC"}
    tag = getattr(cursor._conn, "_fx_query_tag", "")
    if tag:
        values["QUERY_TAG"] = tag
    return values


def shown_by(project_dir) -> set[str]:  # noqa: ANN001
    """The account SHOW commands a project's SQL runs (its models, macros and packages): what parity mirrors."""
    kinds = sorted(SHOWN, key=len, reverse=True)
    found = re.compile(r"(?i)\bshow\s+(?:terse\s+)?(" + "|".join(k.replace(" ", r"\s+") for k in kinds) + r")\b")
    seen: set[str] = set()
    for path in Path(project_dir).rglob("*.sql"):
        if any(part in (".fauxflake", "target", "logs") for part in path.parts):
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        seen |= {re.sub(r"\s+", " ", k.lower()) for k in found.findall(text)}
    return {k for k in seen if k in catalog()["columns"]}
