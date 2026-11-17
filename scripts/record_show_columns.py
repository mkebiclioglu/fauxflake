"""Record the columns real Snowflake returns for SHOW and DESCRIBE commands into src/fauxflake/show_columns.json.

    uv run python scripts/record_show_columns.py <connection name>

The emulator gives its own SHOW results these columns, in this order (see emulator._conform_show)."""
import json
import sys
from pathlib import Path

import snowflake.connector

SETUP = ["use schema analytics.ci", "create or replace table fx_shw (id int, name varchar)", "create or replace view fx_shw_v as select * from fx_shw"]
COMMANDS = {
    "show tables": "show tables like 'FX_SHW' in schema analytics.ci", "show terse tables": "show terse tables in schema analytics.ci",
    "show views": "show views in schema analytics.ci", "show terse views": "show terse views in schema analytics.ci",
    "show schemas": "show schemas in database analytics", "show terse schemas": "show terse schemas in database analytics",
    "show objects": "show objects in schema analytics.ci", "show terse objects": "show terse objects in schema analytics.ci",
    "show columns": "show columns in table fx_shw", "describe table": "describe table fx_shw", "describe view": "describe view fx_shw_v",
    "show databases": "show databases", "show terse databases": "show terse databases",
    "show primary keys": "show primary keys in table fx_shw", "show imported keys": "show imported keys in table fx_shw",
    "show unique keys": "show unique keys in table fx_shw", "show user functions": "show user functions", "show procedures": "show procedures",
    "show sequences": "show sequences", "show stages": "show stages", "show file formats": "show file formats",
    "show warehouses": "show warehouses", "show grants": "show grants on table fx_shw", "show roles": "show roles",
    "show variables": "show variables", "show dynamic tables": "show dynamic tables in schema analytics.ci",
    "show materialized views": "show materialized views", "show streams": "show streams", "show tasks": "show tasks",
    "show pipes": "show pipes", "show alerts": "show alerts", "show masking policies": "show masking policies",
    "show row access policies": "show row access policies", "show tags": "show tags", "show external tables": "show external tables",
    "show parameters": "show parameters like 'TIMEZONE'", "show functions": "show functions like 'ABS'",
    "show users": "show users", "show shares": "show shares", "show integrations": "show integrations",
    "show network policies": "show network policies", "show resource monitors": "show resource monitors",
}

conn = snowflake.connector.connect(connection_name=sys.argv[1])
for statement in SETUP:
    conn.cursor().execute(statement)
out = {}
for key, command in COMMANDS.items():
    cur = conn.cursor()
    try:
        cur.execute(command)
        out[key] = [d[0] for d in cur.description]
    except Exception as e:  # noqa: BLE001 - not visible to this role: left out, the emulator keeps its own columns
        print(f"{key}: {str(e).splitlines()[0][:100]}", file=sys.stderr)
for statement in ("drop view fx_shw_v", "drop table fx_shw"):
    conn.cursor().execute(statement)
conn.close()
path = Path(__file__).resolve().parents[1] / "src" / "fauxflake" / "show_columns.json"
path.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
print(f"{len(out)} commands recorded in {path}")
