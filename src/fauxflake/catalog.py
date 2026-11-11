"""What Snowflake itself provides: the columns of its system views and table functions.

`snowflake_views.json` was recorded from a real account's SNOWFLAKE database (ORGANIZATION_USAGE,
READER_ACCOUNT_USAGE, INFORMATION_SCHEMA and others). The table functions below are written out
by hand from the documentation; locally they return no rows, with the right columns.
"""

from __future__ import annotations

import collections
import json
from pathlib import Path

_VIEWS: dict[str, dict[str, str]] | None = None

TABLE_FUNCTIONS: dict[str, dict[str, str]] = {
    "EXTERNAL_TABLE_FILE_REGISTRATION_HISTORY": {
        "FILE_NAME": "VARCHAR", "REGISTERED_ON": "TIMESTAMPTZ", "FILE_SIZE": "BIGINT", "LAST_MODIFIED": "TIMESTAMPTZ",
        "ETAG": "VARCHAR", "MD5": "VARCHAR", "OPERATION_STATUS": "VARCHAR", "MESSAGE": "VARCHAR"},
    "EXTERNAL_TABLE_FILES": {
        "FILE_NAME": "VARCHAR", "REGISTERED_ON": "TIMESTAMPTZ", "FILE_SIZE": "BIGINT", "LAST_MODIFIED": "TIMESTAMPTZ",
        "ETAG": "VARCHAR", "MD5": "VARCHAR"},
    "TASK_HISTORY": {
        "QUERY_ID": "VARCHAR", "NAME": "VARCHAR", "DATABASE_NAME": "VARCHAR", "SCHEMA_NAME": "VARCHAR", "QUERY_TEXT": "VARCHAR",
        "CONDITION_TEXT": "VARCHAR", "STATE": "VARCHAR", "ERROR_CODE": "VARCHAR", "ERROR_MESSAGE": "VARCHAR",
        "SCHEDULED_TIME": "TIMESTAMPTZ", "QUERY_START_TIME": "TIMESTAMPTZ", "NEXT_SCHEDULED_TIME": "TIMESTAMPTZ",
        "COMPLETED_TIME": "TIMESTAMPTZ", "ROOT_TASK_ID": "VARCHAR", "GRAPH_VERSION": "BIGINT", "RUN_ID": "BIGINT",
        "RETURN_VALUE": "VARCHAR", "SCHEDULED_FROM": "VARCHAR", "ATTEMPT_NUMBER": "BIGINT"},
    "COPY_HISTORY": {
        "FILE_NAME": "VARCHAR", "STAGE_LOCATION": "VARCHAR", "LAST_LOAD_TIME": "TIMESTAMPTZ", "ROW_COUNT": "BIGINT",
        "ROW_PARSED": "BIGINT", "FILE_SIZE": "BIGINT", "FIRST_ERROR_MESSAGE": "VARCHAR", "FIRST_ERROR_LINE_NUMBER": "BIGINT",
        "FIRST_ERROR_CHARACTER_POS": "BIGINT", "FIRST_ERROR_COLUMN_NAME": "VARCHAR", "ERROR_COUNT": "BIGINT",
        "ERROR_LIMIT": "BIGINT", "STATUS": "VARCHAR", "TABLE_CATALOG_NAME": "VARCHAR", "TABLE_SCHEMA_NAME": "VARCHAR",
        "TABLE_NAME": "VARCHAR", "PIPE_CATALOG_NAME": "VARCHAR", "PIPE_SCHEMA_NAME": "VARCHAR", "PIPE_NAME": "VARCHAR",
        "PIPE_RECEIVED_TIME": "TIMESTAMPTZ"},
    "DYNAMIC_TABLE_REFRESH_HISTORY": {
        "NAME": "VARCHAR", "SCHEMA_NAME": "VARCHAR", "DATABASE_NAME": "VARCHAR", "STATE": "VARCHAR", "STATE_CODE": "VARCHAR",
        "STATE_MESSAGE": "VARCHAR", "QUERY_ID": "VARCHAR", "DATA_TIMESTAMP": "TIMESTAMPTZ", "REFRESH_START_TIME": "TIMESTAMPTZ",
        "REFRESH_END_TIME": "TIMESTAMPTZ", "COMPLETION_TARGET": "TIMESTAMPTZ", "QUALIFIED_NAME": "VARCHAR",
        "REFRESH_ACTION": "VARCHAR", "REFRESH_TRIGGER": "VARCHAR", "TARGET_LAG_SEC": "BIGINT", "STATISTICS": "JSON"},
    "ALERT_HISTORY": {
        "NAME": "VARCHAR", "DATABASE_NAME": "VARCHAR", "SCHEMA_NAME": "VARCHAR", "CONDITION": "VARCHAR",
        "CONDITION_QUERY_ID": "VARCHAR", "ACTION": "VARCHAR", "ACTION_QUERY_ID": "VARCHAR", "STATE": "VARCHAR",
        "SQL_ERROR_CODE": "BIGINT", "SQL_ERROR_MESSAGE": "VARCHAR", "SCHEDULED_TIME": "TIMESTAMPTZ", "COMPLETED_TIME": "TIMESTAMPTZ"},
    "TAG_REFERENCES": {
        "TAG_DATABASE": "VARCHAR", "TAG_SCHEMA": "VARCHAR", "TAG_NAME": "VARCHAR", "TAG_VALUE": "VARCHAR", "LEVEL": "VARCHAR",
        "OBJECT_DATABASE": "VARCHAR", "OBJECT_SCHEMA": "VARCHAR", "OBJECT_NAME": "VARCHAR", "DOMAIN": "VARCHAR",
        "COLUMN_NAME": "VARCHAR"},
    "POLICY_REFERENCES": {
        "POLICY_DB": "VARCHAR", "POLICY_SCHEMA": "VARCHAR", "POLICY_NAME": "VARCHAR", "POLICY_KIND": "VARCHAR",
        "REF_DATABASE_NAME": "VARCHAR", "REF_SCHEMA_NAME": "VARCHAR", "REF_ENTITY_NAME": "VARCHAR", "REF_ENTITY_DOMAIN": "VARCHAR",
        "REF_COLUMN_NAME": "VARCHAR", "REF_ARG_COLUMN_NAMES": "VARCHAR", "TAG_DATABASE": "VARCHAR", "TAG_SCHEMA": "VARCHAR",
        "TAG_NAME": "VARCHAR", "POLICY_STATUS": "VARCHAR"},
    "VALIDATE_PIPE_LOAD": {
        "ERROR": "VARCHAR", "FILE": "VARCHAR", "LINE": "BIGINT", "CHARACTER": "BIGINT", "BYTE_OFFSET": "BIGINT",
        "CATEGORY": "VARCHAR", "CODE": "BIGINT", "SQL_STATE": "VARCHAR", "COLUMN_NAME": "VARCHAR", "ROW_NUMBER": "BIGINT",
        "ROW_START_LINE": "BIGINT", "REJECTED_RECORD": "VARCHAR"},
    "TASK_DEPENDENTS": {
        "CREATED_ON": "TIMESTAMPTZ", "NAME": "VARCHAR", "DATABASE_NAME": "VARCHAR", "SCHEMA_NAME": "VARCHAR", "OWNER": "VARCHAR",
        "COMMENT": "VARCHAR", "WAREHOUSE": "VARCHAR", "SCHEDULE": "VARCHAR", "PREDECESSORS": "JSON", "STATE": "VARCHAR",
        "DEFINITION": "VARCHAR", "CONDITION": "VARCHAR"},
}
_ALIASES = {"QUERY_HISTORY_BY_SESSION": "QUERY_HISTORY", "QUERY_HISTORY_BY_USER": "QUERY_HISTORY",
            "QUERY_HISTORY_BY_WAREHOUSE": "QUERY_HISTORY", "LOGIN_HISTORY_BY_USER": "LOGIN_HISTORY",
            "SERVERLESS_TASK_HISTORY": "TASK_HISTORY", "COMPLETE_TASK_GRAPHS": "TASK_HISTORY",
            "CURRENT_TASK_GRAPHS": "TASK_HISTORY"}


def _views() -> dict[str, dict[str, str]]:
    global _VIEWS
    if _VIEWS is None:
        _VIEWS = json.loads((Path(__file__).parent / "snowflake_views.json").read_text())
    return _VIEWS


def _usual(column: str) -> str | None:
    """The type a column of this name has across the recorded SNOWFLAKE usage views, when they agree."""
    kinds = collections.Counter(cols[column] for key, cols in _views().items() if "_USAGE." in key and column in cols)
    if not kinds:
        return None
    kind, n = kinds.most_common(1)[0]
    return kind if n == sum(kinds.values()) else None


def view(name: str, columns: tuple[str, ...] = ()) -> dict[str, str | None]:
    """Columns and types of a view in the SNOWFLAKE database (ACCOUNT_USAGE and friends), or of an
    INFORMATION_SCHEMA view, where known. A view missing from the recording borrows from its
    namesake in another schema (ACCOUNT_USAGE.QUERY_HISTORY from READER_ACCOUNT_USAGE.QUERY_HISTORY)."""
    database, _, rest = name.partition(".")
    if database != "SNOWFLAKE" and ".INFORMATION_SCHEMA." not in f".{rest}.":
        return {}
    views = _views()
    if rest in views:
        return dict(views[rest])
    table = rest.split(".")[-1]
    borrowed = next((cols for key, cols in sorted(views.items()) if key.split(".")[-1] == table), {})
    if not borrowed and columns:
        # a view never recorded: its columns are named like those of its neighbours (START_TIME is
        # a TIMESTAMP_LTZ in every usage view), so each takes the type that name has elsewhere
        return {col: kind for col in columns if (kind := _usual(col)) and kind != "VARIANT"}
    # a namesake can differ in the odd type (the reader view holds some durations as VARIANT):
    # keep its column names, and leave those types to how the project uses the column
    return {col: (None if kind == "VARIANT" else kind) for col, kind in borrowed.items()}


def table_function(name: str) -> dict[str, str] | None:
    """Columns of a Snowflake system table function such as INFORMATION_SCHEMA.TASK_HISTORY()."""
    name = _ALIASES.get(name.upper(), name.upper())
    if name in TABLE_FUNCTIONS:
        return TABLE_FUNCTIONS[name]
    if not name.endswith(("_HISTORY", "_USAGE", "_FILES")):
        return None
    borrowed = next((cols for key, cols in sorted(_views().items()) if key.split(".")[-1] == name), None)
    if borrowed is None:
        return None
    return {col: {"VARIANT": "JSON", "OBJECT": "JSON", "ARRAY": "JSON", "BINARY": "BLOB"}.get(kind, kind) for col, kind in borrowed.items()}
