"""Queries over staged files: `SELECT $1, $2, METADATA$FILENAME FROM @stage/path (FILE_FORMAT => 'fmt', PATTERN => '...')`,
as Snowpark's `session.read.csv(...)` and projects that peek at a stage write them. Each such reference becomes a view
over DuckDB's `read_csv` of the stage's files, its columns named COLUMN1..N (what `$1`..`$N` are read as).

As measured on Snowflake (2026-11-26): the path is a prefix (`iris.csv` matches the `iris.csv.gz` that PUT wrote), the
stage is read recursively, PATTERN is a regular expression over the path within the stage, `$n` past a file's last
field is NULL, every field is text, METADATA$FILENAME is the path within the stage and METADATA$FILE_ROW_NUMBER counts
from 1 in each file."""

from __future__ import annotations

import itertools
import json
import os
import re

_REFERENCE = re.compile(
    r"""(?is)(?P<lead>\b(?:from|join)\s+)@(?P<stage>(?:"[^"]+"|[\w$]+)(?:\.(?:"[^"]+"|[\w$]+)){0,2})(?P<path>/[^\s()';,]*)?"""
    r"""(?:\s*\((?P<args>(?:\s*(?:file_format|pattern)\s*=>\s*(?:'(?:[^']|'')*'|[\w$."]+)\s*,?)+)\))?""")
_ARG = re.compile(r"""(?is)(file_format|pattern)\s*=>\s*('(?:[^']|'')*'|[\w$."]+)""")
_COUNTER = itertools.count(1)


def rewrite(cursor, command):  # noqa: ANN001, ANN201
    """The statement with each staged-file reference read through a view (the statement unchanged when it has none)."""
    if not isinstance(command, str) or "@" not in command or not _REFERENCE.search(command):
        return command
    if not re.match(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(select|with|insert|create)\b", command):
        return command
    if re.match(r"(?is)^(?:\s|--[^\n]*\n|/\*.*?\*/)*(copy|put|get|list|ls|remove|rm)\b", command):
        return command
    highest = max([int(n) for n in re.findall(r"\$(\d+)", command)] or [1])

    def replace(m: re.Match) -> str:
        view = _view(cursor, m, highest)
        return m.group("lead") + view if view else m.group(0)

    return _REFERENCE.sub(replace, command)


def _unquote(text: str) -> str:
    text = text.strip()
    return text[1:-1].replace("''", "'") if text.startswith("'") else text


def _view(cursor, m: re.Match, highest: int) -> str | None:  # noqa: ANN001, C901
    from fakesnow.transforms import stage

    duck, conn = cursor._duck_conn, cursor._conn
    database, schema, name = stage.parts_from_var(m.group("stage"), conn.database, conn.schema)
    row = duck.execute("select url from _fs_global._fs_information_schema._fs_stages where database_name = ? and schema_name = ? "
                       "and name = ?", [database, schema, name]).fetchone()
    if row is None or row[0]:
        return None  # an unknown stage (Snowflake's own error follows) or an external one: as it was
    root = stage.internal_dir(f"{database}.{schema}.{name}")
    args = {k.lower(): _unquote(v) for k, v in _ARG.findall(m.group("args") or "")}
    options: dict = {}
    if "file_format" in args:
        fdb, fschema, fname = stage.parts_from_var(args["file_format"], conn.database, conn.schema)
        found = duck.execute("select options from _fs_global._fs_information_schema._fs_file_formats where upper(database_name) = ? "
                             "and upper(schema_name) = ? and upper(name) = ?", [fdb.upper(), fschema.upper(), fname.upper()]).fetchone()
        if found is not None:
            options = {k.upper(): v for k, v in json.loads(found[0] or "{}").items()}
    prefix = (m.group("path") or "/").lstrip("/")
    files = []
    for folder, _dirs, names in os.walk(root):
        for file in sorted(names):
            relative = os.path.relpath(os.path.join(folder, file), root).replace(os.sep, "/")
            if not relative.startswith(prefix):
                continue
            if "pattern" in args and not re.fullmatch(args["pattern"], relative):
                continue
            files.append((os.path.join(folder, file), relative))
    delimiter = options.get("FIELD_DELIMITER") or ","
    quote = options.get("FIELD_OPTIONALLY_ENCLOSED_BY")
    skip = int(options.get("SKIP_HEADER") or 0)
    settings = [f"delim = '{_sql(delimiter)}'", "header = false", "all_varchar = true", f"skip = {skip}", "auto_detect = false",
                f"quote = '{_sql(quote)}'" if quote and quote.upper() != "NONE" else "quote = ''", "null_padding = true"]
    parts = []
    widths = {path: _width(path, delimiter, skip) for path, _ in files}
    highest = max([highest, *widths.values()])  # (every file as wide as the widest, for the union)
    for path, relative in sorted(files, key=lambda f: f[1]):
        width = widths[path]
        if width == 0:
            continue
        columns = ", ".join(f"'column{i}': 'VARCHAR'" for i in range(width))
        listed = ", ".join(f"column{i} as COLUMN{i + 1}" if i < width else f"null::varchar as COLUMN{i + 1}"
                           for i in range(max(width, highest)))
        parts.append(f"select {listed}, '{_sql(relative)}' as \"METADATA$FILENAME\", row_number() over () as \"METADATA$FILE_ROW_NUMBER\" "
                     f"from read_csv('{_sql(path)}', {', '.join(settings)}, columns = {{{columns}}})")
    if not parts:
        empty = ", ".join(f"null::varchar as COLUMN{i + 1}" for i in range(highest))
        parts.append(f"select {empty}, null::varchar as \"METADATA$FILENAME\", null::bigint as \"METADATA$FILE_ROW_NUMBER\" where false")
    view = f"_fx_staged_{next(_COUNTER)}"
    duck.execute(f"create or replace view _fs_global.main.{view} as " + " union all ".join(parts))
    return f"_fs_global.main.{view}"


def _width(path: str, delimiter: str, skip: int) -> int:
    """How many fields the file's widest line has (read with Python: DuckDB's sniffer guesses types and headers)."""
    import csv
    import gzip

    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", newline="") as handle:
        rows = list(itertools.islice(csv.reader(handle, delimiter=delimiter[0] if delimiter else ","), skip, skip + 1000))
    return max((len(r) for r in rows), default=0)


def _sql(text: str) -> str:
    return str(text).replace("'", "''")
