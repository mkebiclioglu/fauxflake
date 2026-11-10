"""Where Snowflake and DuckDB disagree on the *answer*, not the syntax.

The emulator transpiles SQL faithfully enough to run, but several functions then
return different values or types. Each fix here exists because a case in
`conformance/` failed against an answer recorded from real Snowflake.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

_NS = "_fs_global.main."
_SCALE = r"coalesce(try_cast(nullif(regexp_extract(typeof(a), ',(\d+)\)', 1), '') as integer), 0)"

MACROS = [
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
    # TYPEOF looks inside a VARIANT. DuckDB only says JSON.
    f"""create or replace macro {_NS}_sf_typeof(v) as case when typeof(v) <> 'JSON' then typeof(v) else
          case json_type(v::varchar)
            when 'UBIGINT' then 'INTEGER' when 'BIGINT' then 'INTEGER' when 'DOUBLE' then 'DECIMAL'
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
    # Casting keeps the left operand's type: DATE + INTERVAL '1 month' is still a DATE.
    f"""create or replace macro {_NS}_sf_like
          (x date, r timestamp) as r::date,
          (x, r) as r""",
    # Text of a value: floats print the way C's %g does (100, 1e+20, 0.1), timestamps always
    # show milliseconds, and a VARIANT string loses its JSON quotes.
    f"""create or replace macro {_NS}_sf_text(x) as case
          when typeof(x) in ('DOUBLE', 'FLOAT') then format('{{:.15g}}', x::double)
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
          (x json) as json_array_length(x),
          (x) as len(x)""",
    f"create or replace macro {_NS}_sf_current_account_name() as 'FAUXFLAKE'",
    f"create or replace macro {_NS}_sf_try_base64(x) as try(from_base64(x)::varchar)",
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
          else left(regexp_replace(upper(s), '[^A-Z]', '', 'g'), 1) || left(replace(
            case when left(regexp_replace(upper(s), '[^A-Z]', '', 'g'), 1) in ('H', 'W')
                 then {_NS}_sf_soundex_squeeze({_NS}_sf_soundex_codes(s))
                 else substr({_NS}_sf_soundex_squeeze({_NS}_sf_soundex_codes(s)), 2) end, '0', '') || '000', 3) end""",
    # PARSE_URL gives an object; the path has no leading slash and missing parts are null.
    f"""create or replace macro {_NS}_sf_url_query(u) as regexp_extract(u, '^[^?#]*\\?([^#]*)', 1)""",
    f"""create or replace macro {_NS}_sf_parse_url(u) as case when u is null then null else json_object(
          'fragment', nullif(regexp_extract(u, '#(.*)$', 1), ''),
          'host', nullif(regexp_extract(u, '^[A-Za-z][A-Za-z0-9+.-]*://(?:[^/@?#]*@)?([^/:?#]*)', 1), ''),
          'parameters', case when {_NS}_sf_url_query(u) = '' then null else to_json(map_from_entries(list_transform(
              string_split({_NS}_sf_url_query(u), '&'),
              lambda p: struct_pack(k := split_part(p, '=', 1), v := split_part(p, '=', 2))))) end,
          'path', nullif(regexp_extract(u, '^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*/([^?#]*)', 1), ''),
          'port', nullif(regexp_extract(u, '^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*:([0-9]+)', 1), ''),
          'query', nullif({_NS}_sf_url_query(u), ''),
          'scheme', nullif(regexp_extract(u, '^([A-Za-z][A-Za-z0-9+.-]*)://', 1), '')) end""",
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
          (ip, kind, permissive) as case when ip is null then null else json_object(
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
    f"""create or replace macro {_NS}_sf_try_timestamp(x) as coalesce(
          try_cast(x::varchar as timestamp),
          try_strptime(x::varchar, ['%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y', '%d-%b-%Y %H:%M:%S', '%d-%b-%Y']),
          timezone('UTC', {_NS}_sf_epoch(x)))""",
    f"""create or replace macro {_NS}_sf_try_date(x) as coalesce(try_cast(x as date), {_NS}_sf_try_timestamp(x)::date)""",
    f"""create or replace macro {_NS}_sf_to_date(x) as coalesce({_NS}_sf_try_date(x), cast(x::varchar as date))""",
    f"""create or replace macro {_NS}_sf_try_time(x) as coalesce(
          case when regexp_matches(x::varchar, '^\s*[0-9]{{1,2}}:[0-9]{{2}}') then try_cast(x::varchar as time) end,
          timezone('UTC', {_NS}_sf_epoch(x))::time)""",
    # Object keys come back sorted: Snowflake stores objects ordered by key.
    f"create or replace macro {_NS}_sf_object_keys(o) as to_json(list_sort(json_keys(o::json)))",
    f"create or replace macro {_NS}_sf_hash_agg(x) as bit_xor(hash(x))",
    # FLATTEN(..., RECURSIVE => TRUE) walks every level.
    f"""create or replace macro {_NS}_sf_flatten_recursive(input) as table
          select hash(to_json(input))::ubigint as SEQ,
                 case when fullkey like '%]' then null else "key" end as KEY,
                 regexp_replace(fullkey, '^\\$\\.?', '') as PATH,
                 case when fullkey like '%]' then "key"::bigint end as INDEX,
                 "value"::json as VALUE,
                 json_extract(to_json(input), "path") as THIS
          from json_tree(to_json(input)::varchar) where parent is not null order by id""",
    *[f"create or replace macro {_NS}_sf_is_{name}(v) as json_type(v::varchar) = 'VARCHAR' and try_cast((v::json ->> '$') as {kind}) is not null"
      for name, kind in {"date": "date", "time": "time", "timestamp_ntz": "timestamp", "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp"}.items()],
    *[f"create or replace macro {_NS}_sf_as_{name}(v) as try_cast((v::json ->> '$') as {kind})"
      for name, kind in {"date": "date", "time": "time", "timestamp_ntz": "timestamp", "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp"}.items()],
    *[f"create or replace macro {_NS}_sf_as_{name}(v) as case when json_type(v::varchar) in ('UBIGINT', 'BIGINT', 'DOUBLE') then v::double end"
      for name in ("double", "real", "number", "decimal")],
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
          array_to_string(list_transform({_NS}_sf_list(a), lambda e: coalesce(e ->> '$', '')), s)""",
    f"create or replace macro {_NS}_sf_arrays_overlap(a, b) as list_has_any({_NS}_sf_list(a), {_NS}_sf_list(b))",
    f"""create or replace macro {_NS}_sf_array_slice(a, f, t) as to_json(list_slice({_NS}_sf_list(a),
          case when f >= 0 then f + 1 else f end, case when t < 0 then t - 1 else t end))""",
    f"create or replace macro {_NS}_sf_array_cat(a, b) as to_json(list_concat({_NS}_sf_list(a), {_NS}_sf_list(b)))",
    f"create or replace macro {_NS}_sf_array_append(a, v) as to_json(list_append({_NS}_sf_list(a), to_json(v)))",
    f"create or replace macro {_NS}_sf_array_prepend(a, v) as to_json(list_prepend(to_json(v), {_NS}_sf_list(a)))",
    f"create or replace macro {_NS}_sf_array_unique(a) as to_json({_NS}_sf_array_distinct({_NS}_sf_list(a)))",
    f"""create or replace macro {_NS}_sf_array_sort
          (a) as to_json({_NS}_sf_sorted(a)),
          (a, up) as to_json(case when up then {_NS}_sf_sorted(a) else list_reverse({_NS}_sf_sorted(a)) end)""",
    f"create or replace macro {_NS}_sf_array_max(a) as list_last(list_filter({_NS}_sf_sorted(a), lambda e: json_type(e) <> 'NULL'))",
    f"create or replace macro {_NS}_sf_array_min(a) as list_first(list_filter({_NS}_sf_sorted(a), lambda e: json_type(e) <> 'NULL'))",
    f"""create or replace macro {_NS}_sf_array_compact(a) as
          to_json(list_filter({_NS}_sf_list(a), lambda e: e is not null and json_type(e) <> 'NULL'))""",
    *[f"""create or replace macro {_NS}_sf_array_{name}(a, b) as to_json(list_transform(list_filter(
          list_zip({_NS}_sf_list(a), range(1, len({_NS}_sf_list(a)) + 1)),
          lambda p: len(list_filter({_NS}_sf_list(a)[1:p[2]], lambda e: e = p[1])) {op} len(list_filter({_NS}_sf_list(b), lambda e: e = p[1]))),
          lambda p: p[1]))""" for name, op in (("intersection", "<="), ("except", ">"))],
    f"create or replace macro {_NS}_sf_array_remove(a, v) as to_json(list_filter({_NS}_sf_list(a), lambda e: e <> to_json(v)))",
    f"""create or replace macro {_NS}_sf_array_insert(a, p, v) as
          to_json(list_concat({_NS}_sf_list(a)[1:p], [to_json(v)], {_NS}_sf_list(a)[p + 1:]))""",
    f"create or replace macro {_NS}_sf_array_remove_at(a, p) as to_json(list_concat({_NS}_sf_list(a)[1:p], {_NS}_sf_list(a)[p + 2:]))",
    f"create or replace macro {_NS}_sf_array_flatten(a) as to_json(flatten(list_transform({_NS}_sf_list(a), lambda e: e::json[])))",
    f"create or replace macro {_NS}_sf_array_reverse(a) as to_json(list_reverse({_NS}_sf_list(a)))",
    f"""create or replace macro {_NS}_sf_array_range
          (f, t) as to_json(range(f, t)),
          (f, t, s) as to_json(range(f, t, s))""",
    # ENCRYPT_RAW has no local equivalent: a stable, opaque stand-in with the same shape.
    f"create or replace macro {_NS}_sf_encrypt_raw " + ", ".join(
        f"({args}) as json_object('ciphertext', upper(sha256(v::varchar || k::varchar)), "
        "'iv', upper(md5(k::varchar)), 'tag', upper(md5(v::varchar)))"
        for args in ("v, k", "v, k, iv", "v, k, iv, aad", "v, k, iv, aad, mode")),
    # UPPER('ß') is 'SS'.
    f"create or replace macro {_NS}_sf_upper(s) as replace(upper(s), 'ẞ', 'SS')",
]

_RENAMED = {  # Snowflake function -> macro, same arguments
    "width_bucket": "_sf_width_bucket",
    "jarowinkler_similarity": "_sf_jarowinkler", "object_insert": "_sf_object_insert",
    "object_delete": "_sf_object_delete", "object_pick": "_sf_object_pick",
    "current_account_name": "_sf_current_account_name", "try_base64_decode_string": "_sf_try_base64",
    "soundex": "_sf_soundex", "parse_url": "_sf_parse_url", "parse_ip": "_sf_parse_ip",
    "get_ignore_case": "_sf_get_ignore_case", "object_keys": "_sf_object_keys", "hash_agg": "_sf_hash_agg",
    "try_hex_decode_string": "_sf_try_hex", "encrypt_raw": "_sf_encrypt_raw",
    **{f"is_{k}": f"_sf_is_{k}" for k in ("date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz")},
    **{f"as_{k}": f"_sf_as_{k}" for k in ("date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "double", "real",
                                          "number", "decimal", "boolean", "object", "array")}, "upper": "_sf_upper", "ucase": "_sf_upper",
    "as_integer": "_sf_as_integer", "as_varchar": "_sf_as_varchar", "as_char": "_sf_as_varchar",
    **{f"is_{k}": f"_sf_is_{k}" for k in ("object", "array", "integer", "varchar", "char", "boolean", "decimal", "double", "real")},
}
_DATE_TOKENS = re.compile(r"YYYY|YY|MM|DD|HH|MI|SS|MON|DY|AM|PM", re.IGNORECASE)
_HASHES = (exp.MD5, exp.SHA, exp.SHA2, exp.MD5Digest)
UDFS: dict[str, str] = {}  # user-defined functions from the spec: name -> macro name
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


_NUMBER_FORMAT = re.compile(r"^(FM)?(\$)?([09,]+)(?:\.([09]+))?$", re.IGNORECASE)


def _number_format(value: exp.Expression, model: str) -> exp.Expression | None:
    """TO_CHAR(number, '9,999.00') as a DuckDB format() call, for the common format models."""
    if re.fullmatch(r"X+", model.strip()):  # hexadecimal
        return sqlglot.parse_one(f"format('{{:X}}', ({value.sql(dialect='duckdb')})::bigint)", read="duckdb")
    m = _NUMBER_FORMAT.match(model.strip())
    if not m:
        return None
    fill, dollar, whole, frac = m.groups()
    decimals = len(frac or "")
    digits = whole.replace(",", "")
    spec = ("," if "," in whole else "") + f".{decimals}f"
    if "0" in digits and "," not in whole:  # zero padded to the width of the model
        spec = f"0{len(digits) + (decimals + 1 if decimals else 0)}.{decimals}f"
    v = f"({value.sql(dialect='duckdb')})::double"
    text = f"format('{{:{spec}}}', abs({v}))"
    if dollar:
        text = f"concat('$', {text})"
    text = f"(case when {v} < 0 then '-' else '' end || {text})"
    if not fill:  # Snowflake right-aligns in the model's width, plus one position for the sign
        text = f"lpad({text}, {len(model.strip()) + 1}, ' ')"
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


def _text(node: exp.Expression) -> exp.Expression:
    """The value as Snowflake prints it. A division carries its scale into the text."""
    inner = node.unnest()
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


def _collation(spec: str) -> str | None:
    """A Snowflake collation spec ('en-ci', 'en-ci-ai') as DuckDB's NOCASE / NOACCENT."""
    parts = spec.lower().replace("_", "-").split("-")
    names = [name for flag, name in (("ci", "NOCASE"), ("ai", "NOACCENT")) if flag in parts]
    return ".".join(names) or None


def _fix(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Collate) and isinstance(node.expression, (exp.Literal, exp.Var, exp.Identifier)):
        name = _collation(node.expression.name)
        if name is None:
            return node.this
        node.set("expression", exp.var(name))
        return node
    if isinstance(node, exp.CollateColumnConstraint) and isinstance(node.this, (exp.Literal, exp.Var, exp.Identifier)):
        node.set("this", exp.var(_collation(node.this.name) or "BINARY"))
        return node
    if isinstance(node, exp.Interval) and isinstance(node.this, exp.Literal) and "," in node.this.name:
        node.this.set("this", node.this.name.replace(",", " "))  # INTERVAL '1 day, 2 hours'
    if isinstance(node, (exp.Mul, exp.Add, exp.Sub)) and all(
            isinstance(side, exp.Literal) and side.is_int for side in (node.this, node.expression)):
        node.set("this", exp.cast(node.this, "bigint"))  # 86400 * 30000 does not overflow a NUMBER
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast):
        to, inner = node.to, node.this
        if to.is_type("timestamp", "timestampntz") and isinstance(inner, exp.Cast) and inner.to.is_type("timestamptz") \
                and isinstance(inner.this, exp.Literal) and inner.this.is_string:
            # a TIMESTAMP_TZ keeps its wall-clock time when the zone is dropped
            return exp.cast(exp.Literal.string(_OFFSET.sub("", inner.this.name)), "timestamp")
        if to.is_type("timestamp", "timestampntz", "timestampltz", "timestamptz") and to.expressions \
                and to.expressions[0].name == "0":
            return exp.Anonymous(this="date_trunc", expressions=[exp.Literal.string("second"), node])
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
    if isinstance(node, exp.ParseJSON) and not isinstance(node.this, (exp.Literal, exp.Null)):
        text = exp.cast(node.this, "varchar")
        node.set("this", exp.Nullif(this=text, expression=exp.Literal.string("")))  # PARSE_JSON('') is NULL
    if isinstance(node, exp.ToArray):
        return _fn("_sf_to_array", node.this)
    if isinstance(node, exp.JSONKeys):
        return _fn("_sf_object_keys", node.this)
    if isinstance(node, exp.GetExtract):
        # GET(v, key) is v[key]; as a bracket it gets the same handling as every other access
        bracket = sqlglot.parse_one("x[0]", read="snowflake")
        bracket.set("this", node.this)
        bracket.set("expressions", [node.expression])
        return bracket
    if isinstance(node, exp.Literal) and not node.is_string and "." in node.name and "e" not in node.name.lower():
        # 1.50 is NUMBER(2,1) in Snowflake: trailing zeros are not part of a literal's scale
        trimmed = node.name.rstrip("0").rstrip(".")
        return exp.Literal.number(trimmed or "0") if trimmed != node.name else node
    if isinstance(node, _ARITHMETIC):
        node.set("this", _operand(node.this))
        node.set("expression", _operand(node.expression))
    if isinstance(node, exp.DPipe):
        for side in ("this", "expression"):
            part = node.args[side]
            if isinstance(part, exp.JSONExtract):
                node.set(side, exp.cast(part, "varchar"))  # v:s || 'y'
            elif not (isinstance(part, exp.Literal) and part.is_string) and not isinstance(part, exp.DPipe):
                node.set(side, _text(part))
    if isinstance(node, _COMPARISONS) or isinstance(node, exp.In):
        other = node.expression if isinstance(node, _COMPARISONS) else (node.expressions[0] if node.expressions else None)
        if isinstance(node.this, _VARIANT_ACCESS) and isinstance(other, exp.Literal) and other.is_string:
            node.set("this", exp.cast(node.this, "varchar"))  # v:s = 'x' compares the value, not JSON text
    if isinstance(node, (exp.Like, exp.ILike)) and isinstance(node.this, _VARIANT_ACCESS):
        node.set("this", exp.cast(node.this, "varchar"))
    if isinstance(node, exp.TryCast) and node.to.is_type("timestamp", "timestampntz") and node.args.get("requires_string"):
        return _fn("_sf_try_timestamp", node.this)
    if isinstance(node, exp.UnixToTime) and isinstance(node.this, exp.Literal) and node.this.is_string \
            and _NUMERIC_TEXT.match(node.this.name):
        node.set("this", exp.Literal.number(node.this.name.strip()))  # TO_TIMESTAMP('1735689600') is an epoch
    if isinstance(node, exp.Anonymous) and node.name.upper() == "TO_DATE" and len(node.expressions) == 1 \
            and isinstance(node.expressions[0], exp.Literal) and re.fullmatch(r"\d{8,}", node.expressions[0].name):
        return exp.cast(exp.UnixToTime(this=exp.Literal.number(node.expressions[0].name)), "date")
    if isinstance(node, exp.Func) and not isinstance(node, exp.Anonymous):
        renamed = next((_RENAMED[n.lower()] for n in type(node).sql_names() if n.lower() in _RENAMED), None)
        if renamed:
            return _fn(renamed, *_func_args(node))
    if isinstance(node, (exp.TimeToStr, exp.StrToTime, exp.StrToDate)) and isinstance(node.args.get("format"), exp.Literal):
        fmt = node.args["format"]
        fmt.set("this", fmt.name.replace("Mon", "%b").replace("Dy", "%a"))
    if isinstance(node, exp.Cast) and not isinstance(node, exp.TryCast) and node.to.is_type("varchar", "text") \
            and not node.to.expressions and not isinstance(node.this, (exp.JSONExtract, exp.Bracket)) \
            and not (isinstance(node.this, exp.Literal) and node.this.is_string):
        return _text(node.this)
    if isinstance(node, exp.Add) and isinstance(node.expression, exp.Interval) \
            and (node.expression.unit and node.expression.unit.name.upper() in _DAY_UNITS):
        return _fn("_sf_like", node.this.copy(), node)
    if isinstance(node, exp.DateAdd) and node.unit and node.unit.name.upper() == "QUARTER":
        node.set("unit", exp.var("MONTH"))
        node.set("expression", exp.Mul(this=exp.paren(node.expression), expression=exp.Literal.number(3)))
    if isinstance(node, exp.MonthsBetween):
        return exp.func("round", node, exp.Literal.number(6))
    if isinstance(node, (exp.Variance, exp.VariancePop)) and not isinstance(node.parent, exp.Window):
        return _fn("_sf_var", node, exp.Min(this=node.this.copy()))
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
        return _fn("_sf_try_timestamp", node.expressions[0]) if node.name.lower().startswith("try_") \
            else exp.cast(node.expressions[0], "timestamp")
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
        if isinstance(inner, exp.Avg):
            return _fn("_sf_div3", _over(node, exp.Sum(this=inner.this.copy())), _over(node, exp.Count(this=inner.this.copy())))
        if isinstance(inner, exp.Anonymous) and inner.expressions:
            name, arg = inner.name.lower(), inner.expressions[0]
            if name == "ratio_to_report":
                return _fn("_sf_div", arg.copy(), _over(node, exp.Sum(this=arg.copy())))
            if name == "conditional_true_event":
                flag = exp.Case(ifs=[exp.If(this=arg.copy(), true=exp.Literal.number(1))], default=exp.Literal.number(0))
                return _over(node, exp.Sum(this=flag))
    return node


def _self_referencing(cte: exp.CTE) -> bool:
    return any(not t.db and t.name.lower() == cte.alias.lower() for t in cte.this.find_all(exp.Table))


def layer_windows(tree: exp.Expression) -> exp.Expression:
    """`lag(x) over (...) as prev, sum(prev) over (...)`: Snowflake computes the first window, then
    the second. DuckDB will not nest them, so compute the aliased windows in a subquery first."""
    for _ in range(6):
        changed = False
        for select in list(tree.find_all(exp.Select)):
            source = select.args.get("from_")
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
            inner = exp.select("*", *[exp.alias_(windowed[n].this.copy(), hidden[n]) for n in sorted(nested)])
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
            for proj in select.expressions:
                for col in list(proj.find_all(exp.Column)):
                    owner = defined.get(col.name.upper())
                    if owner is None or owner is proj or col.table or col.find_ancestor(exp.Select) is not select:
                        continue
                    col.replace(exp.paren(owner.this.copy()))
                    changed = True
        if not changed:
            break
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


def finish(transformed: exp.Expression) -> exp.Expression:
    """Fix-ups on the DuckDB side of the translation."""
    for ident in transformed.find_all(exp.Identifier):
        # `end`, `overlaps`... are ordinary names in Snowflake and reserved words in DuckDB
        if not ident.args.get("quoted") and ident.name.upper() in _RESERVED \
                and not (isinstance(ident.parent, exp.Column) and ident.name.upper() in _BARE):
            ident.set("quoted", True)
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
    return transformed


_BARE = {"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "LOCALTIME", "LOCALTIMESTAMP",
         "TRUE", "FALSE", "NULL", "DEFAULT", "SESSION_USER", "USER"}


def rewrite(node: exp.Expression) -> exp.Expression:
    """Apply the fixes bottom-up, so nested cases (a ratio of averages) all get rewritten."""
    if isinstance(node, exp.With) and not node.args.get("recursive") and any(
            isinstance(c.this, exp.SetOperation) and _self_referencing(c) for c in node.expressions):
        node.set("recursive", True)  # the keyword is optional in Snowflake
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
_SUPPORTED = {"BIGINT", "BLOB", "BOOLEAN", "DATE", "DOUBLE", "HUGEINT", "INTEGER", "JSON", "TIME", "TIMESTAMP",
              "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "UBIGINT", "VARCHAR", '"NULL"'}


def coerce_result(cursor) -> None:  # noqa: ANN001
    """Re-select result columns whose DuckDB type the Snowflake wire protocol cannot carry:
    ARRAY and OBJECT values travel as JSON, small integers as BIGINT, everything else as text."""
    if cursor._arrow_table is None or not cursor._last_sql or not cursor._duck_conn.description:
        return
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


_OPERATORS = (exp.Binary, exp.Unary, exp.In, exp.Between, exp.Is)


def parenthesize_arrows(transformed: exp.Expression) -> exp.Expression:
    """DuckDB's `->` binds more loosely than `=`: `a -> '$[2]' = 'c'` parses as
    `a -> ('$[2]' = 'c')`. Wrap every JSON access that sits inside an operator."""
    for node in list(transformed.find_all(exp.JSONExtract, exp.JSONExtractScalar)):
        if isinstance(node.parent, _OPERATORS):
            node.replace(exp.paren(node.copy()))
    return transformed


def keep_outer_rows(transformed: exp.Expression, flags: list[bool], recursive: list[bool] = ()) -> exp.Expression:
    """FLATTEN(..., OUTER => TRUE) keeps rows that have nothing to expand: a LEFT lateral join."""
    if not any(flags) and not any(recursive):
        return transformed
    calls = [n for n in transformed.find_all(exp.Anonymous) if n.name.upper() == "_FS_FLATTEN"]
    for call, deep in zip(calls, recursive):
        if deep:
            call.set("this", _NS + "_sf_flatten_recursive")
    for call, outer in zip(calls, flags):
        join = call.find_ancestor(exp.Join)
        if outer and join is not None:
            join.set("kind", None)  # an explicit INNER JOIN LATERAL FLATTEN(... OUTER => TRUE)
            join.set("side", "LEFT")
            join.set("on", exp.true())
    return transformed


def patch_upstream() -> None:
    """Switch off emulator transforms that change answers."""
    import fakesnow.transforms as upstream

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
        for node in tree.find_all(exp.AtTimeZone):
            sql = node.sql(dialect="duckdb")
            start = text.find(sql)
            if start >= 0 and start <= pos <= start + len(sql) and not isinstance(node.this, exp.Cast):
                _convert(node.this, "timestamp")  # CONVERT_TIMEZONE on a text column
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
                if not details or not _repair(tree, text, details):
                    return tree if fixed else None  # keep what was fixed: the remaining error is the one to report
                fixed += 1
        return tree
    finally:
        conn.execute("set errors_as_json = false")
