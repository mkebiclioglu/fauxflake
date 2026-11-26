"""VARCHAR lengths and collations of the columns a CREATE ... AS SELECT makes (lengths.py), and ALTER COLUMN's type
rules: every expected value here was measured on a live account (2026-11-26)."""

import pytest

from fauxflake import emulator, semantics


@pytest.fixture
def run():
    with emulator.serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        conn = emu._fs.connect(database="ANALYTICS", schema="CI")

        def run(sql):
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()

        run("create or replace table src (a varchar(5), b varchar(12), n int, d date, x number(10,2), s varchar)")
        yield run
    semantics.UDFS.clear()
    semantics.UDTFS.clear()


MEASURED = [
    ("a", 5), ("'initial'", 7), ("''", 1), ("upper(a)", 15), ("lower(b)", 12), ("trim(a)", 5), ("a || b", 17), ("a || 'xyz'", 8),
    ("concat(a, b, 'z')", 18), ("a || n", 16777216), ("a || d", 16777216), ("coalesce(a, b)", 12), ("nvl(a, 'toolongvalue')", 12),
    ("case when n = 1 then a else b end", 12), ("iff(n = 1, a, 'q')", 5), ("a::varchar(20)", 20), ("a::varchar", 16777216),
    ("n::varchar", 16777216), ("substr(b, 1, 3)", 12), ("left(b, 3)", 12), ("replace(a, 'x', 'yy')", 16777216),
    ("lpad(a, 8, '0')", 16777216), ("md5(a)", 32), ("sha2(a)", 128), ("to_char(d, 'YYYY')", 16777216), ("split_part(b, ',', 1)", 12),
    ("s", 16777216), ("s || a", 16777216), ("max(a) over ()", 5), ("initcap(a)", 15), ("uuid_string()", 36), ("nullif(a, 'x')", 5),
    ("greatest(a, b)", 12), ("decode(n, 1, a, b)", 12), ("hex_encode(a)", 40), ("base64_encode(a)", 28), ("null", 16777216),
    ("cast(null as varchar(3))", 3), ("case when n = 1 then a else null end", 16777216), ("coalesce(null, a)", 16777216),
    ("lag(b) over (order by n)", 12), ("lead(a, 1, 'toolong') over (order by n)", 7), ("cast(a as char(10))", 10)]


def test_ctas_lengths_as_snowflake_derives_them(run):
    run("create or replace table derived as select " + ", ".join(f"{e} as c{i}" for i, (e, _) in enumerate(MEASURED)) + " from src")
    got = [r[0] for r in run("select character_maximum_length from information_schema.columns where table_name = 'DERIVED' "
                             "order by ordinal_position")]
    assert dict(zip([e for e, _ in MEASURED], got)) == dict(MEASURED)


def test_lengths_through_unions_ctes_joins_and_views(run):
    run("create or replace table u1 as select a from src union all select b from src")
    run("create or replace table u2 as select a, null as z from src union all select null, b from src")
    run("create or replace table w1 as with t as (select a || b as ab, 'lit' l from src) select ab, l, upper(l) ul from t")
    run("create or replace table j1 as select * from (select a x, b from src) s join (select b as b2, 'q' q from src) r on s.b = r.b2")
    run("create or replace view v1 as (select a, a || 'xx' as ax from src)")
    run("create or replace temporary view analytics.ci.v2 as (select b from src)")  # (dbt's incremental models describe one)
    assert [r[1] for r in run("describe table analytics.ci.v2")] == ["VARCHAR(12)"]
    got = run("select table_name, column_name, character_maximum_length from information_schema.columns "
              "where table_name in ('U1', 'U2', 'W1', 'J1', 'V1') order by 1, ordinal_position")
    assert got == [("J1", "X", 5), ("J1", "B", 12), ("J1", "B2", 12), ("J1", "Q", 1), ("U1", "A", 12), ("U2", "A", 5), ("U2", "Z", 12),
                   ("V1", "A", 5), ("V1", "AX", 7), ("W1", "AB", 17), ("W1", "L", 3), ("W1", "UL", 9)]
    assert run("describe table u2")[1][1] == "VARCHAR(12)"


def test_lateral_aliases_read_once(run):
    chain = ", ".join(f"c{i - 1} || c{i - 1} || c{i - 1} as c{i}" for i in range(1, 25))
    run(f"create or replace table chained as select a as c0, {chain} from src")
    got = run("select character_maximum_length from information_schema.columns where table_name = 'CHAINED' order by ordinal_position")
    assert [r[0] for r in got][:4] == [5, 15, 45, 135] and got[-1][0] == 16777216


def test_collation_kept_and_described_as_snowflake(run):
    run("create or replace table coll_a as select 1 as id, collate('initial', 'en-ci') as s, 'x' as plain, collate('y', 'en-ci-rtrim') as r, "
        "collate('z','en') as e, upper(collate('q','en-ci')) as u, collate('q','en-ci') || 'w' as cat")
    assert [r[1] for r in run("describe table coll_a")][1:] == [
        "VARCHAR(7) COLLATE 'en-ci'", "VARCHAR(1)", "VARCHAR(1) COLLATE 'en-ci-rtrim'", "VARCHAR(1) COLLATE 'en'", "VARCHAR(3) COLLATE 'en-ci'",
        "VARCHAR(2) COLLATE 'en-ci'"]
    assert run("select column_name, collation_name from information_schema.columns where table_name = 'COLL_A' and collation_name is not null "
               "order by ordinal_position") == [("S", "en-ci"), ("R", "en-ci-rtrim"), ("E", "en"), ("U", "en-ci"), ("CAT", "en-ci")]
    run("create or replace table coll_b (a varchar collate 'EN-CI', b varchar(10) collate 'utf8', c varchar)")
    run("create or replace table coll_c as select * from coll_b")
    assert [r[1] for r in run("describe table coll_c")] == ["VARCHAR(16777216) COLLATE 'en-ci'", "VARCHAR(10) COLLATE 'utf8'", "VARCHAR(16777216)"]
    assert run("select get_ddl('table', 'coll_b')") == [
        ("create or replace TABLE COLL_B (\n\tA VARCHAR(16777216) COLLATE 'en-ci',\n\tB VARCHAR(10) COLLATE 'utf8',\n\tC VARCHAR(16777216)\n);",)]
    shown = [r[3] for r in run("show columns in table coll_b")]
    assert shown[0] == '{"type":"TEXT","length":16777216,"byteLength":67108864,"nullable":true,"fixed":false,"collation":"en-ci"}'
    assert shown[1] == '{"type":"TEXT","length":10,"byteLength":40,"nullable":true,"fixed":false,"collation":"utf8"}'
    assert run("select count(*) from coll_b where a = 'X'") == [(0,)]
    run("insert into coll_b values ('x', 'y', 'z')")
    assert run("select count(*) from coll_b where a = 'X'") == [(1,)]


@pytest.mark.parametrize(("old", "new", "error"), [
    ("varchar(10)", "varchar(5)", "cannot change column A from type VARCHAR(10) to VARCHAR(5) because reducing the byte-length of a varchar is not supported."),
    ("varchar(10)", "number", "cannot change column A from type VARCHAR(10) to NUMBER(38,0)"),
    ("number(10,2)", "number(12,3)", "cannot change column A from type NUMBER(10,2) to NUMBER(12,3) because changing the scale of a number is not supported."),
    ("date", "timestamp", "cannot change column A from type DATE to TIMESTAMP_NTZ(9)"),
    ("varchar(10)", "varchar(20) collate 'en-ci'",
     "cannot change column A from type \"VARCHAR(10)\" to \"VARCHAR(20) COLLATE 'en-ci'\" because they have incompatible collations."),
    ("varchar(10) collate 'en-ci'", "varchar(30)",
     "cannot change column A from type \"VARCHAR(10) COLLATE 'en-ci'\" to \"VARCHAR(30)\" because they have incompatible collations."),
    ("varchar(10) collate 'en-ci'", "varchar(20) collate 'EN-CI'", None),
    ("number(10,2)", "number(38,2)", None),
    ("varchar(10)", "varchar", None),
])
def test_alter_column_type_as_snowflake_allows_it(run, old, new, error):
    run(f"create or replace table cz (a {old})")
    if error is None:
        run(f"alter table cz alter column a set data type {new}")
        return
    with pytest.raises(Exception, match="SQL compilation error") as raised:
        run(f"alter table cz alter column a set data type {new}")
    assert error in str(raised.value)
