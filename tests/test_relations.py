"""What Snowflake keeps about relations (relations.py): every expected value here was measured on a live account."""

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

        def named(sql):
            cur = conn.cursor()
            cur.execute(sql)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

        run.named = named
        run("create or replace table base (id int, v varchar)")
        run("insert into base values (1, 'a'), (2, 'b')")
        yield run
    semantics.UDFS.clear()
    semantics.UDTFS.clear()


def test_show_tables_has_snowflakes_columns_in_order(run):
    run("create or replace transient table tr (a int) cluster by (a) comment = 'kept'")
    rows = {r["name"]: r for r in run.named("show tables in schema analytics.ci")}
    assert list(rows["BASE"]) == [
        "created_on", "name", "database_name", "schema_name", "kind", "comment", "cluster_by", "rows", "bytes", "owner", "retention_time",
        "automatic_clustering", "change_tracking", "search_optimization", "search_optimization_progress", "search_optimization_bytes",
        "is_external", "enable_schema_evolution", "owner_role_type", "is_event", "is_hybrid", "is_iceberg", "is_dynamic", "is_immutable",
        "is_interactive", "row_timestamp", "error_logging"]
    assert (rows["BASE"]["kind"], rows["BASE"]["rows"], rows["BASE"]["bytes"], rows["BASE"]["retention_time"]) == ("TABLE", 2, 1024, "1")
    assert rows["BASE"]["created_on"].year > 2000
    tr = rows["TR"]
    assert (tr["kind"], tr["comment"], tr["cluster_by"], tr["automatic_clustering"], tr["rows"], tr["bytes"]) == \
        ("TRANSIENT", "kept", "LINEAR(a)", "ON", 0, 0)


def test_dynamic_table_lifecycle(run):
    run("create or replace dynamic table dt target_lag = '1 hour' warehouse = wh refresh_mode = full cluster by (id) "
        "as (select id, v from base)")
    run("create or replace transient dynamic table later target_lag = 'downstream' warehouse = wh initialize = on_schedule "
        "as select id from base")
    shown = {r["name"]: r for r in run.named("show dynamic tables in schema analytics.ci")}
    dt = shown["DT"]
    assert (dt["target_lag"], dt["refresh_mode"], dt["warehouse"], dt["cluster_by"], dt["rows"], dt["scheduling_state"]) == \
        ("1 hour", "FULL", "WH", "LINEAR(id)", 2, "ACTIVE")
    assert dt["text"] == ("create or replace dynamic table dt cluster by (id) lag = '1 hour' refresh_mode = 'FULL' "
                          "initialize = 'ON_CREATE' warehouse = WH as (select id, v from base)")
    assert (shown["LATER"]["target_lag"], shown["LATER"]["refresh_mode"], shown["LATER"]["configured_refresh_mode"]) == \
        ("DOWNSTREAM", "ADAPTIVE", "AUTO")
    assert shown["LATER"]["data_timestamp"] is None
    with pytest.raises(Exception, match="is not initialized"):
        run("select count(*) from later")
    with pytest.raises(Exception, match="Invalid Operation INSERT on Dynamic Table"):
        run("insert into dt values (9, 'z')")
    info = {r[0]: r[1:] for r in run("select table_name, table_type, is_transient, is_dynamic, clustering_key from "
                                     "information_schema.tables where table_name in ('DT', 'LATER', 'BASE')")}
    assert info == {"DT": ("BASE TABLE", "NO", "YES", "LINEAR(id)"), "LATER": ("BASE TABLE", "YES", "YES", None),
                    "BASE": ("BASE TABLE", "NO", "NO", None)}
    # a dynamic table's source gets change tracking
    assert {r["name"]: r["change_tracking"] for r in run.named("show tables in schema analytics.ci")}["BASE"] == "ON"
    run("insert into base values (3, 'c')")
    refreshed = run("alter dynamic table dt refresh")[0]
    assert (refreshed[0], refreshed[1], refreshed[2]) == ("ANALYTICS.CI.DT", '{"insertedRows":3,"copiedRows":0,"deletedRows":2}', 1)
    assert run("select count(*) from dt") == [(3,)]
    run("alter dynamic table dt set target_lag = '2 hours' warehouse = other")
    run("alter dynamic table dt suspend")
    dt = {r["name"]: r for r in run.named("show dynamic tables like 'DT' in schema analytics.ci")}["DT"]
    assert (dt["target_lag"], dt["warehouse"], dt["scheduling_state"], dt["rows"]) == ("2 hours", "OTHER", "SUSPENDED", 3)
    assert run("select get_ddl('table', 'dt')") == [(
        "create or replace dynamic table DT(\n\tID,\n\tV\n) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE "
        "warehouse = OTHER\n cluster by (id) as (select id, v from base);",)]
    run("drop dynamic table later")
    assert [r["name"] for r in run.named("show dynamic tables in schema analytics.ci")] == ["DT"]


def test_get_ddl_as_snowflake_writes_it(run):
    run("create or replace transient table gd (id int not null, name varchar(20) default 'x' comment 'the name', amt number(10,2), "
        "ts timestamp_ntz) cluster by (id, name) comment = 'a ''q'' b'")
    assert run("select get_ddl('table', 'gd')") == [(
        "create or replace TRANSIENT TABLE GD cluster by (id, name)(\n\tID NUMBER(38,0) NOT NULL,\n\tNAME VARCHAR(20) DEFAULT 'x' "
        "COMMENT 'the name',\n\tAMT NUMBER(10,2),\n\tTS TIMESTAMP_NTZ(9)\n)COMMENT='a ''q'' b'\n;",)]
    run("create or replace table keys (a int primary key, b varchar unique, c int references keys(a))")
    assert run("select get_ddl('table', 'keys')") == [(
        "create or replace TABLE KEYS (\n\tA NUMBER(38,0) NOT NULL,\n\tB VARCHAR(16777216),\n\tC NUMBER(38,0),\n\tunique (B),\n\t"
        "primary key (A),\n\tforeign key (C) references ANALYTICS.CI.KEYS(A)\n);",)]
    run("create or replace view vw comment = 'vw' as select * from base")
    assert run("select get_ddl('view', 'vw')") == [("create or replace view VW(\n\tID,\n\tV\n) COMMENT='vw'\n as select * from base;",)]
    run("create or replace secure view sv as select 1 x")
    assert run("select get_ddl('view', 'sv')") == [("create or replace secure view SV(\n\tX\n) as select 1 x;",)]


def test_comments_and_last_altered(run):
    run("comment on table base is $$dollar 'quoted'$$")
    run("comment on column base.v is 'vee''s'")
    run("alter table base alter id comment $$the id$$")
    assert run("select comment from information_schema.tables where table_name = 'BASE'") == [("dollar 'quoted'",)]
    assert run("select column_name, comment from information_schema.columns where table_name = 'BASE' order by 1") == \
        [("ID", "the id"), ("V", "vee's")]
    assert [r[9] for r in run("describe table base")] == ["the id", "vee's"]
    (before,) = run("select last_altered from information_schema.tables where table_name = 'BASE'")[0]
    run("delete from base where id = 1")
    (after, rows) = run("select last_altered, row_count from information_schema.tables where table_name = 'BASE'")[0]
    assert after > before and rows == 1


def test_show_objects_pages_and_starts_with(run):
    for i in range(5):
        run(f"create or replace table p{i} (a int)")
    names = [r[1] for r in run("show terse objects in schema analytics.ci starts with 'P' limit 2 from 'P1'")]
    assert names == ["P2", "P3"]
