"""Snowflake objects beyond tables and views: functions, procedures, streams, tasks, policies."""

import json

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

        run.emu = emu
        run("create or replace table t (id int, name string, amt number(10,2))")
        run("insert into t values (1, 'a', 10), (2, 'b', 20)")
        yield run
    semantics.UDFS.clear()
    semantics.UDTFS.clear()


def test_sql_functions_scalar_overloaded_and_table(run):
    run("create or replace function area(r float) returns float as 'pi() * r * r'")
    run("create or replace function area(w float, h float) returns float as $$ w * h $$")
    assert run("select round(area(1), 2) as one, analytics.ci.area(2, 3) as two") == [(3.14, 6)]
    run("create function bigger(x int) returns table (id int) as $$ select id from t where id > x $$")
    assert run("select * from table(bigger(1))") == [(2,)]
    assert [r[1] for r in run("show user functions")] == ["AREA", "AREA", "BIGGER"]
    run("drop function area(float)")
    with pytest.raises(Exception, match="(?i)area"):
        run("select area(1)")


def test_python_and_javascript_functions(run):
    run("""create or replace function tags(s string) returns array language python runtime_version='3.11' handler='f' as $$
def f(s):
    return None if s is None else sorted(set(s.split(',')))
$$""")
    assert run("select array_size(tags('b,a,b')), tags(null)") == [(2, None)]
    run("create or replace function dbl(x float) returns float language javascript as $$ return X * 2; $$")
    assert run("select dbl(amt) from t order by id") == [(20.0,), (40.0,)]
    # variants cross as objects; newer syntax than ES5 is compiled down
    run("create function shape(v variant) returns variant language javascript as $$ const k = Object.keys(V).map(x => x.toUpperCase()); return {keys: k, n: V.a + 1}; $$")
    assert json.loads(run("""select shape(parse_json('{"a": 1, "b": 2}'))""")[0][0]) == {"keys": ["A", "B"], "n": 2}


def test_external_function_is_null_and_a_spec_stand_in_wins(run, capsys):
    run("create or replace external function geo(ip string) returns variant api_integration = x as 'https://example.com/geo'")
    assert run("select geo('1.1.1.1')") == [(None,)]
    assert "returns NULL here" in capsys.readouterr().err


SCRIPT = """
create or replace procedure fill(n int, label string) returns table (id int, name string) language sql as
$$
declare
  total int default 0;
  rs resultset;
begin
  for i in 1 to n do
    insert into t values (:i + 100, :label, :i);
    total := total + i;
  end for;
  let c cursor for select id, amt from t where id > 100;
  for rec in c do
    if (rec.amt > 2) then
      update t set name = 'big' where id = :rec.id;
    elseif (rec.amt = 2) then
      update t set name = 'two' where id = :rec.id;
    end if;
  end for;
  let k := 0;
  repeat k := k + 1; until (k >= 3) end repeat;
  case when k = 3 then insert into t values (200, 'case', :total); else null; end case;
  begin
    select * from no_such_table;
  exception when other then
    insert into t values (999, 'caught', 0);
  end;
  rs := (select id, name from t where id > 100 order by id);
  return table(rs);
end;
$$
"""


def test_snowflake_scripting_procedure(run):
    run(SCRIPT)
    assert run("call fill(3, 'x')") == [(101, "x"), (102, "two"), (103, "big"), (200, "case"), (999, "caught")]
    assert run("select amt from t where id = 200") == [(6,)]
    assert run("execute immediate $$ declare x int default 3; begin return x * 2; end; $$") == [(6,)]
    assert run("begin let n := (select count(*) from t); return n; end") == [(7,)]
    assert run("execute immediate 'select count(*) from t'") == [(7,)]


def test_javascript_and_python_procedures(run):
    run("""create or replace procedure small(tbl string) returns variant language javascript as $$
  var rs = snowflake.execute({sqlText: "select id, name from " + TBL + " where id < ? order by id", binds: [3]});
  var out = [];
  while (rs.next()) { out.push({id: rs.getColumnValue(1), name: rs.getColumnValue('NAME')}); }
  snowflake.createStatement({sqlText: "insert into t values (50, 'js', 1)"}).execute();
  return out;
$$""")
    assert json.loads(run("call small('t')")[0][0]) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    run("""create or replace procedure py(n int) returns string language python runtime_version='3.11'
packages=('snowflake-snowpark-python') handler='run' as $$
def run(session, n):
    rows = session.sql("select count(*) as c from t").collect()
    session.table("t").filter("id < 3").write.mode("overwrite").save_as_table("t_small")
    return f"{rows[0].C + n} rows, {session.table('t_small').count()} small"
$$""")
    assert run("call py(1)") == [("4 rows, 2 small",)]
    with pytest.raises(Exception, match="Unknown procedure"):
        run("call nope()")


def test_stream_shows_changes_until_a_dml_consumes_it(run):
    run("create or replace stream s on table t")
    assert run("select count(*) from s") == [(0,)]
    run("insert into t values (3, 'c', 30)")
    run("delete from t where id = 1")
    assert run("select id, metadata$action from s order by id") == [(1, "DELETE"), (3, "INSERT")]
    assert run("select system$stream_has_data('s')") == [(True,)]
    run("select * from s")  # reading does not consume
    assert run("select count(*) from s") == [(2,)]
    run("create or replace table seen as select id from s where metadata$action = 'INSERT'")
    assert run("select id from seen") == [(3,)]
    assert run("select count(*) from s") == [(0,)]
    assert run("show streams")[0][1] == "S"
    run("drop stream s")
    with pytest.raises(Exception, match="(?i)does not exist"):
        run("select * from s")


def test_tasks_run_on_demand_with_their_resumed_children(run):
    run("create or replace stream s on table t append_only = true")
    run("create or replace task root_task warehouse = wh schedule = 'USING CRON 0 * * * * UTC' as insert into t values (600, 'root', 0)")
    run("create or replace task child after root_task when system$stream_has_data('s') as create or replace table consumed as select id from s")
    run("create or replace task idle after root_task as insert into t values (700, 'idle', 0)")
    run("alter task child resume")
    assert run("execute task root_task") == [("Task executed: ROOT_TASK, CHILD.",)]
    assert run("select id from consumed") == [(600,)]
    assert run("select count(*) from t where id = 700") == [(0,)]  # suspended tasks do not run
    assert {r[1]: r[10] for r in run("show tasks")} == {"ROOT_TASK": "suspended", "CHILD": "started", "IDLE": "suspended"}


def test_masking_and_row_access_policies_apply_on_read(run):
    run("create or replace masking policy hide as (val string) returns string -> case when current_role() = 'ADMIN' then val else '***' end")
    run("alter table t modify column name set masking policy hide")
    run("create or replace row access policy big_only as (amount number) returns boolean -> amount > 10")
    run("alter table t add row access policy big_only on (amt)")
    assert run("select id, name from t") == [(2, "***")]
    assert run("select x.name from analytics.ci.t x join t y on x.id = y.id") == [("***",)]
    run("create or replace view v as select name from t")
    assert run("select * from v") == [("***",)]
    run("insert into t values (3, 'c', 30)")  # writes are not filtered
    run("use role admin")  # the policy answers for whoever reads, not whoever wrote it
    assert run("select name from t order by id") == [("b",), ("c",)] and run("select * from v order by 1") == [("b",), ("c",)]
    run("alter table t modify column name unset masking policy")
    run("alter table t drop row access policy big_only")
    assert run("select count(*), min(name) from t") == [(3, "a")]


def test_result_scan_last_query_swap_and_context(run):
    run("show tables like 'T'")
    assert run('select "name", "kind" from table(result_scan(last_query_id()))') == [("T", "TABLE")]
    run("create or replace table u as select 9 as id, 'z' as name, 1.00 as amt")
    run("alter table t swap with u")
    assert run("select id from t") == [(9,)] and run("select count(*) from u") == [(2,)]
    assert run("select current_warehouse(), current_role(), current_account()") == [("LOCAL", "SYSADMIN", "FAUXFLAKE")]


def test_pipes_and_alerts_are_stored_and_run_on_demand(run):
    run("create or replace alert low warehouse = wh schedule = '1 minute' if (exists (select 1 from t where amt < 15)) then insert into t values (77, 'alert', 0)")
    assert "true" in run("execute alert low")[0][0]
    assert run("select count(*) from t where id = 77") == [(1,)]
    run("create or replace pipe p auto_ingest = true as copy into t from @some_stage")
    assert [r[1] for r in run("show pipes")] == ["P"]


def test_copy_into_from_cloud_storage_served_locally(run, tmp_path):
    import gzip

    from fauxflake import objects

    (tmp_path / "v1").mkdir()
    with gzip.open(tmp_path / "v1" / "codes.csv_0_0_0.csv.gz", "wt") as f:
        f.write('"A, with comma",1,\n"B",2,__NULL__\n')
    (tmp_path / "v1" / "other.csv").write_text("x,9,y\n")
    objects.LOCATIONS = {"s3://my-bucket/sets": str(tmp_path)}
    try:
        run("create or replace table codes (label string, n int, note string)")
        loaded = run("""copy into codes from s3://my-bucket/sets/v1
            file_format = (type = CSV compression = 'GZIP' empty_field_as_null = true field_optionally_enclosed_by = '"'
                           escape_unenclosed_field = NONE
                           /* a comment with an apostrophe: Snowflake's default \\N marker */
                           null_if = ('__NULL__'))
            pattern = '.*\\/codes.csv.*';""")
        assert [(r[1], r[3]) for r in loaded] == [("LOADED", 2)]
        assert run("select label, n, note from codes order by n") == [("A, with comma", 1, None), ("B", 2, None)]
    finally:
        objects.LOCATIONS = {}


def test_anonymous_procedure_and_system_table_function(run):
    assert run("""with total as procedure (floor int) returns int language sql as
$$ begin return (select count(*) from t where id >= :floor); end; $$
call total(2);""") == [(1,)]
    assert run("select count(*) from table(information_schema.task_history(task_name => 'x'))") == [(0,)]
    assert run("select query_id from table(information_schema.query_history_by_session()) limit 1") == []


def test_objects_maps_and_empty_keys(run):
    row = run("""select object_delete(parse_json('{"a":1,"b":2}'), 'a'), object_insert(object_construct('a', 1), 'b', 'x'),
                        object_pick(parse_json('{"a":1,"b":2,"c":3}'), 'a', 'z'),
                        map_cat({'a': 1}::map(varchar, number), {'a': 2, 'c': 3}::map(varchar, number)),
                        parse_json('{"": 5, "a": {"": 7}}'):a:""::int""")
    assert [json.loads(v) if isinstance(v, str) else v for v in row[0]] == [{"b": 2}, {"a": 1, "b": "x"}, {"a": 1}, {"a": 2, "c": 3}, 7]


def test_external_tables_account_objects_and_schema_clone(run):
    run("create view v as select id from t")
    run("""create or replace external table ext_orders (order_id int as (value:order_id::int))
           location = @my_stage/orders file_format = (type = parquet) auto_refresh = false""")
    assert run("select count(*), max(order_id) from ext_orders") == [(0, None)]
    run("create or replace external table t (x int as (value:x::int)) location = @s file_format = (type = csv)")
    assert run("select count(*) from t") == [(2,)]  # a table that already holds rows keeps them
    assert run("show warehouses") == [] and run("show grants to role sysadmin") == []
    run("rollback")
    run("create or replace schema analytics.backup clone analytics.ci")
    assert run("select (select count(*) from analytics.backup.t), (select count(*) from analytics.backup.v)") == [(2, 2)]


def test_scheduled_tasks_run_as_simulated_time_passes(run):
    from fauxflake import objects

    run("create or replace stream s on table t append_only = true")
    run("create or replace table seen (id int)")
    run("create or replace task load_seen warehouse = wh schedule = '60 minute' when system$stream_has_data('s') as insert into seen select id from s")
    run("create or replace task never_resumed schedule = '60 minute' as insert into seen values (-1)")
    run("alter task load_seen resume")

    emu = run.emu
    assert objects.run_due_tasks(emu, "ANALYTICS") == []  # the stream is empty: the WHEN condition holds it back
    run("insert into t values (3, 'c', 30)")
    assert objects.run_due_tasks(emu, "ANALYTICS") == ["LOAD_SEEN"]
    assert run("select id from seen") == [(3,)]
    assert objects.run_due_tasks(emu, "ANALYTICS") == []  # consumed


def test_describe_reaches_tables_in_other_databases(run):
    # a source lives in RAW while dbt is connected to ANALYTICS: dbt_utils.star(source(...)) depends on this
    run("create database if not exists raw")
    run("create schema if not exists raw.app")
    run("create or replace table raw.app.event (id string, received_at timestamp)")
    run("use schema analytics.ci")
    assert [r[0] for r in run("describe table raw.app.event")] == ["ID", "RECEIVED_AT"]


def test_frozen_clock_leaves_text_alone(run):
    from datetime import datetime

    from fauxflake import emulator

    emulator.FROZEN_NOW = datetime(2025, 3, 2)
    try:
        assert run("""select 'macro.dbt.current_timestamp' as name, "current_date" as quoted, current_date() as today
                      from (select 1 as "current_date")""") == [("macro.dbt.current_timestamp", 1, datetime(2025, 3, 2).date())]
    finally:
        emulator.FROZEN_NOW = None
