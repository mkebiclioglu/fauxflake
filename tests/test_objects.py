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
    # a hook creates its functions again on every dbt run: replacing one swaps the code behind it
    run("""create or replace function tags(s string) returns array language python runtime_version='3.11' handler='f' as $$
def f(s):
    return None if s is None else ['replaced']
$$""")
    assert run("select tags('b,a,b')") == [('["replaced"]',)]
    # several arguments, quoted lower-case names, a body in single quotes: as real projects write them
    run("""create or replace function "ANALYTICS".ci.regexp_to_array("input_text" string, "regex_text" STRING) returns array language javascript as '
    var found = input_text == null ? null : input_text.match(new RegExp(regex_text, "g"));
    return found == null ? [] : found;
'""")
    assert run("select regexp_to_array('a1b22c333', '[0-9]+'), array_size(analytics.ci.regexp_to_array(null, 'x'))") == [('["1","22","333"]', 0)]
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


def test_tags_are_set_inherited_and_read_back(run):
    run("create tag if not exists cost_center allowed_values 'finance', 'eng'")
    run("create tag pii")
    run("alter table t set tag cost_center = 'eng'")
    run("alter table t modify column name set tag pii = 'name'")
    run("alter schema ci set tag pii = 'none'")
    refs = run("select level, tag_name, tag_value, column_name from table(information_schema.tag_references_all_columns('ci.t', 'table')) order by 1")
    assert refs == [("COLUMN", "PII", "name", "NAME"), ("SCHEMA", "PII", "none", None), ("TABLE", "COST_CENTER", "eng", None)]
    assert run("select system$get_tag('cost_center', 't', 'table'), system$get_tag('pii', 't.name', 'column')") == [("eng", "name")]
    assert [r[1] for r in run("show tags")] == ["COST_CENTER", "PII"]
    with pytest.raises(Exception, match="allowed"):
        run("alter table t set tag cost_center = 'sales'")
    run("alter table t unset tag cost_center")
    assert run("select tag_name from table(information_schema.tag_references('t', 'table'))") == [("PII",)]


def test_transactions_roll_back_and_survive_a_failed_statement(run):
    run("create or replace table src (id int)")
    run("insert into src values (2), (9)")
    run("begin")
    run("insert into t values (3, 'c', 30)")
    with pytest.raises(Exception, match="NOPE"):
        run("select nope from t")  # a failed statement leaves the transaction open, as in Snowflake
    run("merge into t using src on t.id = src.id when matched then update set name = 'merged' "
        "when not matched then insert (id, name, amt) values (src.id, 'new', 0)")
    assert run("select id, name from t order by id") == [(1, "a"), (2, "merged"), (3, "c"), (9, "new")]
    run("rollback")
    assert run("select id, name from t order by id") == [(1, "a"), (2, "b")]
    run("begin transaction")
    run("delete from t where id = 1")
    run("create or replace table other (x int)")  # DDL commits the open transaction
    run("rollback")
    assert run("select count(*) from t") == [(1,)]
    run("start transaction")
    run("insert into t values (5, 'e', 50)")
    run("commit")
    assert run("select count(*) from t") == [(2,)]


def test_merge_in_a_transaction_into_another_database(run):
    run("create database if not exists OTHERDB")
    run("create schema if not exists OTHERDB.S")
    run("create or replace table OTHERDB.S.tgt (id int, v varchar)")
    run("insert into OTHERDB.S.tgt values (1, 'a'), (2, 'b')")
    run("create or replace table src2 as select 2 as id, 'B' as v union all select 3, 'C'")
    merge = ("merge into OTHERDB.S.tgt t using src2 s on t.id = s.id when matched then update set v = s.v "
             "when not matched then insert (id, v) values (s.id, s.v)")
    run("begin")  # what dbt sends around an incremental model's merge
    assert run(merge) == [(1, 1)]
    run("commit")
    assert run("select id, v from OTHERDB.S.tgt order by id") == [(1, "a"), (2, "B"), (3, "C")]
    run("begin")
    run("merge into OTHERDB.S.tgt t using src2 s on t.id = s.id when matched then update set v = 'x'")
    run("rollback")
    assert run("select id, v from OTHERDB.S.tgt order by id") == [(1, "a"), (2, "B"), (3, "C")]
    run("merge into OTHERDB.S.tgt t using src2 s on t.id = s.id when matched then update set v = 'y'")  # no transaction
    assert run("select id, v from OTHERDB.S.tgt order by id") == [(1, "a"), (2, "y"), (3, "y")]
    assert run("show tables like '__FX%' in schema OTHERDB.S") == []


def test_regexp_functions_read_a_number_as_text(run):
    run("create or replace table rx as select 12345.5::double as q, 77::int as n")
    assert run("select regexp_substr(q, '[0-9]+'), regexp_count(n, '7'), regexp_replace(n, '7', 'x'), regexp_like(n, '7+') from rx") == \
        [("12345", 2, "xx", True)]


def test_a_column_alias_wins_over_a_joined_table_of_the_same_name(run):
    run("create or replace table joined as select 1 as id, 10.0::double as annual_price, 3 as quantity")
    run("create or replace table lp as select 1 as id, 8.0::double as billing_list_price")
    assert run("with list_price as (select * from lp) select billing_list_price as list_price, annual_price - list_price as d, "
               "quantity * list_price as t from joined left join list_price on joined.id = list_price.id") == [(8.0, 2.0, 24.0)]


def test_several_columns_dropped_in_one_statement(run):
    run("create or replace table drop_many (x int, a text, b text, c text)")
    run('alter table drop_many drop column a, "B"')
    run("alter table drop_many drop column if exists c, never_there")
    assert [r[0] for r in run("describe table drop_many")] == ["X"]


def test_a_view_in_a_schema_named_like_another_database(run):
    for sql in ("create database if not exists snowflake", "create schema if not exists snowflake.account_usage",
                "create or replace table snowflake.account_usage.query_history (query_id text, end_time timestamp)",
                "insert into snowflake.account_usage.query_history values ('a', '2024-01-01')",
                "create database if not exists prep_db", "create schema if not exists prep_db.snowflake",
                "create or replace table prep_db.snowflake.seen (query_id text, query_end_at timestamp)",
                "insert into prep_db.snowflake.seen values ('z', '2023-01-01')",
                "create or replace view prep_db.snowflake.fresh as (select query_id from snowflake.account_usage.query_history "
                "where end_time > (select max(query_end_at) from prep_db.snowflake.seen))"):
        run(sql)
    assert run("select * from prep_db.snowflake.fresh") == [("a",)]


def test_a_secure_temporary_view_in_a_named_schema(run):
    run("create or replace secure temporary view analytics.ci.sec__dbt_tmp as (select 1 as x)")
    assert run("select x from analytics.ci.sec__dbt_tmp") == [(1,)]


def test_dateadd_on_a_date_stays_a_date_for_day_or_longer_units(run):
    # recorded on Snowflake with SYSTEM$TYPEOF: DATE, DATE, TIMESTAMP_NTZ, DATE, TIMESTAMP_NTZ
    run("create or replace table da as select dateadd(day, 1, to_date('11/01/2009', 'mm/dd/yyyy')) as a, dateadd(quarter, 1, current_date) as b, "
        "dateadd(hour, 1, current_date) as c, dateadd(week, 1, date_trunc('month', current_date)) as d, dateadd(day, 1, '2020-01-01') as e")
    kinds = [r[0] for r in run("select data_type from information_schema.columns where table_name = 'DA' order by ordinal_position")]
    assert kinds == ["DATE", "DATE", "TIMESTAMP_NTZ", "DATE", "TIMESTAMP_NTZ"]
    assert run("select a + 1 from da")[0][0].isoformat() == "2009-11-03"


def test_group_by_all_looks_through_aliases_of_aggregates(run):
    run("create or replace table gba as select 'a' as dept, 1 as emp, 10 as mrs union all select 'a', 2, 10 union all select 'b', 3, 4")
    assert run("select dept, count(distinct emp) as employees, count(*) as n, round(n / nullif(employees, 0), 2) as rate "
               "from gba group by all order by 1") == [("a", 2, 2, 1.0), ("b", 1, 1, 1.0)]
    assert run("select gba.dept, count(distinct emp) as employees, gba.mrs, round(mrs / nullif(employees, 0), 2) as rate "
               "from gba group by all order by 1") == [("a", 2, 10, 5.0), ("b", 1, 4, 4.0)]


def test_regexp_instr_is_a_number_other_functions_take(run):
    assert run("select right('abcdef', len('abcdef') - regexp_instr('abcdef', 'c', 1, 1))") == [("def",)]


def test_an_alias_of_a_function_call_is_read_in_where_and_flatten(run):
    run("create or replace function to_arr(s string) returns array language python runtime_version = '3.8' handler = 'f' as $$\n"
        "def f(s):\n    return s.split(',')\n$$")
    run("create or replace table arr_src as select 'a,b' as d, 1 as id union all select '', 2")
    assert run("with p as (select *, to_arr(d) as links from arr_src where array_size(links) != 1) "
               "select p.id, f.value::varchar from p, table(flatten(links)) f order by 1, 2") == [(1, "a"), (1, "b")]


def test_a_date_format_chosen_per_row(run):
    run("create or replace table fmt_src as select 'snowplow_2024_03' as s union all select 'x_2024_03_09' union all select 'none'")
    rows = run("select s, case when regexp_instr(s, '\\\\d{4}_\\\\d{2}_\\\\d{2}') > 0 then 'YYYY_MM_DD' when regexp_instr(s, '\\\\d{4}_\\\\d{2}') > 0 "
               "then 'YYYY_MM' end as f, to_date(regexp_substr(s, '\\\\d{4}_\\\\d{2}_?\\\\d{0,2}'), f) as d from fmt_src order by 1")
    assert [(r[0], r[2].isoformat() if r[2] else None) for r in rows] == [
        ("none", None), ("snowplow_2024_03", "2024-03-01"), ("x_2024_03_09", "2024-03-09")]


def test_a_kept_warehouse_remembers_the_functions_its_views_call(tmp_path):
    import snowflake.connector

    from fauxflake.emulator import serve

    def connect(e):
        e.ensure_database("ANALYTICS", "PUBLIC")
        return snowflake.connector.connect(**e.connect_kwargs(database="ANALYTICS", schema="PUBLIC")).cursor()

    with serve(db_path=tmp_path) as e:
        cur = connect(e)
        cur.execute("create or replace function twice(n number) returns number as $$ n * 2 $$")
        cur.execute("create or replace table t as select 21 as n")
        cur.execute("create or replace view v as select twice(n) as m from t")
    with serve(db_path=tmp_path, keep=True) as e:
        cur = connect(e)
        assert cur.execute("select m from v").fetchall() == [(42,)]


def test_a_union_reads_text_with_an_offset_as_the_time_its_column_holds(run):
    run("create or replace table amendments(id int, starts timestamp_ntz, day date)")
    run("insert into amendments values (1, '2024-01-01 10:00:00', '2024-01-01')")
    run("create or replace view amendments_v as select id, starts, day from amendments "
        "union all select 2, '9999-12-31 00:00:00.000 +0000', '9999-12-31 00:00:00.000 +0000'")
    rows = run("select id, starts::varchar, day::varchar from amendments_v order by id")
    assert rows == [(1, "2024-01-01 10:00:00.000", "2024-01-01"), (2, "9999-12-31 00:00:00.000", "9999-12-31")]


def test_one_repair_makes_way_for_the_next(run):
    run("create or replace table days(day date, quarter_start date)")
    run("insert into days values ('2024-02-01', '2024-01-01'), ('2024-03-01', '2024-01-01')")
    run("create or replace table opps(id int, made date, closed date, weight varchar)")
    run("insert into opps values (1, '2024-02-01', '2024-03-01', '2')")
    # `weight * 1.5` needs text read as a number; `made_day` is a column just defined and a joined table
    rows = run("select o.made as made_day, made_day.quarter_start as q, o.weight * 1.5 as w, "
               "case when 1 = 1 then datediff(day, made_day, closed_day.day) end as n "
               "from opps o left join days as made_day on o.made = made_day.day left join days as closed_day on o.closed = closed_day.day")
    assert [(str(r[0]), str(r[1]), float(r[2]), r[3]) for r in rows] == [("2024-02-01", "2024-01-01", 3.0, 29)]


def test_unnamed_expressions_are_named_as_snowflake_names_them(run):
    def names(sql):
        cur = run.emu._fs.connect(database="ANALYTICS", schema="CI").cursor()
        cur.execute(sql)
        return [d[0] for d in cur.description]

    assert names("select count(*), sum(amt), max( name ), id+1, id  +  1, upper(name), id, t.id, (id) from t group by id, name") == \
        ["COUNT(*)", "SUM(AMT)", "MAX( NAME )", "ID+1", "ID  +  1", "UPPER(NAME)", "ID", "ID", "ID"]
    assert names("with c as (select id, count(*) n from t group by 1) select distinct max(n), id from c group by id") == ["MAX(N)", "ID"]
    assert names("select name || 'x', case when id = 1 then 'y' end, id::varchar, t.*, id*2 from t") == \
        ["NAME || 'X'", "CASE WHEN ID = 1 THEN 'Y' END", "ID::VARCHAR", "ID", "NAME", "AMT", "ID*2"]
    run("create or replace table counted as select count(*), id + 1 from t group by id")
    assert names("select * from counted") == ["COUNT(*)", "ID + 1"]


def test_having_without_grouping_filters_rows_by_the_columns_just_defined(run):
    rows = run("with first as (select 'k' || id as key from t where id = 1) "
               "select 'k' || id as key, name from t where amt > 0 having key not in (select key from first) order by 1")
    assert rows == [("k2", "b")]
    assert run("select count(*) from t having count(*) > 1") == [(2,)]


def test_a_qualified_temporary_table_goes_with_its_session():
    import snowflake.connector

    from fauxflake.emulator import serve

    with serve() as emu:
        first = snowflake.connector.connect(**emu.connect_kwargs())
        cur = first.cursor()
        cur.execute("create database if not exists ANALYTICS")
        cur.execute("create schema if not exists ANALYTICS.CI")
        cur.execute("create temporary table ANALYTICS.CI.SCRATCH as select 1 as id")
        with pytest.raises(Exception, match="already exists"):
            cur.execute("create temporary table ANALYTICS.CI.SCRATCH as select 2 as id")  # the same session: as in Snowflake
        second = snowflake.connector.connect(**emu.connect_kwargs())
        other = second.cursor()
        other.execute("create temporary table ANALYTICS.CI.SCRATCH as select 3 as id")  # another session has its own
        assert other.execute("select id from ANALYTICS.CI.SCRATCH").fetchall() == [(3,)]
        second.close()
        first.close()
        third = snowflake.connector.connect(**emu.connect_kwargs())
        with pytest.raises(Exception, match="does not exist"):
            third.cursor().execute("select id from ANALYTICS.CI.SCRATCH")
        third.close()


def test_a_missing_table_is_reported_in_snowflakes_words(run):
    with pytest.raises(Exception, match="does not exist or not authorized") as failure:
        run("describe table ANALYTICS.CI.NOT_THERE")
    assert "NOT_THERE" in str(failure.value)


def test_dateadd_on_text_put_together_and_objects_of_every_column_in_an_aggregate(run):
    import datetime
    import json

    assert run("select dateadd('month', 1, concat('2020' - 1, '-01-01')), dateadd('hour', 2, '2020' || '-01-01')") == [
        (datetime.datetime(2019, 2, 1), datetime.datetime(2020, 1, 1, 2))]
    rows = run("select id, array_agg(object_construct(*)) from t group by 1 order by 1")
    assert json.loads(rows[0][1]) == [{"ID": 1, "NAME": "a", "AMT": 10.0}]


def test_columns_built_on_each_other_are_computed_once(run):
    # each column reads the one before it three times: written out, the last is 3**24 expressions
    chain = ", ".join(f"iff(c{i - 1} > 0, c{i - 1}, c{i - 1}) + 1 as c{i}" for i in range(1, 25))
    run(f"create or replace table chained as select t.id as c0, {chain}, row_number() over (order by c24 desc) as rn, t.name, amt "
        "from t where t.id >= 1 qualify rn = 1 order by c0")
    assert run("select c0, c24, rn, name from chained") == [(top := max(r[0] for r in run("select id from t")), top + 24, 1,
                                                             run(f"select name from t where id = {top}")[0][0])]
    assert len(run("select column_name from information_schema.columns where table_name = 'CHAINED'")) == 28


def test_a_union_keeps_text_beside_a_variant_as_a_variant_string(run):
    run("create or replace table docs as select 1 as n, 'abc'::variant as v")
    run("create or replace table texts as select 2 as n, '' as v union all select 3, 'plain'")
    assert run("select n, v from (select * from docs union all select * from texts) order by n") == [
        (1, '"abc"'), (2, '""'), (3, '"plain"')]
    # the same inside COALESCE, and after another repair was needed first (SUM over text)
    assert run("select n, coalesce(v, 'none') from (select * from docs union all select 4, null) order by n") == [(1, '"abc"'), (4, '"none"')]
    run("create or replace table counted as select '2' as users, v from docs union all select '3', v from texts")
    assert run("select sum(users) over (partition by coalesce(v, '')) from counted order by 1") == [(2,), (3,), (3,)]
