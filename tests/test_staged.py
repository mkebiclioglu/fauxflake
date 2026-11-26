"""SELECT over staged files (staged.py): every expected value here was measured on a live account (2026-11-26)."""


def test_staged_files_read_as_snowflake_reads_them(tmp_path):
    import snowflake.connector

    from fauxflake import emulator

    (tmp_path / "iris.csv").write_text('1,2,3,4,setosa\n5,6,7,8,"vir, ginica"\n')
    (tmp_path / "pipe.txt").write_text("a|b\nx|y\n")
    with emulator.serve() as emu:
        emu.ensure_database("ANALYTICS", "CI")
        conn = snowflake.connector.connect(**emu.connect_kwargs(database="ANALYTICS", schema="CI"))
        cur = conn.cursor()
        for sql in ["create or replace temporary stage st", f"put file://{tmp_path}/iris.csv @st/", f"put file://{tmp_path}/pipe.txt @st/sub/",
                    "create or replace temporary file format fx_csv type = csv field_optionally_enclosed_by = '\"'",
                    "create or replace temporary file format fx_pipe type = csv field_delimiter = '|' skip_header = 1"]:
            cur.execute(sql)

        def rows(sql):  # noqa: ANN001, ANN202
            return [tuple(r) for r in cur.execute(sql).fetchall()]

        assert rows("select $1, $2, $5 from @st/iris.csv (file_format => 'fx_csv') order by 1") == [("1", "2", "setosa"), ("5", "6", "vir, ginica")]
        assert rows("select $1::float + 1 as x, $5 v from @st/iris.csv (file_format => 'fx_csv') t order by 1") == [(2.0, "setosa"), (6.0, "vir, ginica")]
        assert rows("select $1, $2, $3 from @st/sub/pipe.txt (file_format => 'fx_pipe')") == [("x", "y", None)]
        assert rows("select metadata$filename, metadata$file_row_number, $1 from @st (file_format => 'fx_csv', pattern => '.*iris.*') "
                    "order by 2") == [("iris.csv.gz", 1, "1"), ("iris.csv.gz", 2, "5")]
        assert rows("select count(*) from @st (file_format => 'fx_csv')") == [(4,)]
        assert rows("select t.$1 from @st/iris.csv (file_format => fx_csv) t order by 1") == [("1",), ("5",)]
        assert rows("drop file format if exists fx_never") == [("Drop statement executed successfully (FX_NEVER already dropped).",)]
        assert rows("drop file format fx_pipe") == [("FX_PIPE successfully dropped.",)]
        conn.close()
