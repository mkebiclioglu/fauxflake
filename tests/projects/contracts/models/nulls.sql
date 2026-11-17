select iff(id = 2, null, id)::number(38,0) as id from (select 1 as id union all select 2)
