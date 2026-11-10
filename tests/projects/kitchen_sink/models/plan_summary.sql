{{ config(materialized='dynamic_table', target_lag='1 hour', snowflake_warehouse='LOCAL') }}
-- a dynamic table: Snowflake keeps it fresh on its own; locally it is rebuilt each run
select plan, count(*) as users
from {{ ref('dim_users') }}
group by plan
