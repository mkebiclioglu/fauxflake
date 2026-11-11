{{ config(materialized='incremental', unique_key='account_id') }}
-- reads its own table by name, as projects that keep a high-water mark often do
select account_id, sum(amount) as total, count(*) as events
from {{ ref('fct_events') }}
where created_at >= (select coalesce(max(last_seen), '1900-01-01'::timestamp) from {{ target.database }}.{{ target.schema }}.account_watermark)
group by 1
