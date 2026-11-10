{{ config(materialized='incremental', unique_key=['event_date', 'event_type'], incremental_strategy='delete+insert') }}

-- Events arrive late (mobile clients buffer offline), so each run reprocesses
-- the trailing `late_window_days`. Set the var to 0 and `fauxflake simulate`
-- will show this model drifting away from a full refresh.
select
    event_date,
    event_type,
    count(*)                             as events,
    count(distinct customer_id)          as customers,
    div0(sum(iff(arrival_lag_seconds > 3600, 1, 0)), count(*)) as late_share
from {{ ref('stg_events') }}
{% if is_incremental() %}
where event_date >= (
    select dateadd('day', -{{ var('late_window_days') }}, coalesce(max(event_date), '1900-01-01'::date))
    from {{ this }}
)
{% endif %}
group by 1, 2
