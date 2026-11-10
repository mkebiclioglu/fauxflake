{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

-- The watermark is on load time, not event time: an order version can arrive
-- after a newer one, and `updated_at > max(updated_at)` would silently skip it.
select
    o.order_id,
    o.customer_id,
    c.country,
    o.status,
    o.channel,
    o.amount,
    o.net_amount,
    o.ordered_at,
    o.order_date,
    datediff('day', c.created_at, o.ordered_at) as customer_age_days,
    o.updated_at,
    o._loaded_at
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
{% if is_incremental() %}
where o._loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
