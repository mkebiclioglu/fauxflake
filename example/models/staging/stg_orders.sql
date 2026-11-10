-- Raw orders are CDC-style: a row is re-delivered every time its status changes,
-- and the loader is at-least-once. Keep the latest version per order.
select
    order_id,
    customer_id,
    status,
    channel,
    amount::number(12, 2)                            as amount,
    iff(status = 'returned', 0, amount)::number(12, 2) as net_amount,
    ordered_at,
    ordered_at::date                                 as order_date,
    updated_at,
    _loaded_at
from {{ source('shop', 'orders') }}
qualify row_number() over (partition by order_id order by updated_at desc, _loaded_at desc) = 1
