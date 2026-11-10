select
    event_id,
    customer_id,
    event_type,
    occurred_at,
    date_trunc('day', occurred_at)::date as event_date,
    datediff('second', occurred_at, _loaded_at) as arrival_lag_seconds,
    _loaded_at
from {{ source('shop', 'events') }}
qualify row_number() over (partition by event_id order by _loaded_at) = 1
