select
    customer_id,
    email,
    country,
    created_at,
    _loaded_at
from {{ source('shop', 'customers') }}
qualify row_number() over (partition by customer_id order by _loaded_at desc) = 1
