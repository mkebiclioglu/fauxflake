select
    event_id,
    account_id,
    amount,
    bucket(amount) as size,
    to_timestamp_ntz(created) as created_at,
    payload:device:os::string as os,
    split(path, '/')[0]::number as shard
from {{ ref('stg_events') }}
where amount > 100
  and kind = 'purchase'
  and region_code between 40 and 60
