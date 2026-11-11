select account_id, max(created_at) as last_seen from {{ ref('fct_events') }} group by 1
