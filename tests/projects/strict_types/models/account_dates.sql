select
    account_id,
    year(signed_up) as signup_year,
    date_trunc('month', billed_on) as billed_month,
    last_day(renewal) as renewal_month_end,
    hour(seen) as seen_hour,
    extract(dow from shipped) as shipped_dow,
    months_between(closed, opened) as months_open
from {{ ref('stg_accounts') }}
where month(signed_up) >= 1
