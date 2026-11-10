select
    {{ dbt_utils.generate_surrogate_key(['p.id', 'p.currency']) }} as payment_key,
    p.id as payment_id,
    p.user_id,
    p.currency,
    {{ dbt_utils.safe_divide('p.amount_cents', '100') }} * fx.usd_rate as amount_usd,
    p.paid_at,
    {{ dbt.date_trunc('month', 'p.paid_at') }} as paid_month,
    {{ dbt.datediff('u.signed_up_at', 'p.paid_at', 'day') }} as days_since_signup
from {{ ref('stg_payments') }} p
join {{ ref('fx_rates') }} fx on fx.currency = p.currency
left join {{ ref('stg_users') }} u on u.user_id = p.user_id
