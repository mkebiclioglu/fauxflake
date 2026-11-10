select
    u.user_id,
    u.email,
    u.plan,
    {{ dbt_utils.star(from=ref('user_payment_pivot'), except=['USER_ID']) }}
from {{ ref('stg_users') }} u
left join {{ ref('user_payment_pivot') }} p using (user_id)
