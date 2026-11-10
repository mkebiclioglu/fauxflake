select
    user_id,
    {{ dbt_utils.pivot('currency', dbt_utils.get_column_values(ref('fx_rates'), 'currency'), agg='sum', then_value='amount_cents') }}
from {{ ref('stg_payments') }}
group by 1
