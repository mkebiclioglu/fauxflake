with renamed as (
    select account_id, is_trial as trial, churned, paid_on, due_on as due, flagged
    from {{ ref('stg_accounts') }}
)
select
    account_id,
    iff(trial, 'trial', 'paid') as plan,
    case when churned then 1 else 0 end as churn,
    current_date - paid_on as days_since_paid,
    due - current_date as days_until_due
from renamed
where flagged
