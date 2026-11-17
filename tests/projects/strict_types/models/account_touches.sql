with renamed as (select account_id, first_touch as touched, last_touch from {{ ref('stg_accounts') }})
select account_id, quarter(touched) as q, dayofweek((last_touch)) as dow from renamed
