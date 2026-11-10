{{ config(materialized='view', secure=true, copy_grants=true) }}
select user_id, plan from {{ ref('dim_users') }}
