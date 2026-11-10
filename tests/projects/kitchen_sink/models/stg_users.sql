{{ config(materialized='ephemeral') }}
with source as (select * from {{ source('app', 'users') }}),
renamed as (
    select id as user_id, lower(email) as email, plan, signed_up_at, _loaded_at
    from source
)
select * from renamed
