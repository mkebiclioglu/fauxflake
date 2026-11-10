{% snapshot users_snapshot %}
{{ config(target_schema='SNAPSHOTS', unique_key='id', strategy='check', check_cols=['plan', 'email']) }}
select * from {{ source('app', 'users') }}
{% endsnapshot %}
