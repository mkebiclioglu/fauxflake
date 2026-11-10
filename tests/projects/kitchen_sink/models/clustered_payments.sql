{{ config(materialized='incremental', unique_key='payment_key', cluster_by=['currency'], automatic_clustering=true,
          transient=false, on_schema_change='append_new_columns', incremental_strategy='delete+insert') }}
select payment_key, payment_id, currency, amount_usd
from {{ ref('fct_payments') }}
