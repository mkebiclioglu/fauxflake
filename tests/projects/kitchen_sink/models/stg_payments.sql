{{ config(materialized='view') }}
{{ dbt_utils.deduplicate(relation=source('app', 'payments'), partition_by='id', order_by='_loaded_at desc') }}
