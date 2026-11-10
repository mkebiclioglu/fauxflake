{{ dbt_utils.union_relations(relations=[ref('fct_payments'), ref('dim_users')], include=['USER_ID']) }}
