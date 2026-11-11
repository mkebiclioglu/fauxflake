{% macro create_bucket_udf() %}
create or replace function {{ target.database }}.{{ target.schema }}.bucket(x float) returns string
as $$ iff(x > 500, 'big', 'small') $$
{% endmacro %}
