{% macro store_schemas(schemas) %}
    create or replace table schema_table as select {{schemas}} as all_schemas;
{% endmacro %}