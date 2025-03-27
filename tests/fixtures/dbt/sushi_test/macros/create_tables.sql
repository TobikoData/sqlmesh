{% macro create_tables(schemas) %}
    {% for schema in schemas %}
        create or replace table schema_table_{{schema}} as select '{{schema}}' as schema;
    {% endfor%}
{% endmacro %}