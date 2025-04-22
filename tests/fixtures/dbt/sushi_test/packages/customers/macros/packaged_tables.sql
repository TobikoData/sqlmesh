{% macro packaged_tables(schemas) %}
    {% for schema in schemas %}
        create or replace table schema_table_{{schema}}_nested_package as select '{{schema}}' as schema;
    {% endfor%}
{% endmacro %}