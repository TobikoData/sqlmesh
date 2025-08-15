{% macro graph_usage() %}
{% if execute %}
  {% set model_nodes = graph.nodes.values()
     | selectattr("resource_type", "equalto", "model")
     | list %}

  {% set out = [] %}
  {% for node in model_nodes %}
    {% set line = "select '" ~ node.unique_id ~ "' as unique_id, '" ~ node.config.materialized ~ "' as materialized" %}
    {% do out.append(line) %}
  {% endfor %}

  {% if out %}
    {% set sql_statement = "create or replace table graph_table as\n" ~ (out | join('\nunion all\n')) %}
    {{ return(sql_statement) }}
  {% endif %}
{% endif %}
{% endmacro %}
