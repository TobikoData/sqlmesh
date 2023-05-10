{% macro alias(expr, alias) %}
  {{ expr }} AS {{ alias }}
{% endmacro %}


{% macro identity(y) %}
  {{ y }}
{% endmacro %}
