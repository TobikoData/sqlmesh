{% macro round_dollars(column, scale=2) %}
    ROUND(({{ column }} / 100)::numeric(16, {{ scale }}), {{ scale }})
{% endmacro %}