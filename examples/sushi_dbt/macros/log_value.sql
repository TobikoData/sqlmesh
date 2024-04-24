{% macro log_value(v) %}
    {{ log("Entered value is: " ~ v) }}
{% endmacro %}
