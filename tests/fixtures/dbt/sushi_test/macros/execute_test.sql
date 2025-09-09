{% macro runtime_sql() %}
    {% if execute %}
        {% set result = run_query("SELECT 1 as test_col") %}
        {% set test_value = result.columns[0][0] %}
        {{ return(test_value) }}
    {% else %}
        {{ return("parse_time_placeholder") }}
    {% endif %}
{% endmacro %}
