{% macro test_dependencies() %}
    {{ nested_test_dependencies() }}
{% endmacro %}

{% macro nested_test_dependencies() %}
    {{ log(var("yet_another_var", 2)) }}
{% endmacro %}
