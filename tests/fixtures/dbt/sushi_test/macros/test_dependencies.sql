{% macro test_dependencies() %}
    {{ nested_test_dependencies() }}
{% endmacro %}

{% macro nested_test_dependencies() %}
    {{ log(var("yet_another_var", 2)) }}
    {{ log(var("nested_vars")['some_nested_var']) }}
{% endmacro %}


{% macro dynamic_var_name_dependency(var_name) %}
    {% set results = run_query('select 1 as one') %}
    {{ return(var(var_name)) }}
{% endmacro %}
