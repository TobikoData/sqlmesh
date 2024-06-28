{% macro current_engine() %}{{ return(adapter.dispatch('current_engine')) }}{% endmacro %}

{% macro default__current_engine() %}default{% endmacro %}

{% macro duckdb__current_engine() %}duckdb{% endmacro %}
