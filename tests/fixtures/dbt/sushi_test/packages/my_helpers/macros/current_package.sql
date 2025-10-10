{% macro current_package() %}{{ return(adapter.dispatch('current_package', 'my_helpers')) }}{% endmacro %}

{% macro default__current_package() %}my_helpers{% endmacro %}
