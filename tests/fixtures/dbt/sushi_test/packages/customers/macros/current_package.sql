{% macro current_package() %}{{ return(adapter.dispatch('current_package', 'customers')) }}{% endmacro %}

{% macro default__current_package() %}customers{% endmacro %}
