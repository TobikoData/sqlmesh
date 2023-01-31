{% macro incremental_by_ds(col) %}
    {% if is_incremental() %}
    WHERE
        {{ col }} > (select max({{ col }}) from {{ this }})
    {% endif %}
    {% if sqlmesh is defined %}
    WHERE
        {{ col }} BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
    {% endif %}
{% endmacro %} 
