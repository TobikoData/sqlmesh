{% macro incremental_by_time(col, time_type) %}
    {% if is_incremental() %}
    WHERE
        {{ col }} > (select max({{ col }}) from {{ this }})
    {% endif %}
    {% if sqlmesh_incremental is defined %}
    {% set dates = incremental_dates_by_time_type(time_type) %}
    WHERE
        {{ col }} BETWEEN '{{ dates[0] }}' AND '{{ dates[1] }}'
    {% endif %}
{% endmacro %}