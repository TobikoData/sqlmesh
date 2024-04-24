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

{% macro incremental_dates_by_time_type(time_type) %}
    {% if time_type == 'ds' %}
        {{ return([start_ds, end_ds]) }}
    {% elif time_type == 'ts' %}
        {{ return([start_ts, end_ts]) }}
    {% elif time_type == 'epoch' %}
        {{ return([start_epoch, end_epoch]) }}
    {% elif time_type == 'millis' %}
        {{ return([start_millis, end_millis]) }}
    {% elif time_type == 'date' %}
        {{ return([start_date, end_date]) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown time type: " ~time_type) }}
    {% endif %}
{% endmacro %}
