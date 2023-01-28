{{
    config(
        materialized='ephemeral',
    )
}}

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM {{ ref('orders') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
{% if sqlmesh is defined %}
  WHERE
      ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
{% endif %}
