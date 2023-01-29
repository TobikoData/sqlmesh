{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        time_column='ds',
        cluster_by=['ds'],
    )
}}

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM {{ ref('raw_orders') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
{% if sqlmesh is defined %}
  WHERE
      ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
{% endif %}
