{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        cluster_by=['ds'],
        time_column='ds',
    )
}}

SELECT
  id::INT AS id, /* Primary key */
  customer_id::INT AS customer_id, /* Id of customer who made the order */
  waiter_id::INT AS waiter_id, /* Id of waiter who took the order */
  start_ts::TEXT AS start_ts, /* Start timestamp */
  end_ts::TEXT AS end_ts, /* End timestamp */
  ds::TEXT AS ds /* Date of order */
FROM {{ ref('raw_orders') }}
{% if is_incremental() %}
WHERE
  ds > (select max(ds) from {{ this }})
{% endif %}
{% if sqlmesh is defined %}
  WHERE
      ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
{% endif %}
