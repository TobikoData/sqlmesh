{{
  config(
    materialized='ephemeral',
  )
}}

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM {{ ref('orders') }}
{{ incremental_by_time('ds', 'ds') }}
