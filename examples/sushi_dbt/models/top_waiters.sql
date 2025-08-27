{{
  config(
    materialized='view'
  )
}}

SELECT
  waiter_id::INT AS waiter_id,
  revenue::DOUBLE AS revenue,
  1 AS unused_column
FROM {{ ref('waiter_revenue_by_day', version=1) }}
WHERE
  ds = (
    SELECT
      MAX(ds)
    FROM {{ ref('waiter_revenue_by_day', v=1) }}
  )
ORDER BY
  revenue DESC
LIMIT {{ var('top_waiters:limit') }}
