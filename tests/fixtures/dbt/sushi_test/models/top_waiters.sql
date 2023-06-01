{{
    config(
        materialized='view'
    )
}}

SELECT
  waiter_id::INT AS waiter_id,
  revenue::DOUBLE AS {{ var("top_waiters:revenue") }}
FROM {{ ref('sushi', 'waiter_revenue_by_day') }}
WHERE
  ds = (
    SELECT
      MAX(ds)
    FROM {{ ref('waiter_revenue_by_day') }}
  )
ORDER BY
  revenue DESC
LIMIT {{ var('top_waiters:limit') }}
