SELECT DISTINCT
  customer_id::INT AS customer_id
FROM {{ ref('raw_orders') }}
