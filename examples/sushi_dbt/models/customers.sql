SELECT DISTINCT
  customer_id::INT AS customer_id
FROM {{ ref('orders') }} as o
