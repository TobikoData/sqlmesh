SELECT DISTINCT
  customer_id::INT AS customer_id
FROM {{ source('raw', 'orders') }}