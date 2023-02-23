SELECT DISTINCT
  customer_id::INT AS {{ var('customer:customer_id') }}
FROM {{ source('raw', 'orders') }}
