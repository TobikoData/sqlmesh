SELECT DISTINCT
  customer_id::INT AS {{ var("customers:customer_id") }}
FROM {{ source('raw', 'orders') }}
