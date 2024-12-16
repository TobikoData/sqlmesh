MODEL (
  name ecommerce.silver.customers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [customer_id]
  ),
  references [ecommerce.bronze.raw_customers]
);

WITH latest_customers AS (
  SELECT *
  FROM ecommerce.bronze.raw_customers
  WHERE _loaded_at >= @start_date
    AND _loaded_at < @end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id 
    ORDER BY _loaded_at DESC
  ) = 1
)

SELECT
  customer_id,
  email,
  first_name,
  last_name,
  phone_number,
  address,
  city,
  state,
  postal_code,
  country,
  created_at,
  updated_at,
  _loaded_at
FROM latest_customers
