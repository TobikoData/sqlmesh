MODEL (
  name ecommerce.bronze.raw_customers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [customer_id],
  tags ['bronze']
);

SELECT
  id as customer_id,
  email,
  first_name,
  last_name,
  phone_number,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_customers
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
