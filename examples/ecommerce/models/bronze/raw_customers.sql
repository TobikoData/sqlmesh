MODEL (
  name ecommerce.bronze.raw_customers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [customer_id],
  tags ['bronze'],
  columns (
    customer_id INTEGER,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
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
