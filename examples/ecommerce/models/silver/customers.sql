MODEL (
  name silver.customers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [customer_id]
  ),
  tags ['silver'],
  columns (
    customer_id INT,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    phone_number TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [bronze.raw_customers]
);

SELECT
  customer_id,
  email,
  first_name,
  last_name,
  first_name || ' ' || last_name AS full_name,
  phone_number,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM bronze.raw_customers