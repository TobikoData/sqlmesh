MODEL (
  name bronze.raw_customer_addresses,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [address_id],
  tags ['bronze'],
  columns (
    address_id INT,
    customer_id INT,
    address_type TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    is_default BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_customer_addresses]
);

SELECT
  id AS address_id,
  customer_id,
  address_type,
  street_address,
  city,
  state,
  postal_code,
  country,
  is_default,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_customer_addresses
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date