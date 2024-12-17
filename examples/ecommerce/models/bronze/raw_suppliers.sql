MODEL (
  name bronze.raw_suppliers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [supplier_id],
  tags ['bronze'],
  columns (
    supplier_id INT,
    company_name TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_suppliers]
);

SELECT
  id AS supplier_id,
  company_name,
  contact_name,
  contact_email,
  contact_phone,
  address,
  city,
  state,
  postal_code,
  country,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_suppliers
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date