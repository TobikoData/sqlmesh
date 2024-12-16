MODEL (
  name ecommerce.bronze.raw_suppliers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [supplier_id],
  tags ['bronze']
);

SELECT
  id as supplier_id,
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
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
