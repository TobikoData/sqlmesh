MODEL (
  name silver.suppliers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [supplier_id]
  ),
  tags ['silver'],
  references [bronze.raw_suppliers]
);

WITH latest_suppliers AS (
  SELECT
    *
  FROM bronze.raw_suppliers
  WHERE
    _loaded_at >= @start_date AND _loaded_at < @end_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY _loaded_at DESC) = 1
)
SELECT
  supplier_id,
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
  _loaded_at
FROM latest_suppliers