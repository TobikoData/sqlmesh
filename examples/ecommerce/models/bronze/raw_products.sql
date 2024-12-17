MODEL (
  name ecommerce.bronze.raw_products,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [product_id],
  tags ['bronze'],
  columns (
    product_id INTEGER,
    sku TEXT,
    product_name TEXT,
    description TEXT,
    category_id INTEGER,
    supplier_id INTEGER,
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);

SELECT
  id as product_id,
  sku,
  name as product_name,
  description,
  category_id,
  supplier_id,
  unit_price,
  stock_quantity,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_products
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
