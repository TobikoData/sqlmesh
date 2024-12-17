MODEL (
  name bronze.raw_products,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [product_id],
  tags ['bronze'],
  columns (
    product_id INT,
    sku TEXT,
    product_name TEXT,
    description TEXT,
    category_id INT,
    supplier_id INT,
    unit_price DECIMAL(10, 2),
    stock_quantity INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_products]
);

SELECT
  id AS product_id,
  sku,
  name AS product_name,
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
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date