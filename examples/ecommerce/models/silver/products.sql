MODEL (
  name ecommerce.silver.products,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [product_id]
  ),
  tags ['silver'],
  references [ecommerce.bronze.raw_products]
);

WITH latest_products AS (
  SELECT *
  FROM ecommerce.bronze.raw_products
  WHERE _loaded_at >= @start_date
    AND _loaded_at < @end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id 
    ORDER BY _loaded_at DESC
  ) = 1
)

SELECT
  product_id,
  sku,
  product_name,
  description,
  category_id,
  supplier_id,
  unit_price,
  stock_quantity,
  CASE 
    WHEN stock_quantity > 0 THEN true
    ELSE false
  END as is_in_stock,
  created_at,
  updated_at,
  _loaded_at
FROM latest_products
