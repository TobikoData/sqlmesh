MODEL (
  name ecommerce.bronze.raw_product_categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [category_id],
  tags ['bronze'],
  columns (
    category_id INTEGER,
    category_name TEXT,
    parent_category_id INTEGER,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);

SELECT
  id as category_id,
  name as category_name,
  parent_category_id,
  description,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_product_categories
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
