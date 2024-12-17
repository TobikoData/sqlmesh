MODEL (
  name silver.product_categories,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [category_id]
  ),
  tags ['silver'],
  references [bronze.raw_product_categories]
);

WITH latest_categories AS (
  SELECT
    *
  FROM bronze.raw_product_categories
  WHERE
    _loaded_at >= @start_date AND _loaded_at < @end_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY _loaded_at DESC) = 1
)
SELECT
  category_id,
  category_name,
  description,
  parent_category_id,
  created_at,
  updated_at,
  _loaded_at
FROM latest_categories