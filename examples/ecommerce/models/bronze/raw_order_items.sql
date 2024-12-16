MODEL (
  name ecommerce.bronze.raw_order_items,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_timestamp
  ),
  grain [order_item_id],
  tags ['bronze']
);

SELECT
  id as order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  created_at as order_timestamp,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_order_items
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
