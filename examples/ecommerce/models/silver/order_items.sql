MODEL (
  name silver.order_items,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [order_item_id]
  ),
  tags ['silver'],
  references [bronze.raw_order_items]
);

WITH latest_order_items AS (
  SELECT
    *
  FROM bronze.raw_order_items
  WHERE
    _loaded_at >= @start_date AND _loaded_at < @end_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY _loaded_at DESC) = 1
)
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  quantity * unit_price AS total_item_amount,
  order_timestamp,
  _loaded_at
FROM latest_order_items