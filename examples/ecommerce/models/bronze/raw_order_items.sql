MODEL (
  name bronze.raw_order_items,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_timestamp
  ),
  grain [order_item_id],
  tags ['bronze'],
  columns (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    order_timestamp TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_order_items]
);

SELECT
  id AS order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  created_at AS order_timestamp,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_order_items
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date