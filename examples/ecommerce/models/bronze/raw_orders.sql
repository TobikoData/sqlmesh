MODEL (
  name bronze.raw_orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_timestamp
  ),
  grain [order_id],
  tags ['bronze'],
  columns (
    order_id INT,
    user_id INT,
    total_amount DECIMAL(10, 2),
    status TEXT,
    order_timestamp TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_orders]
);

SELECT
  id AS order_id,
  user_id,
  total_amount,
  status,
  created_at AS order_timestamp,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_orders
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date