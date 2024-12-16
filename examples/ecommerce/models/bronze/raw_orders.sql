MODEL (
  name ecommerce.bronze.raw_orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_timestamp
  ),
  grain [order_id],
  tags ['bronze']
);

SELECT
  id as order_id,
  user_id,
  total_amount,
  status,
  created_at as order_timestamp,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_orders
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
