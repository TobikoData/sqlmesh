MODEL (
  name ecommerce.silver.orders,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [order_id]
  ),
  tags ['silver'],
  references [ecommerce.bronze.raw_orders]
);

WITH latest_orders AS (
  SELECT *
  FROM ecommerce.bronze.raw_orders
  WHERE _loaded_at >= @start_date
    AND _loaded_at < @end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY _loaded_at DESC
  ) = 1
)

SELECT
  order_id,
  user_id,
  total_amount,
  status,
  order_timestamp,
  CASE 
    WHEN status = 'completed' THEN true
    ELSE false
  END as is_completed,
  updated_at,
  _loaded_at
FROM latest_orders
