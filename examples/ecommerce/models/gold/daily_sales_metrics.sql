MODEL (
  name ecommerce.gold.daily_sales_metrics,
  kind FULL,
  grain [date],
  tags ['gold'],
  references [ecommerce.silver.orders, ecommerce.silver.order_items]
);

WITH daily_orders AS (
  SELECT
    DATE(order_timestamp) as date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT user_id) as unique_customers,
    SUM(total_amount) as total_revenue,
    SUM(CASE WHEN is_completed THEN 1 ELSE 0 END) as completed_orders
  FROM ecommerce.silver.orders
  GROUP BY 1
),
daily_items AS (
  SELECT
    DATE(order_timestamp) as date,
    COUNT(DISTINCT product_id) as unique_products_sold,
    SUM(quantity) as total_items_sold,
    AVG(unit_price) as avg_unit_price
  FROM ecommerce.silver.order_items
  GROUP BY 1
)
SELECT
  o.date,
  o.total_orders,
  o.unique_customers,
  o.total_revenue,
  o.completed_orders,
  i.unique_products_sold,
  i.total_items_sold,
  i.avg_unit_price,
  o.total_revenue / NULLIF(o.total_orders, 0) as avg_order_value,
  o.completed_orders::FLOAT / NULLIF(o.total_orders, 0) as order_completion_rate
FROM daily_orders o
JOIN daily_items i ON o.date = i.date
