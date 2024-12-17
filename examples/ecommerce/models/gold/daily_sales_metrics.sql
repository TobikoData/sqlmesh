MODEL (
  name gold.daily_sales_metrics,
  kind FULL,
  grain [date],
  tags ['gold'],
  references [silver.orders, silver.order_items]
);

WITH daily_orders AS (
  SELECT
    order_timestamp::DATE AS date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    SUM(CASE WHEN is_completed THEN 1 ELSE 0 END) AS completed_orders
  FROM silver.orders
  GROUP BY
    1
), daily_items AS (
  SELECT
    order_timestamp::DATE AS date,
    COUNT(DISTINCT product_id) AS unique_products_sold,
    SUM(quantity) AS total_items_sold,
    AVG(unit_price) AS avg_unit_price
  FROM silver.order_items
  GROUP BY
    1
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
  o.total_revenue / NULLIF(o.total_orders, 0) AS avg_order_value,
  o.completed_orders::REAL / NULLIF(o.total_orders, 0) AS order_completion_rate
FROM daily_orders AS o
JOIN daily_items AS i
  ON o.date = i.date