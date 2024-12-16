MODEL (
  name ecommerce.gold.customer_metrics,
  kind FULL,
  grain [customer_id],
  references [ecommerce.silver.customers, ecommerce.silver.orders]
);

SELECT
  c.customer_id,
  c.full_name,
  c.email,
  COUNT(DISTINCT o.order_id) as total_orders,
  SUM(o.total_amount) as total_spend,
  AVG(o.total_amount) as average_order_value,
  MIN(o.order_timestamp) as first_order_date,
  MAX(o.order_timestamp) as last_order_date,
  DATEDIFF('day', MIN(o.order_timestamp), MAX(o.order_timestamp)) as customer_lifetime_days,
  c.default_city,
  c.default_state,
  c.default_country
FROM ecommerce.silver.customers c
LEFT JOIN ecommerce.silver.orders o
  ON c.customer_id = o.user_id
GROUP BY 1, 2, 3, 10, 11, 12
