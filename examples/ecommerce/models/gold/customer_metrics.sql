MODEL (
  name gold.customer_metrics,
  kind FULL,
  grain [customer_id],
  tags ['gold'],
  references [silver.customers, silver.orders, bronze.raw_customer_addresses]
);

SELECT
  c.customer_id,
  c.full_name,
  c.email,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.total_amount) AS total_spend,
  AVG(o.total_amount) AS average_order_value,
  MIN(o.order_timestamp) AS first_order_date,
  MAX(o.order_timestamp) AS last_order_date,
  DATE_DIFF('DAY', MIN(o.order_timestamp), MAX(o.order_timestamp)) AS customer_lifetime_days,
  ca.city,
  ca.state,
  ca.country
FROM silver.customers AS c
left join bronze.raw_customer_addresses as ca
  on c.customer_id = ca.customer_id
LEFT JOIN silver.orders AS o
  ON c.customer_id = o.user_id
GROUP BY
  1,
  2,
  3,
  10,
  11,
  12