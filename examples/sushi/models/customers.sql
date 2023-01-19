MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily'
);

SELECT DISTINCT
  customer_id::INT AS customer_id
FROM sushi.orders AS o
