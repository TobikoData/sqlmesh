MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  pre NOOP(x=1),
  post (noop(), noop(y=['a', 2])),
);

SELECT DISTINCT
  customer_id::INT AS customer_id
FROM sushi.orders AS o
