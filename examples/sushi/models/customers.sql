MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  pre NOOP(x=1),
  post (noop(), noop(y=['a', 2])),
);

SELECT @SQL(@REDUCE([100, 200, 300, 400], (x,y) -> x + y));

SELECT DISTINCT
  customer_id::INT AS customer_id
FROM sushi.orders AS o
