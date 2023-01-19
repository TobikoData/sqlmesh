MODEL (
  name sushi.waiters,
  kind EMBEDDED,
  owner jen,
  cron '@daily',
);

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM sushi.orders AS o
WHERE
  ds BETWEEN @start_ds AND @end_ds
