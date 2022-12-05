MODEL (
  name sushi.waiters,
  owner jen,
  cron '@daily',
  batch_size 30,
  time_column ds
);

SELECT DISTINCT
  waiter_id::INT AS waiter_id,
  ds::TEXT AS ds
FROM sushi.orders AS o
WHERE
  ds BETWEEN @start_ds AND @end_ds
