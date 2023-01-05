
MODEL (
  name sushi.waiter_as_customer_by_day,
  owner jen,
  cron '@daily'
);

SELECT
  w.ds as ds,
  w.waiter_id as waiter_id
FROM sushi.waiters AS w
JOIN sushi.customers as c ON w.waiter_id = c.customer_id
