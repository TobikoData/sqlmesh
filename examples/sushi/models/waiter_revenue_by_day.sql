/* Table of revenue generated by waiters by day. */
MODEL (
  name sushi.waiter_revenue_by_day,
  kind incremental_by_time_range (
    time_column (ds, '%Y-%m-%d'),
    batch_size 10,
  ),
  start '1 week ago',
  owner jen,
  cron '@daily',
  audits (
    NUMBER_OF_ROWS(threshold=0)
  ),
  grain (waiter_id, ds)
);

SELECT
  o.waiter_id::INT AS waiter_id, /* Waiter id */
  SUM(oi.quantity * i.price)::DOUBLE AS revenue, /* Revenue from orders taken by this waiter */
  o.ds::TEXT AS ds /* Date */
FROM sushi.orders AS o
LEFT JOIN sushi.order_items AS oi
  ON o.id = oi.order_id AND o.ds = oi.ds
LEFT JOIN sushi.items AS i
  ON oi.item_id = i.id AND oi.ds = i.ds
WHERE
  o.ds BETWEEN @start_ds AND @end_ds
GROUP BY
  o.waiter_id,
  o.ds
