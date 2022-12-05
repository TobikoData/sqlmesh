/* Table of revenue from customers by day. */
MODEL (
  name sushi.customer_revenue_by_day,
  owner jen,
  cron '@daily',
  batch_size 10,
  time_column ds
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.ds AS ds
  FROM sushi.order_items AS oi
  LEFT JOIN sushi.items AS i
    ON oi.item_id = i.id AND oi.ds = i.ds
  WHERE
    oi.ds BETWEEN @start_ds AND @end_ds
  GROUP BY
    oi.order_id,
    oi.ds
)
SELECT
  o.customer_id::INT AS customer_id, /* Customer id */
  SUM(ot.total)::DOUBLE AS revenue, /* Revenue from orders made by this customer */
  o.ds::TEXT AS ds /* Date */
FROM sushi.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.ds = ot.ds
WHERE
  o.ds BETWEEN @start_ds AND @end_ds
GROUP BY
  o.customer_id,
  o.ds
