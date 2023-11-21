/* Table of revenue from customers by day. */
MODEL (
  name sushi.customer_revenue_by_day,
  kind incremental_by_time_range (
    time_column date,
    batch_size 10,
  ),
  owner jen,
  cron '@daily',
  dialect hive,
  tags expensive,
  grain [customer_id, date],
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.date AS date
  FROM sushi.order_items AS oi
  LEFT JOIN sushi.items AS i
    ON oi.item_id = i.id AND oi.date = i.date
  WHERE
    oi.date BETWEEN @start_date AND @end_date
  GROUP BY
    oi.order_id,
    oi.date
)
SELECT
  o.customer_id::INT AS customer_id, /* Customer id */
  SUM(ot.total)::DOUBLE AS revenue, /* Revenue from orders made by this customer */
  o.date::DATE AS date /* Date */
FROM sushi.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.date = ot.date
WHERE
  o.date BETWEEN @start_date AND @end_date
GROUP BY
  o.customer_id,
  o.date
