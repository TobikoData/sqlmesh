/*
    Table of lifetime customer revenue.
    Date is available to get lifetime value up to a certain date.
    Use latest date to get current lifetime value.
*/
MODEL (
  name sushi.customer_revenue_lifetime,
  kind incremental_by_time_range (
    time_column event_date,
    batch_size 1
  ),
  owner jen,
  cron '@daily',
  dialect hive,
  tags expensive,
  columns (
    customer_id INT,
    revenue DOUBLE,
    event_date DATE
  ),
  grain (customer_id, event_date),
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total
  FROM sushi.order_items AS oi
  LEFT JOIN sushi.items AS i
    ON oi.item_id = i.id AND oi.event_date = i.event_date
  WHERE
    oi.event_date = @end_date
  GROUP BY
    oi.order_id
), incremental_total AS (
  SELECT
    o.customer_id::INT AS customer_id,
    SUM(ot.total)::DOUBLE AS revenue,
  FROM sushi.orders AS o
  LEFT JOIN order_total AS ot
    ON o.id = ot.order_id
  WHERE
    o.event_date = @end_date
  GROUP BY
    o.customer_id
), prev_total AS (
  SELECT
    crl.customer_id,
    crl.revenue
  FROM sushi.customer_revenue_lifetime AS crl
  WHERE
    crl.event_date = @end_date - INTERVAL 1 DAY
)
SELECT
  COALESCE(it.customer_id, prev_total.customer_id) AS customer_id, /* Customer id */
  COALESCE(it.revenue, 0) + COALESCE(prev_total.revenue, 0) AS revenue, /* Lifetime revenue from this customer */
  @end_date AS event_date /* End date of the lifetime calculation */
FROM incremental_total AS it
FULL OUTER JOIN prev_total AS prev_total
  ON it.customer_id = prev_total.customer_id
