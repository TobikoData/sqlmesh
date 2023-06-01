/*
    Table of lifetime customer revenue.
    Date is available to get lifetime value up to a certain date.
    Use latest date to get current lifetime value.
*/
MODEL (
  name sushi.customer_revenue_lifetime,
  kind incremental_by_time_range (
    time_column (ds, 'YYYY-MM-dd'),
    batch_size 1
  ),
  owner jen,
  cron '@daily',
  dialect hive,
  columns (
    customer_id INT,
    revenue DOUBLE,
    ds STRING
  ),
  grain [customer_id, ds],
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total
  FROM sushi.order_items AS oi
  LEFT JOIN sushi.items AS i
    ON oi.item_id = i.id AND oi.ds = i.ds
  WHERE
    oi.ds = @end_ds
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
    o.ds = @end_ds
  GROUP BY
    o.customer_id
), prev_total AS (
  SELECT
    crl.customer_id,
    crl.revenue
  FROM sushi.customer_revenue_lifetime AS crl
  WHERE
    crl.ds = DATE_FORMAT(@end_date - INTERVAL 1 DAY, 'YYYY-MM-dd')
)
SELECT
  COALESCE(it.customer_id, prev_total.customer_id) AS customer_id, /* Customer id */
  COALESCE(it.revenue, 0) + COALESCE(prev_total.revenue, 0) AS revenue, /* Lifetime revenue from this customer */
  @end_ds AS ds /* End date of the lifetime calculation */
FROM incremental_total AS it
FULL OUTER JOIN prev_total AS prev_total
  ON it.customer_id = prev_total.customer_id
