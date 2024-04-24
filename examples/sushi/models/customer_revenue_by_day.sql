/* Table of revenue from customers by day. */
MODEL (
  name sushi.customer_revenue_by_day,
  kind incremental_by_time_range (
    time_column event_date,
    batch_size 10,
  ),
  owner jen,
  cron '@daily',
  dialect hive,
  tags expensive,
  grain (customer_id, event_date),
);

WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.event_date AS event_date
  FROM sushi.order_items AS oi
  LEFT JOIN sushi.items AS i
    ON oi.item_id = i.id AND oi.event_date = i.event_date
  WHERE
    oi.event_date BETWEEN CAST('{{ start_ds }}' as DATE) AND CAST('{{ end_ds }}' as DATE)
  GROUP BY
    oi.order_id,
    oi.event_date
)
SELECT
  o.customer_id::INT AS customer_id, /* Customer id */
  SUM(ot.total)::DOUBLE AS revenue, /* Revenue from orders made by this customer */
  MAX(0) AS "country code", /* Customer country code, used for testing spaces */
  o.event_date::DATE AS event_date /* Date */
FROM sushi.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.event_date = ot.event_date
WHERE
  o.event_date BETWEEN CAST('{{ start_ds }}' as DATE) AND CAST('{{ end_ds }}' as DATE)
GROUP BY
  o.customer_id,
  o.event_date
