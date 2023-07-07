MODEL (
  name db.order_item_f,
  kind incremental_by_time_range (
    time_column (order_date),
    batch_size 200,
  ),
  dialect "",
  cron '@daily',
  owner jen,
  start '2022-06-01 00:00:00+00:00',
);

SELECT
  oid.id AS order_id,
  customer_id AS customer_id,
  oid.item_id AS item_id,
  quantity AS quantity,
  (
    id.item_price * quantity
  ) AS order_amount,
  order_date AS order_date
FROM src.order_item_details AS oid
INNER JOIN db.item_d AS id
  ON oid.item_id = id.item_id
WHERE
  order_date BETWEEN @start_ds AND @end_ds
