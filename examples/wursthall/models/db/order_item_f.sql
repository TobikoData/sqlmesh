MODEL (
  name db.order_item_f,
  kind incremental_by_time_range (
    time_column (order_ds, '%Y-%m-%d'),
    batch_size 200,
  ),
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
  order_ds AS order_ds
FROM src.order_item_details AS oid
INNER JOIN db.item_d AS id
  ON oid.item_id = id.item_id
WHERE
  order_ds BETWEEN @start_ds AND @end_ds
