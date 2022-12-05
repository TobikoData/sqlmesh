/* Join table of sushi orders and items. */
MODEL (
  name sushi.order_items,
  owner jen,
  cron '@daily',
  batch_size 30,
  time_column ds
);

SELECT
  id::INT AS id, /* Primary key */
  order_id::INT AS order_id, /* Order id */
  item_id::INT AS item_id, /* Item id */
  quantity::INT AS quantity, /* Quantity of items ordered */
  ds::TEXT AS ds /* Date of order */
FROM raw.order_items
WHERE
  ds BETWEEN @start_ds AND @end_ds
