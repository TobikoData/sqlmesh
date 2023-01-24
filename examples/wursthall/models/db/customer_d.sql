MODEL (
  name db.customer_d,
  kind incremental_by_time_range (
    time_column (register_ds, '%Y-%m-%d')
  ),
  dialect "",
  cron '@daily',
  owner jen,
  start '2022-06-01 00:00:00+00:00',
  batch_size 200
);

SELECT DISTINCT
  id AS customer_id,
  @mask(name) AS name,
  @mask(email) AS email,
  @mask(phone) AS phone,
  register_ds AS register_ds
FROM src.customer_details
WHERE
  register_ds BETWEEN @start_ds AND @end_ds