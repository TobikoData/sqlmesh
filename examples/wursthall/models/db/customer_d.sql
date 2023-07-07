MODEL (
  name db.customer_d,
  kind incremental_by_time_range (
    time_column (register_date),
    batch_size 200,
  ),
  dialect "",
  cron '@daily',
  owner jen,
  start '2022-06-01 00:00:00+00:00',
);

SELECT DISTINCT
  id AS customer_id,
  @mask(name) AS name,
  @mask(email) AS email,
  @mask(phone) AS phone,
  register_date AS register_date
FROM src.customer_details
WHERE
  register_date BETWEEN @start_ds AND @end_ds
