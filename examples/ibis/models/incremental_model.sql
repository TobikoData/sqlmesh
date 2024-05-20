MODEL (
  name ibis.incremental_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds
  ),
  start '2020-01-01',
  cron '@daily',
  grain [id, ds]
);

SELECT
  id,
  item_id,
  ds,
FROM
  ibis.seed_model
WHERE
  ds between @start_ds and @end_ds
