/* Table of daily sushi prices. */
MODEL (
  name sushi.items,
  kind incremental,
  dialect duckdb,
  owner jen,
  cron '@daily',
  batch_size 30,
  time_column (ds, '%Y-%m-%d')
);

SELECT
  id::INT AS id, /* Primary key */
  name::TEXT AS name, /* Name of the sushi */
  price::DOUBLE AS price, /* Price of the sushi */
  ds::TEXT AS ds /* Date */
FROM raw.items
WHERE
  ds BETWEEN @start_ds AND @end_ds
