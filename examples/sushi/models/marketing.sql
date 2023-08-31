MODEL (
  name sushi.marketing,
  kind SCD_TYPE_2(unique_key customer_id),
  owner jen,
  cron '@daily',
  grain customer_id,
);

SELECT
    customer_id::INT AS customer_id,
    status::TEXT AS status,
    updated_at::TIMESTAMP AS updated_at
FROM
    sushi.raw_marketing

