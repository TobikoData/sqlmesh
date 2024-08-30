MODEL (
  name sushi.marketing,
  kind SCD_TYPE_2(unique_key customer_id),
  owner jen,
  cron '@daily',
  grain customer_id,
  description 'Sushi marketing data'
);

SELECT
  customer_id::INT AS customer_id, -- customer_id uniquely identifies customers \
  status::TEXT AS status,
  updated_at::TIMESTAMP AS updated_at
FROM
  sushi.raw_marketing;

@assert_has_columns(
    sushi.marketing,
    {
        customer_id: 'int',
        status: 'text',
        updated_at: 'timestamp',
        valid_from: 'timestamp',
        valid_to: 'timestamp',
    }
)
