MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  tags (pii, fact),
  grain customer_id,
);

CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.demographics AS (
  SELECT 1 AS customer_id, '00000' AS zip
);

WITH current_marketing AS (
  SELECT
    customer_id,
    status
  FROM sushi.marketing
  WHERE valid_to is null
)
SELECT DISTINCT
  o.customer_id::INT AS customer_id,
  m.status,
  d.zip
FROM sushi.orders AS o
LEFT JOIN current_marketing AS m
    ON o.customer_id = m.customer_id
LEFT JOIN raw.demographics AS d
    ON o.customer_id = d.customer_id
