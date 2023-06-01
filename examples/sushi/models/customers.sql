MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  tags (pii, fact),
  grain customer_id,
);

CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.marketing AS (
  SELECT 1 AS customer_id, 'active' AS status
);
CREATE TABLE IF NOT EXISTS raw.demographics AS (
  SELECT 1 AS customer_id, '00000' AS zip
);

WITH distinct_marketing AS (
  SELECT DISTINCT
    customer_id,
    status
  FROM raw.marketing
)
SELECT DISTINCT
  o.customer_id::INT AS customer_id,
  COALESCE(m.status, 'UNKNOWN') AS status,
  d.zip
FROM sushi.orders AS o
LEFT JOIN distinct_marketing AS m
    ON o.customer_id = m.customer_id
LEFT JOIN raw.demographics AS d
    ON o.customer_id = d.customer_id
