MODEL (
  name sushi.customers,
  kind FULL,
  owner jen,
  cron '@daily',
  tags (pii, fact),
  grain customer_id,
  description 'Sushi customer data',
  column_descriptions (
    customer_id = 'customer_id uniquely identifies customers'
  )
);

CREATE DATABASE IF NOT EXISTS raw;
DROP VIEW IF EXISTS raw.demographics;
CREATE VIEW raw.demographics AS (
  SELECT 1 AS customer_id, '00000' AS zip
);

SELECT DISTINCT
  o.customer_id::INT AS customer_id, -- this comment should not be registered
  'active' AS status,
  d.zip
  FROM sushi.orders AS o
LEFT JOIN raw.demographics AS d
  ON o.customer_id = d.customer_id
