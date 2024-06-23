MODEL (
  name sushi.active_customers,
  kind CUSTOM (
    materialization 'custom_full'
  ),
  owner jen,
  cron '@daily',
  tags (pii, fact),
  grain customer_id,
  description 'Sushi active customer data',
  column_descriptions (
    customer_id = 'customer_id uniquely identifies customers'
  )
);


SELECT customer_id, zip
FROM sushi.customers
WHERE status = 'active'
