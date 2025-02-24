MODEL (
  name sushi.latest_order,
  kind CUSTOM (
    materialization 'custom_full_with_custom_kind',
    custom_property 'sushi!!!'
  ),
  cron '@daily'
);

SELECT id, customer_id, start_ts, end_ts, event_date
FROM sushi.orders
ORDER BY event_date DESC LIMIT 1

