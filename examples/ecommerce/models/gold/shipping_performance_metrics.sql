MODEL (
  name ecommerce.gold.shipping_performance_metrics,
  kind FULL,
  grain [carrier_id],
  references [ecommerce.silver.shipments, ecommerce.silver.customer_addresses]
);

SELECT
  s.carrier_id,
  COUNT(s.shipment_id) as total_shipments,
  COUNT(CASE WHEN s.status = 'delivered' THEN 1 END) as delivered_shipments,
  COUNT(CASE WHEN s.is_on_time THEN 1 END) as on_time_deliveries,
  AVG(s.delivery_days) as average_delivery_days,
  COUNT(CASE WHEN s.is_on_time THEN 1 END)::FLOAT / NULLIF(COUNT(s.shipment_id), 0) as on_time_delivery_rate,
  COUNT(CASE WHEN s.status = 'delivered' THEN 1 END)::FLOAT / NULLIF(COUNT(s.shipment_id), 0) as delivery_success_rate,
  COUNT(DISTINCT a.postal_code) as unique_delivery_locations,
  COUNT(DISTINCT a.country) as countries_served,
  MIN(s.delivery_days) as fastest_delivery_days,
  MAX(s.delivery_days) as longest_delivery_days,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY s.delivery_days) as p95_delivery_days
FROM ecommerce.silver.shipments s
LEFT JOIN ecommerce.silver.customer_addresses a
  ON s.shipping_address_id = a.address_id
GROUP BY 1
