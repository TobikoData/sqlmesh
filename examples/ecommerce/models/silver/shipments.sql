MODEL (
  name silver.shipments,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [shipment_id]
  ),
  tags ['silver'],
  grain [shipment_id],
  references [bronze.raw_shipments]
);

WITH latest_shipments AS (
  SELECT
    *
  FROM bronze.raw_shipments
  WHERE
    _loaded_at >= @start_date AND _loaded_at < @end_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY _loaded_at DESC) = 1
)
SELECT
  shipment_id,
  order_id,
  tracking_number,
  status,
  estimated_delivery_date,
  actual_delivery_date,
  DATE_DIFF('DAY', created_at, COALESCE(actual_delivery_date, CURRENT_TIMESTAMP)) AS delivery_days,
  CASE
    WHEN NOT actual_delivery_date IS NULL
    AND actual_delivery_date <= estimated_delivery_date
    THEN TRUE
    ELSE FALSE
  END AS is_on_time,
  created_at,
  updated_at,
  _loaded_at
FROM latest_shipments