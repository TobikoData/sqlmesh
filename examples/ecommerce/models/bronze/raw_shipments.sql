MODEL (
  name bronze.raw_shipments,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at
  ),
  grain [shipment_id],
  tags ['bronze'],
  columns (
    shipment_id INT,
    order_id INT,
    tracking_number TEXT,
    status TEXT,
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  ),
  references [source_ecommerce.raw_shipments]
);

SELECT
  id AS shipment_id,
  order_id,
  tracking_number,
  status,
  estimated_delivery_date,
  actual_delivery_date,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_shipments
WHERE
  _loaded_at >= @start_date AND _loaded_at < @end_date