MODEL (
  name ecommerce.bronze.raw_shipments,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column created_at
  ),
  grain [shipment_id],
  tags ['bronze'],
  columns (
    shipment_id INTEGER,
    order_id INTEGER,
    carrier_id INTEGER,
    tracking_number TEXT,
    status TEXT,
    shipping_address_id INTEGER,
    estimated_delivery_date TIMESTAMP,
    actual_delivery_date TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);

SELECT
  id as shipment_id,
  order_id,
  carrier_id,
  tracking_number,
  status,
  shipping_address_id,
  estimated_delivery_date,
  actual_delivery_date,
  created_at,
  updated_at,
  _loaded_at,
  _file_name
FROM source_ecommerce.raw_shipments
WHERE _loaded_at >= @start_date
  AND _loaded_at < @end_date
