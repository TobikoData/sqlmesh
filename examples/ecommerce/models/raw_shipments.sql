MODEL (
  name source_ecommerce.raw_shipments,
  kind SEED (
    path '../seeds/source_shipments.csv'
  ),
  columns (
    id INTEGER,
    order_id INTEGER,
    tracking_number TEXT,
    status TEXT,
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
