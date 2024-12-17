MODEL (
  name source_ecommerce.raw_orders,
  kind SEED (
    path '../seeds/source_orders.csv'
  ),
  columns (
    id INTEGER,
    user_id INTEGER,
    total_amount DECIMAL(10,2),
    status TEXT,
    created_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
