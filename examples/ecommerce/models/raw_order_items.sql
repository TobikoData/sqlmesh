MODEL (
  name source_ecommerce.raw_order_items,
  kind SEED (
    path '../seeds/source_order_items.csv'
  ),
  columns (
    id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
