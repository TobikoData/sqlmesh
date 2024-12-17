MODEL (
  name source_ecommerce.raw_products,
  kind SEED (
    path '../seeds/source_products.csv'
  ),
  columns (
    id INTEGER,
    sku TEXT,
    name TEXT,
    description TEXT,
    category_id INTEGER,
    supplier_id INTEGER,
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
