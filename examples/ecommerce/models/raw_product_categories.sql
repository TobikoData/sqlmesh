MODEL (
  name source_ecommerce.raw_product_categories,
  kind SEED (
    path '../seeds/source_product_categories.csv'
  ),
  columns (
    id INTEGER,
    name TEXT,
    parent_category_id INTEGER,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
