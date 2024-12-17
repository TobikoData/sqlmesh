MODEL (
  name source_ecommerce.raw_customers,
  kind SEED (
    path '../seeds/source_customers.csv'
  ),
  columns (
    id INTEGER,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
