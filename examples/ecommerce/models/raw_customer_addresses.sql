MODEL (
  name source_ecommerce.raw_customer_addresses,
  kind SEED (
    path '../seeds/source_customer_addresses.csv'
  ),
  columns (
    id INTEGER,
    customer_id INTEGER,
    address_type TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    is_default BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
