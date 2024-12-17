MODEL (
  name source_ecommerce.raw_suppliers,
  kind SEED (
    path '../seeds/source_suppliers.csv'
  ),
  columns (
    id INTEGER,
    company_name TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP,
    _file_name TEXT
  )
);
