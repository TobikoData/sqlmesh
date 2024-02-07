MODEL (
    name ibis.seed_model,
    kind SEED (
        path '../seeds/seed_data.csv'
    ),
    columns (
        id INTEGER,
        item_id INTEGER,
        ds TEXT
    ),
    grain [id, ds]
);
