MODEL (
  name sushi.waiter_names,
  kind SEED (
    path '../seeds/waiter_names.csv',
    batch_size 100
  ),
  owner jen
)
