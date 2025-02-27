MODEL (
  name sushi.waiter_names,
  kind SEED (
    path '../seeds/waiter_names.csv',
    batch_size 5
  ),
  owner jen,
  grain id,
  description 'List of waiter names',
  audits (
    assert_positive_id,
    does_not_exceed_threshold(column := id, threshold := @waiter_names_threshold()),
    assert_valid_name,
  )
);

AUDIT (
  name does_not_exceed_threshold,
);
SELECT * FROM @this_model WHERE @column >= @threshold;

AUDIT (
  name assert_positive_id,
);
SELECT * FROM @this_model WHERE id < 0;

AUDIT (
  name assert_valid_name,
);
SELECT * FROM @this_model WHERE LENGTH(name) < 1;
