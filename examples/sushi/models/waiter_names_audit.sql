MODEL (
  name sushi.waiter_names_audit,
  audits (
    assert_positive_id,
    does_not_exceed_threshold(column := waiter_id, threshold := 200),
    assert_valid_name,
  )
);

SELECT
  w.id as waiter_id,
  w.name as waiter_name,
FROM sushi.waiter_names as w;

AUDIT (
name does_not_exceed_threshold,
);
SELECT * FROM @this_model
WHERE @column >= @threshold;

AUDIT (
name assert_positive_id,
);
SELECT *
FROM @this_model
WHERE
waiter_id < 0;

AUDIT (
name assert_valid_name,
);
SELECT *
FROM @this_model
WHERE
LENGTH(waiter_name) < 1;