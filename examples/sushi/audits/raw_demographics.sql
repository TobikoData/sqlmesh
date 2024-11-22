AUDIT (
  name assert_raw_demographics
);
SELECT customer_id
FROM @this_model
WHERE customer_id <> 1
