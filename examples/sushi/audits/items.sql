AUDIT (
  name assert_items_name_not_null
);
SELECT *
FROM @this_model
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  name IS NULL;


AUDIT (
  name assert_items_price_exceeds_threshold
);
SELECT *
FROM @this_model
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  price <= @price;

