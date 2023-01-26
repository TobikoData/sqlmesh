AUDIT (
  name assert_items_name_not_null
);
SELECT *
FROM @this_model
WHERE name IS NULL;


AUDIT (
  name assert_items_price_exceeds_threshold
);
SELECT *
FROM @this_model
WHERE price <= @price;

