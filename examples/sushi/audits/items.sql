AUDIT (
  name assert_items_price_exceeds_threshold
);
SELECT *
FROM @this_model
WHERE price <= @price;

