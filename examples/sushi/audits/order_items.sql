AUDIT (
  name assert_order_items_quantity_exceeds_threshold
);

SELECT *
FROM @this_model
WHERE quantity <= @quantity
