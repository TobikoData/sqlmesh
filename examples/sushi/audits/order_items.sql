AUDIT (
  name assert_order_items_quantity_exceeds_threshold
);

SELECT *
FROM @this_model
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  quantity <= @quantity
