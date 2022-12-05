AUDIT (
  name assert_order_items_quantity_positive,
  model sushi.order_items
);

SELECT *
FROM sushi.order_items
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  quantity < 1
