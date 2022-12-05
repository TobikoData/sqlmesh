AUDIT (
  name assert_items_name_not_null,
  model sushi.items
);
SELECT *
FROM sushi.items
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  name IS NULL;


AUDIT (
  name assert_items_price_positive,
  model sushi.items
);
SELECT *
FROM sushi.items
WHERE
  ds BETWEEN @start_ds AND @end_ds AND
  price <= 0;

