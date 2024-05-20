AUDIT (
  name assert_item_price_above_zero,
  description "Asserts that the price of an item is above zero.",
  standalone true
);

SELECT *
FROM sushi.items
WHERE price <= 0