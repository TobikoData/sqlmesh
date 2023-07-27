METRIC (
  name total_ordered_items,
  owner jen,
  expression SUM(sushi.order_items.quantity)
);

METRIC (
  name total_orders,
  owner jen,
  expression COUNT(sushi.orders.id)
);

METRIC (
  name items_per_order,
  owner jen,
  expression total_ordered_items / total_orders
);
