MODEL (
  name gold.product_performance_metrics,
  kind FULL,
  grain [product_id],
  tags ['gold'],
  references [silver.products, silver.order_items, silver.product_categories]
);

SELECT
  p.product_id,
  p.product_name,
  c.category_name,
  COUNT(DISTINCT oi.order_id) AS number_of_orders,
  SUM(oi.quantity) AS units_sold,
  SUM(oi.total_item_amount) AS total_revenue,
  AVG(oi.unit_price) AS average_selling_price,
  p.stock_quantity AS current_stock_level,
  CASE
    WHEN p.stock_quantity = 0
    THEN 'Out of Stock'
    WHEN p.stock_quantity < 10
    THEN 'Low Stock'
    ELSE 'In Stock'
  END AS inventory_status
FROM silver.products AS p
LEFT JOIN silver.order_items AS oi
  ON p.product_id = oi.product_id
LEFT JOIN silver.product_categories AS c
  ON p.category_id = c.category_id
GROUP BY
  1,
  2,
  3,
  8