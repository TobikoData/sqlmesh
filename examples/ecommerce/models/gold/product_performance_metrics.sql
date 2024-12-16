MODEL (
  name ecommerce.gold.product_performance_metrics,
  kind FULL,
  grain [product_id],
  references [ecommerce.silver.products, ecommerce.silver.order_items, ecommerce.silver.product_categories]
);

SELECT
  p.product_id,
  p.product_name,
  c.category_name,
  COUNT(DISTINCT oi.order_id) as number_of_orders,
  SUM(oi.quantity) as units_sold,
  SUM(oi.total_item_amount) as total_revenue,
  AVG(oi.unit_price) as average_selling_price,
  p.stock_quantity as current_stock_level,
  CASE 
    WHEN p.stock_quantity = 0 THEN 'Out of Stock'
    WHEN p.stock_quantity < 10 THEN 'Low Stock'
    ELSE 'In Stock'
  END as inventory_status
FROM ecommerce.silver.products p
LEFT JOIN ecommerce.silver.order_items oi
  ON p.product_id = oi.product_id
LEFT JOIN ecommerce.silver.product_categories c
  ON p.category_id = c.category_id
GROUP BY 1, 2, 3, 8
