MODEL (
  name ecommerce.gold.supplier_performance_metrics,
  kind FULL,
  grain [supplier_id],
  tags ['gold'],
  references [ecommerce.silver.suppliers, ecommerce.silver.products, ecommerce.silver.order_items]
);

SELECT
  s.supplier_id,
  s.company_name,
  COUNT(DISTINCT p.product_id) as number_of_products,
  SUM(p.stock_quantity) as total_stock_quantity,
  COUNT(DISTINCT CASE WHEN p.stock_quantity = 0 THEN p.product_id END) as out_of_stock_products,
  COUNT(DISTINCT oi.order_id) as number_of_orders,
  SUM(oi.quantity) as total_units_sold,
  SUM(oi.total_item_amount) as total_revenue,
  s.country as supplier_country,
  COUNT(DISTINCT p.category_id) as number_of_categories_supplied
FROM ecommerce.silver.suppliers s
LEFT JOIN ecommerce.silver.products p
  ON s.supplier_id = p.supplier_id
LEFT JOIN ecommerce.silver.order_items oi
  ON p.product_id = oi.product_id
GROUP BY 1, 2, 9
