MODEL (
  name gold.supplier_performance_metrics,
  kind FULL,
  grain [supplier_id],
  tags ['gold'],
  references [silver.suppliers, silver.products, silver.order_items]
);

SELECT
  s.supplier_id,
  s.company_name,
  COUNT(DISTINCT p.product_id) AS number_of_products,
  SUM(p.stock_quantity) AS total_stock_quantity,
  COUNT(DISTINCT CASE WHEN p.stock_quantity = 0 THEN p.product_id END) AS out_of_stock_products,
  COUNT(DISTINCT oi.order_id) AS number_of_orders,
  SUM(oi.quantity) AS total_units_sold,
  SUM(oi.total_item_amount) AS total_revenue,
  s.country AS supplier_country,
  COUNT(DISTINCT p.category_id) AS number_of_categories_supplied
FROM silver.suppliers AS s
LEFT JOIN silver.products AS p
  ON s.supplier_id = p.supplier_id
LEFT JOIN silver.order_items AS oi
  ON p.product_id = oi.product_id
GROUP BY
  1,
  2,
  9