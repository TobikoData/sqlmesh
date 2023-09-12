# Overview
SQLMesh provides a framework for defining and working with metrics (also known as a semantic layer). Metrics are arbitrary functions that perform aggregations for use in analytics, data science, or machine learning.

A semantic layer is valuable because it provides a consistent definition and language for metrics. For example, if an executive asks "How many active users are there?" depending on who the ask or what dashboard they look at, the number could be different. The tables, aggregations, joins, could be complex and duplicated across various tools and scratch pads.

## Examples
### Definition
SQLMesh introduces a SQL based [metrics definition language](definition.md) and querying api.

The following is an example of a metric defined in SQL. Note the following aspects:

- The expression can be any aggregate SQL function
- The referenced columns are fully qualified
- You can reference multiple tables as long as references / grains are properly setup.

```sql linenums="1"
METRIC (
  name total_orders_from_active_customers,
  owner jen,
  expression COUNT(IF(sushi.customers.status = 'ACTIVE', sushi.orders.id, NULL))
);
```

### Querying
SQLMesh uses a SQL interface to query metrics. The METRIC aggregate function is used to indicate computation of a metric.

```sql linenums="1"
-- Input query
SELECT
  ds,
  METRIC(total_orders),  -- magic agg function
  METRIC(items_per_order),
  METRIC(total_orders_from_active_customers),
FROM __semantic.__table  -- special table for simple queries.
GROUP BY ds

-- Rewritten query
SELECT
  __table.ds AS ds,
  __table.total_orders AS total_orders,
  __table.total_ordered_items / __table.total_orders AS items_per_order,
  __table.total_orders_from_active_customers AS total_orders_from_active_customers
FROM (
  SELECT
    sushi__orders.ds,
    COUNT(IF(sushi__customers.status = 'ACTIVE', sushi__orders.id, NULL)) AS total_orders_from_active_customers,
    COUNT(sushi__orders.id) AS total_orders
  FROM sushi.orders AS sushi__orders
  LEFT JOIN sushi.customers AS sushi__customers
    ON sushi__orders.customer_id = sushi__customers.customer_id
  GROUP BY
    sushi__orders.ds
) AS __table
FULL JOIN (
  SELECT
    sushi__order_items.ds,
    SUM(sushi__order_items.quantity) AS total_ordered_items
  FROM sushi.order_items AS sushi__order_items
  GROUP BY
    sushi__order_items.ds
) AS sushi__order_items
  ON __table.ds = sushi__order_items.ds
```
