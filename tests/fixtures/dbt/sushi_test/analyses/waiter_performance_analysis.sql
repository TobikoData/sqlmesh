-- Simple analysis: Top performing waiters by total revenue

SELECT
  waiter_id,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS days_worked
FROM {{ ref('waiter_revenue_by_day') }}
GROUP BY waiter_id
ORDER BY total_revenue DESC
LIMIT 10