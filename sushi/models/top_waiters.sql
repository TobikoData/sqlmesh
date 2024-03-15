/* View of top waiters. */
MODEL (
  name sushi.top_waiters,
  owner jen,
  audits (
    unique_values(columns=(waiter_id))
  ),
  grain waiter_id
);

SELECT
  waiter_id::INT AS waiter_id,
  revenue::DOUBLE AS revenue
FROM sushi.waiter_revenue_by_day
WHERE
  event_date = (
    SELECT
      MAX(event_date)
    FROM sushi.waiter_revenue_by_day
  )
ORDER BY
  revenue DESC
LIMIT 10
