/* View of top waiters. */
MODEL (
  name sushi.top_waiters,
  kind VIEW,
  owner jen,
  audits (
    unique_keys(columns=['waiter_id'])
  )
);

SELECT
  waiter_id::INT AS waiter_id,
  revenue::DOUBLE AS revenue
FROM sushi.waiter_revenue_by_day
WHERE
  ds = (
    SELECT
      MAX(ds)
    FROM sushi.waiter_revenue_by_day
  )
ORDER BY
  revenue DESC
LIMIT 10
