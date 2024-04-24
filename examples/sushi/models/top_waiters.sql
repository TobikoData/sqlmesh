/* View of top waiters. */
MODEL (
  name sushi.top_waiters,
  owner jen,
  audits (
    unique_values(columns=(waiter_id))
  ),
  grain waiter_id
);

WITH test_macros AS (
    SELECT
      @ADD_ONE(1) AS lit_two,
      @IS_POSITIVE(revenue) AS sql_exp,
    FROM sushi.waiter_revenue_by_day
)
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
