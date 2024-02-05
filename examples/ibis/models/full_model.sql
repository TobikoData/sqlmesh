MODEL (
  name ibis.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits [assert_positive_order_ids],
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    ibis.incremental_model
GROUP BY item_id
ORDER BY item_id
