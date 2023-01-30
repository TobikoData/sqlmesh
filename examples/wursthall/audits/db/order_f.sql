AUDIT (
  name assert_valid_order_totals,
);

SELECT *
FROM db.order_f
WHERE
  order_ds BETWEEN @start_ds AND @end_ds
  AND order_total < 0;
