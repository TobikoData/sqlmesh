def test_variables(assert_exp_eq, sushi_dbt_context):
    assert_exp_eq(
        sushi_dbt_context.models["sushi.top_waiters"].render_query(),
        f"""
        SELECT
          CAST(waiter_id AS INT) AS waiter_id,
          CAST(revenue AS DOUBLE) AS revenue
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
        """,
    )
