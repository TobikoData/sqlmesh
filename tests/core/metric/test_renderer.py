from sqlglot import exp

from sqlmesh.core.metric import Renderer


def test_render(sushi_context_pre_scheduling, assert_exp_eq):
    context = sushi_context_pre_scheduling

    renderer = Renderer(
        [context.metrics["total_orders"], context.metrics["items_per_order"]],
        sources={
            "sushi.orders": exp.to_table("sushi.orders"),
            "sushi.order_items": exp.select("*").from_("sushi.order_items"),
        },
    )

    assert_exp_eq(
        renderer.agg("ds"),
        """
        SELECT
          sushi__orders.ds,
          total_orders AS total_orders,
          total_ordered_items / total_orders AS items_per_order
        FROM (
          SELECT
            ds,
            COUNT(sushi__orders.id) AS total_orders
          FROM sushi.orders AS sushi__orders
          GROUP BY
            ds
        ) AS sushi__orders
        FULL JOIN (
          SELECT
            ds,
            SUM(sushi__order_items.quantity) AS total_ordered_items
          FROM (
            SELECT
              *
            FROM sushi.order_items
          ) AS sushi__order_items
          GROUP BY
            ds
        ) AS sushi__order_items
          USING (ds)
        """,
    )
