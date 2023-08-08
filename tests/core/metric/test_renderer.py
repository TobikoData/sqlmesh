from sqlglot import exp

from sqlmesh.core.metric import Renderer
from sqlmesh.core.reference import ReferenceGraph


def test_render(sushi_context_pre_scheduling, assert_exp_eq):
    context = sushi_context_pre_scheduling

    renderer = Renderer(
        [
            context.metrics["total_orders"],
            context.metrics["items_per_order"],
            context.metrics["total_orders_from_active_customers"],
        ],
        graph=ReferenceGraph(context.models.values()),
        sources={
            "sushi.order_items": exp.select("*").from_("sushi.order_items"),
        },
    )

    assert_exp_eq(
        renderer.agg("ds"),
        """
        SELECT
          sushi__order_items.ds,
          total_orders AS total_orders,
          total_ordered_items / total_orders AS items_per_order,
          total_orders_from_active_customers AS total_orders_from_active_customers
        FROM (
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
        FULL JOIN (
          SELECT
            ds,
            COUNT(IF(sushi__customers.status = 'ACTIVE', sushi__orders.id, NULL)) AS total_orders_from_active_customers,
            COUNT(sushi__orders.id) AS total_orders
          FROM sushi.orders AS sushi__orders
          LEFT JOIN sushi.customers AS sushi__customers
            ON sushi__orders.customer_id = sushi__customers.customer_id
          GROUP BY
            ds
        ) AS sushi__orders
          USING (ds)
        """,
    )
