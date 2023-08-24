from sqlmesh.core.metric import rewrite
from sqlmesh.core.reference import ReferenceGraph


def test_rewrite(sushi_context_pre_scheduling, assert_exp_eq):
    context = sushi_context_pre_scheduling
    graph = ReferenceGraph(context.models.values())

    query = rewrite(
        """
        SELECT
          ds,
          METRIC(total_orders),
          METRIC(items_per_order),
          METRIC(total_orders_from_active_customers),
        FROM __semantic.__table
        GROUP BY ds
        """,
        graph=graph,
        metrics=context.metrics,
    )

    assert_exp_eq(
        query,
        """
        SELECT
          __table.ds AS ds,
          total_orders AS _col_1,
          total_ordered_items / total_orders AS _col_2,
          total_orders_from_active_customers AS _col_3
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
        """,
    )

    query = rewrite(
        """
        SELECT
          ds,
          METRIC(total_orders),
          METRIC(items_per_order),
          METRIC(total_orders_from_active_customers),
        FROM sushi.orders
        GROUP BY ds
        """,
        graph=graph,
        metrics=context.metrics,
    )

    assert_exp_eq(
        query,
        """
        SELECT
          orders.ds AS ds,
          total_orders AS _col_1,
          total_ordered_items / total_orders AS _col_2,
          total_orders_from_active_customers AS _col_3
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
        ) AS orders
        FULL JOIN (
          SELECT
            sushi__order_items.ds,
            SUM(sushi__order_items.quantity) AS total_ordered_items
          FROM sushi.order_items AS sushi__order_items
          GROUP BY
            sushi__order_items.ds
        ) AS sushi__order_items
          ON orders.ds = sushi__order_items.ds
        """,
    )
