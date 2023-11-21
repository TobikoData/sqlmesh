from sqlglot import parse_one

from sqlmesh.core.metric import rewrite
from sqlmesh.core.reference import ReferenceGraph


def test_rewrite(sushi_context_pre_scheduling, assert_exp_eq):
    context = sushi_context_pre_scheduling
    graph = ReferenceGraph(context.models.values())

    query = rewrite(
        """
        SELECT
          c.customer_id,
          METRIC(total_orders),
        FROM sushi.customers AS c
        GROUP BY c.customer_id
        """,
        graph=graph,
        metrics=context.metrics,
    )
    assert_exp_eq(
        query,
        parse_one(
            """
            SELECT
              c.customer_id AS customer_id,
              c.total_orders AS total_orders
            FROM (
              SELECT
                sushi__customers.customer_id
              FROM sushi.customers AS sushi__customers
              GROUP BY
                sushi__customers.customer_id
            ) AS c
            FULL JOIN (
              SELECT
                sushi__orders.customer_id,
                COUNT(sushi__orders.id) AS total_orders
              FROM sushi.orders AS sushi__orders
              GROUP BY
                sushi__orders.customer_id
            ) AS sushi__orders
              ON c.customer_id = sushi__orders.customer_id
            """,
            dialect=context.config.dialect,
        ),
    )

    query = rewrite(
        """
        SELECT
          date,
          METRIC(total_orders),
          METRIC(items_per_order),
          METRIC(total_orders_from_active_customers),
        FROM __semantic.__table
        GROUP BY date
        """,
        graph=graph,
        metrics=context.metrics,
    )

    assert_exp_eq(
        query,
        parse_one(
            """
            SELECT
              __table.date AS date,
              __table.total_orders AS total_orders,
              __table.total_ordered_items / __table.total_orders AS items_per_order,
              __table.total_orders_from_active_customers AS total_orders_from_active_customers
            FROM (
              SELECT
                sushi__orders.date,
                COUNT(IF(sushi__customers.status = 'ACTIVE', sushi__orders.id, NULL)) AS total_orders_from_active_customers,
                COUNT(sushi__orders.id) AS total_orders
              FROM sushi.orders AS sushi__orders
              LEFT JOIN sushi.customers AS sushi__customers
                ON sushi__orders.customer_id = sushi__customers.customer_id
              GROUP BY
                sushi__orders.date
            ) AS __table
            FULL JOIN (
              SELECT
                sushi__order_items.date,
                SUM(sushi__order_items.quantity) AS total_ordered_items
              FROM sushi.order_items AS sushi__order_items
              GROUP BY
                sushi__order_items.date
            ) AS sushi__order_items
              ON __table.date = sushi__order_items.date

            """,
            dialect=context.config.dialect,
        ),
    )

    query = rewrite(
        """
        SELECT
          date,
          METRIC(total_orders),
          METRIC(items_per_order),
          METRIC(total_orders_from_active_customers),
        FROM sushi.orders
        GROUP BY date
        """,
        graph=graph,
        metrics=context.metrics,
    )

    assert_exp_eq(
        query,
        parse_one(
            """
            SELECT
              orders.date AS date,
              orders.total_orders AS total_orders,
              orders.total_ordered_items / orders.total_orders AS items_per_order,
              orders.total_orders_from_active_customers AS total_orders_from_active_customers
            FROM (
              SELECT
                sushi__orders.date,
                COUNT(IF(sushi__customers.status = 'ACTIVE', sushi__orders.id, NULL)) AS total_orders_from_active_customers,
                COUNT(sushi__orders.id) AS total_orders
              FROM sushi.orders AS sushi__orders
              LEFT JOIN sushi.customers AS sushi__customers
                ON sushi__orders.customer_id = sushi__customers.customer_id
              GROUP BY
                sushi__orders.date
            ) AS orders
            FULL JOIN (
              SELECT
                sushi__order_items.date,
                SUM(sushi__order_items.quantity) AS total_ordered_items
              FROM sushi.order_items AS sushi__order_items
              GROUP BY
                sushi__order_items.date
            ) AS sushi__order_items
              ON orders.date = sushi__order_items.date

            """,
            dialect=context.config.dialect,
        ),
    )

    query = rewrite(
        """
        SELECT
          date,
          status,
          METRIC(total_orders),
        FROM __semantic.__table
        GROUP BY date, status
        """,
        graph=graph,
        metrics=context.metrics,
    )
    assert_exp_eq(
        query,
        parse_one(
            """
            SELECT
              __table.date AS date,
              __table.status AS status,
              __table.total_orders AS total_orders
            FROM (
              SELECT
                sushi__orders.date,
                sushi__customers.status,
                COUNT(sushi__orders.id) AS total_orders
              FROM sushi.orders AS sushi__orders
              LEFT JOIN sushi.customers AS sushi__customers
                ON sushi__orders.customer_id = sushi__customers.customer_id
              GROUP BY
                sushi__orders.date,
                sushi__customers.status
            ) AS __table
            """,
            dialect=context.config.dialect,
        ),
    )

    query = rewrite(
        """
        SELECT
          t.date,
          m.status,
          METRIC(t.total_orders),
        FROM __semantic.__table t
        LEFT JOIN sushi.raw_marketing AS m
        WHERE t.date > '2022-01-01'
        GROUP BY t.date, m.status
        """,
        graph=graph,
        metrics=context.metrics,
    )
    assert_exp_eq(
        query,
        parse_one(
            """
            SELECT
              t.date AS date,
              t.status AS status,
              t.total_orders AS total_orders
            FROM (
              SELECT
                sushi__orders.date,
                m.status,
                COUNT(sushi__orders.id) AS total_orders
              FROM sushi.orders AS sushi__orders
              LEFT JOIN sushi.raw_marketing AS m
                ON sushi__orders.customer_id = m.customer_id
              WHERE
                sushi__orders.date > '2022-01-01'
              GROUP BY
                sushi__orders.date,
                m.status
            ) AS t
            """,
            dialect=context.config.dialect,
        ),
    )
