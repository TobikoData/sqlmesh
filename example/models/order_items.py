import random
from datetime import datetime

import pandas as pd

from example.helper import iter_dates
from sqlmesh import ExecutionContext, model
from sqlmesh.utils.date import to_ds


@model(
    """
    MODEL(
        name sushi.order_items,
        kind incremental,
        time_column ds,
        depends_on [sushi.orders, sushi.items],
        cron '@daily',
        batch_size 30,
        columns (
            id int,
            order_id int,
            item_id int,
            quantity int,
            ds text,
        ),
    )
    """
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs,
) -> pd.DataFrame:
    dfs = []

    orders_table = context.table("sushi.orders")
    items_table = context.table("sushi.items")

    for dt in iter_dates(start, end):
        orders = context.fetchdf(
            f"""
            SELECT *
            FROM {orders_table}
            WHERE ds = '{to_ds(dt)}'
            """
        )

        if not isinstance(orders, pd.DataFrame):
            orders = orders.toPandas()

        items = context.fetchdf(
            f"""
SELECT *
            FROM {items_table}
            WHERE ds = '{to_ds(dt)}'
            """
        )

        if not isinstance(items, pd.DataFrame):
            items = items.toPandas()

        order_items = []

        for order in orders.to_dict(orient="records"):
            for item in items.sample(n=random.randint(1, 5)).to_dict(orient="records"):
                order_items.append(
                    {
                        "order_id": order["id"],
                        "item_id": item["id"],
                        "quantity": random.randint(1, 10),
                        "ds": dt,
                    }
                )
        dfs.append(
            pd.DataFrame(order_items).reset_index().rename(columns={"index": "id"})
        )

    return pd.concat(dfs)
