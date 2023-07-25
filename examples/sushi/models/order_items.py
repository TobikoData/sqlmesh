import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from helper import iter_dates  # type: ignore
from sqlglot.expressions import to_column

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind
from sqlmesh.utils.date import to_ds

ITEMS = "sushi.items"


@model(
    "sushi.order_items",
    kind=IncrementalByTimeRangeKind(
        time_column="ds",
        batch_size=30,
    ),
    cron="@daily",
    columns={
        "id": "int",
        "order_id": "int",
        "item_id": "int",
        "quantity": "int",
        "ds": "text",
    },
    audits=[
        (
            "NOT_NULL",
            {"columns": [to_column(c) for c in ("id", "order_id", "item_id", "quantity")]},
        ),
        ("assert_order_items_quantity_exceeds_threshold", {"quantity": 0}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    orders_table = context.table("sushi.orders")
    items_table = context.table(ITEMS)

    for dt in iter_dates(start, end):
        orders = context.fetchdf(
            f"""
            SELECT *
            FROM {orders_table}
            WHERE ds = '{to_ds(dt)}'
            """
        )
        # Normalize column names to support Snowflake.
        orders = orders.rename(columns={col: col.lower() for col in orders.columns})  # type: ignore

        items = context.fetchdf(
            f"""
            SELECT *
            FROM {items_table}
            WHERE ds = '{to_ds(dt)}'
            """
        )
        # Normalize column names to support Snowflake.
        items = items.rename(columns={col: col.lower() for col in items.columns})  # type: ignore

        dfs = []

        for order_id in orders["id"]:
            n = random.randint(1, 5)

            dfs.append(
                pd.DataFrame(
                    {
                        "order_id": order_id,
                        "item_id": items.sample(n=n)["id"],
                        "quantity": np.random.randint(1, 10, n),
                        "ds": to_ds(dt),
                    }
                )
                .reset_index()
                .rename(columns={"index": "id"})
            )

        yield pd.concat(dfs)
