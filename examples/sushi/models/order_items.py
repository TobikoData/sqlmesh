import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from sqlglot.expressions import to_column

from examples.sushi.helper import iter_dates
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind
from sqlmesh.utils.date import to_ds

ITEMS = "sushi.items"


@model(
    "sushi.order_items",
    kind=IncrementalByTimeRangeKind(time_column="ds"),
    cron="@daily",
    batch_size=30,
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
    latest: datetime,
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

        items = context.fetchdf(
            f"""
            SELECT *
            FROM {items_table}
            WHERE ds = '{to_ds(dt)}'
            """
        )

        for order_id in orders["id"]:
            n = random.randint(1, 5)

            yield pd.DataFrame(
                {
                    "order_id": order_id,
                    "item_id": items.sample(n=n)["id"],
                    "quantity": np.random.randint(1, 10, n),
                    "ds": to_ds(dt),
                }
            ).reset_index().rename(columns={"index": "id"})
