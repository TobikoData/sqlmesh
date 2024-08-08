import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from helper import iter_dates  # type: ignore
from sqlglot import exp
from sqlglot.expressions import to_column

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.utils.date import to_date, to_ds

ITEMS = "sushi.items"


def get_items_table(context: ExecutionContext) -> str:
    return context.table(ITEMS)


@model(
    "sushi.order_items",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="event_date",
        batch_size=30,
    ),
    cron="@daily",
    columns={
        "id": "int",
        "order_id": "int",
        "item_id": "int",
        "quantity": "int",
        "event_date": "date",
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
    engine_dialect = context.engine_adapter.dialect

    items_table = get_items_table(context)

    rng = random.Random()

    for dt in iter_dates(start, end):
        rng.seed(int(dt.timestamp()))

        # Generate query with sqlglot dialect/quoting
        orders = context.fetchdf(
            exp.select("*")
            .from_(orders_table, dialect=engine_dialect)
            .where(f"event_date = CAST('{to_ds(dt)}' AS DATE)"),
            quote_identifiers=True,
        )

        # Normalize column names to support Snowflake.
        orders = orders.rename(columns={col: col.lower() for col in orders.columns})  # type: ignore

        # Generate query with sqlglot dialect/quoting
        items = context.fetchdf(
            exp.select("*")
            .from_(items_table, dialect=engine_dialect)
            .where(f"event_date = CAST('{to_ds(dt)}' AS DATE)"),
            quote_identifiers=True,
        )

        # Normalize column names to support Snowflake.
        items = items.rename(columns={col: col.lower() for col in items.columns})  # type: ignore

        dfs = []

        for order_id in orders["id"]:
            n = rng.randint(1, 5)

            dfs.append(
                pd.DataFrame(
                    {
                        "order_id": order_id,
                        "item_id": items.sample(n=n)["id"],
                        "quantity": np.random.randint(1, 10, n),
                        "event_date": to_date(dt),
                    }
                )
                .reset_index()
                .rename(columns={"index": "id"})
            )

        yield pd.concat(dfs).reset_index(drop=True)
