import random
import typing as t
from datetime import datetime, timedelta

import pandas as pd
from helper import iter_dates  # type: ignore

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind
from sqlmesh.utils.date import to_date

CUSTOMERS = list(range(0, 100))
WAITERS = list(range(0, 10))


@model(
    "sushi.orders",
    description="Table of sushi orders.",
    kind=IncrementalByTimeRangeKind(time_column="event_date", batch_size=30),
    start="1 week ago",
    cron="@daily",
    grains=[
        "id AS order_id",
    ],
    references=[
        "customer_id",
        "waiter_id",
    ],
    columns={
        "id": "int",
        "customer_id": "int",
        "waiter_id": "int",
        "start_ts": "int",
        "end_ts": "int",
        "event_date": "date",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    dfs = []
    for dt in iter_dates(start, end):
        num_orders = random.randint(10, 30)

        start_ts = [
            int((dt + timedelta(seconds=random.randint(0, 80000))).timestamp())
            for _ in range(num_orders)
        ]

        end_ts = [int(s + random.randint(0, 60 * 60)) for s in start_ts]

        dfs.append(
            pd.DataFrame(
                {
                    "customer_id": random.choices(CUSTOMERS, k=num_orders),
                    "waiter_id": random.choices(WAITERS, k=num_orders),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "event_date": to_date(dt),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

    return pd.concat(dfs)
