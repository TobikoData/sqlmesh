import random
from datetime import datetime, timedelta

import pandas as pd
from sqlglot import exp

from example.helper import iter_dates
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind
from sqlmesh.utils.date import to_ds

CUSTOMERS = list(range(0, 100))
WAITERS = list(range(0, 10))


@model(
    "sushi.orders",
    description="Table of sushi orders.",
    kind=IncrementalByTimeRangeKind(time_column="ds"),
    start="2022-01-01",
    cron="@daily",
    batch_size=30,
    columns={
        "id": exp.DataType.build("int"),
        "customer_id": exp.DataType.build("int"),
        "waiter_id": exp.DataType.build("int"),
        "start_ts": exp.DataType.build("int"),
        "end_ts": exp.DataType.build("int"),
        "ds": exp.DataType.build("text"),
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs,
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
                    "ds": to_ds(dt),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

    return pd.concat(dfs)
