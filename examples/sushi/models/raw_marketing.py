import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from helper import iter_dates  # type: ignore
from sqlglot import exp

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import FullKind


@model(
    "sushi.raw_marketing",
    description="Table of marketing status.",
    kind=FullKind(),
    start="1 week ago",
    cron="@daily",
    grains=[
        "customer_id",
    ],
    columns={
        "customer_id": "int",
        "status": "text",
        "updated_at": "timestamp",
    },
)
def execute(
    context: ExecutionContext,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # Generate query with sqlglot dialect/quoting
    existing_table = context.table("sushi.raw_marketing")
    df_existing = context.fetchdf(
        exp.select("customer_id", "status", "updated_at").from_(existing_table),
        quote_identifiers=True,
    )

    seed = int(end.strftime("%Y%m%d"))
    np.random.seed(seed)
    num_customers = random.randint(30, 100)

    # Remove timezone because we specify `timestamp` in the columns type list
    exec_time = execution_time.replace(tzinfo=None)

    df_new = pd.DataFrame(
        {
            "customer_id": random.sample(range(0, 100), k=num_customers),
            "status": np.random.choice(["active", "inactive"], size=num_customers, p=[0.8, 0.2]),
            "updated_at": [exec_time] * num_customers,
        }
    )
    df = df_new.merge(df_existing, on="customer_id", how="left", suffixes=(None, "_old"))
    df["updated_at"] = pd.to_datetime(
        np.where(  # type: ignore
            df["status_old"] != df["status"], execution_time, df["updated_at_old"]
        ),
        errors="coerce",
        utc=True,
    )
    df = df.drop(columns=["status_old", "updated_at_old"])
    return df
