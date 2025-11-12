import random
import typing as t
from datetime import datetime

import numpy as np  # noqa: TID253
import pandas as pd  # noqa: TID253
from sqlglot import exp

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


@model(
    "sushi.raw_marketing",
    description="Table of marketing status.",
    kind=dict(name=ModelKindName.FULL),
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
    column_descriptions={
        "customer_id": "Unique identifier of the customer",
    },
)
def execute(
    context: ExecutionContext,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # Generate query with sqlglot dialect/quoting
    existing_table = context.resolve_table("sushi.raw_marketing")
    engine_dialect = context.engine_adapter.dialect

    df_existing = context.fetchdf(
        exp.select("customer_id", "status", "updated_at").from_(
            existing_table, dialect=engine_dialect
        ),
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

    # clickhouse returns a dataframe with no columns if the query is empty, so we can't merge
    if not df_existing.empty:
        df = df_new.merge(df_existing, on="customer_id", how="left", suffixes=(None, "_old"))
    else:
        df = df_new
        df["status_old"] = pd.NA
        df["updated_at_old"] = pd.NA

    df["updated_at"] = pd.to_datetime(
        np.where(  # type: ignore
            df["status_old"] != df["status"], execution_time, df["updated_at_old"]
        ),
        errors="coerce",
        utc=True,
    )
    df = df.drop(columns=["status_old", "updated_at_old"]).reset_index(drop=True)
    return df
