import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from models.src.shared import DATA_START_DATE_STR, set_seed  # type: ignore
from sqlglot import parse_one

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind, TimeColumn
from sqlmesh.utils.date import to_ds

COLUMN_TO_TYPE = {
    "order_id": "text",
    "customer_id": "text",
    "num_guests": "smallint",
    "order_total": "decimal(7, 2)",
    "order_tax": "decimal(7, 2)",
    "order_tip": "decimal(7, 2)",
    "credit_card_provider": "text",
    "charge_total": "decimal(7, 2)",
    "order_ds": "text",
}


@model(
    "db.order_f",
    kind=IncrementalByTimeRangeKind(
        time_column=TimeColumn(column="order_ds", format="%Y-%m-%d"),
        batch_size=200,
    ),
    start=DATA_START_DATE_STR,
    cron="@daily",
    columns=COLUMN_TO_TYPE,
    audits=["assert_valid_order_totals"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    item_d_table_name = context.resolve_table("db.item_d")
    order_item_f_table_name = context.resolve_table("db.order_item_f")

    # We use parse_one here instead of a raw string because this is a multi-dialect
    # project and we want to ensure that the resulting query is properly quoted in
    # the target dialect before executing it
    df_item_d = context.fetchdf(
        parse_one(f"SELECT item_id, item_price FROM {item_d_table_name}"),
        quote_identifiers=True,
    )

    df_order_item_f = context.fetchdf(
        parse_one(
            f"""
            SELECT
                order_id,
                customer_id,
                item_id,
                quantity,
                order_ds
            FROM {order_item_f_table_name}
            WHERE
                order_ds BETWEEN '{to_ds(start)}' AND '{to_ds(end)}'
            """
        ),
        quote_identifiers=True,
    )

    df_order_item_f = df_order_item_f.merge(df_item_d, how="inner", on="item_id")
    df_order_item_f["item_price"] = 1.00
    df_order_item_f["item_total"] = df_order_item_f["item_price"] * df_order_item_f["quantity"]
    df_order_item_f = (
        df_order_item_f.groupby(["order_id", "customer_id", "order_ds"], dropna=False)
        .agg(order_total=("item_total", "sum"))
        .sort_values(["order_ds"])
        .reset_index()
    )
    df_order_item_f = df_order_item_f.replace({np.nan: None})
    prev_order_ds = None
    for i, row in df_order_item_f.iterrows():
        i = t.cast(int, i)
        if row["order_ds"] != prev_order_ds:
            set_seed(datetime.strptime(row["order_ds"], "%Y-%m-%d").date())
            prev_order_ds = row["order_ds"]
        df_order_item_f.loc[i, "order_tax"] = row["order_total"] * 0.1
        df_order_item_f.loc[i, "order_tip"] = row["order_total"] * np.random.choice(
            [0, 0.1, 0.15, 0.2], p=[0.1, 0.2, 0.4, 0.3]
        )
        df_order_item_f.loc[i, "charge_total"] = (
            row["order_total"]
            + df_order_item_f.loc[i, "order_tax"]
            + df_order_item_f.loc[i, "order_tip"]
        )
        df_order_item_f.loc[i, "credit_card_provider"] = random.choice(
            ["Visa", "Mastercard", "Amex", "Discover"]
        )
        df_order_item_f.loc[i, "num_guests"] = np.random.choice(
            range(1, 10), p=[0.1, 0.2, 0.3, 0.2, 0.1, 0.025, 0.025, 0.025, 0.025]
        )
    return df_order_item_f[list(COLUMN_TO_TYPE)]
