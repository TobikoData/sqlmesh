import random
import typing as t
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from faker import Faker
from models.src.shared import DATA_START_DATE_STR, iter_dates, set_seed  # type: ignore
from sqlglot import parse_one

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind, TimeColumn
from sqlmesh.utils.date import to_ds


@dataclass(frozen=True)
class OrderItemDetails:
    id: str
    customer_id: t.Optional[str]
    item_id: str
    quantity: int
    table_id: int
    order_ds: str


@model(
    "src.order_item_details",
    kind=IncrementalByTimeRangeKind(
        time_column=TimeColumn(column="order_ds", format="%Y-%m-%d"),
        batch_size=100,
    ),
    start=DATA_START_DATE_STR,
    cron="@daily",
    columns={
        "id": "TEXT",
        "customer_id": "TEXT",
        "item_id": "TEXT",
        "quantity": "SMALLINT",
        "table_id": "SMALLINT",
        "order_ds": "TEXT",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    customer_details_table_name = context.table("src.customer_details")
    menu_item_details_table_name = context.table("src.menu_item_details")

    # We use parse_one here instead of a raw string because this is a multi-dialect
    # project and we want to ensure that the resulting query is properly quoted in
    # the target dialect before executing it
    df_customers = context.fetchdf(
        parse_one(
            f"""
            SELECT
                id AS customer_id,
                register_ds
            FROM {customer_details_table_name}
            WHERE
                register_ds <= '{to_ds(end)}'
            """
        ),
        quote_identifiers=True,
    )

    df_menu_items = context.fetchdf(
        parse_one(f"SELECT id AS item_id FROM {menu_item_details_table_name}"),
        quote_identifiers=True,
    )

    num_menu_items = len(df_menu_items.index)
    results = []
    for order_date in iter_dates(start, end):
        set_seed(order_date)
        faker = Faker()
        order_ds = order_date.strftime("%Y-%m-%d")
        for _ in range(random.choice(range(50, 100))):
            order_item_id = str(faker.uuid4())
            table_id = np.random.choice(range(0, 20))
            is_registered_customer = np.random.choice([True, False], p=[0.1, 0.9])
            if is_registered_customer:
                df_possible_customers = df_customers[
                    df_customers["register_ds"] <= order_ds
                ].reset_index()
                num_possible_customers = len(df_possible_customers.index)
                customer_id = df_possible_customers.iloc[
                    [random.choice(range(num_possible_customers))]
                ]["customer_id"].values[0]
            else:
                customer_id = None
            for _ in range(
                np.random.choice(
                    range(1, 10),
                    p=[0.1, 0.2, 0.3, 0.2, 0.1, 0.025, 0.025, 0.025, 0.025],
                )
            ):
                item_id = str(
                    df_menu_items.iloc[[random.choice(range(num_menu_items))]]["item_id"].values[0]
                )
                quantity = np.random.choice(range(1, 4), p=[0.8, 0.1, 0.1])
                results.append(
                    OrderItemDetails(
                        id=order_item_id,
                        customer_id=customer_id,
                        item_id=item_id,
                        quantity=quantity,
                        table_id=table_id,
                        order_ds=order_ds,
                    )
                )
    return pd.DataFrame(results)
