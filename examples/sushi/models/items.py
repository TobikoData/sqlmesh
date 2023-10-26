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

ITEMS = [
    "Ahi",
    "Aji",
    "Amaebi",
    "Anago",
    "Aoyagi",
    "Bincho",
    "Katsuo",
    "Ebi",
    "Escolar",
    "Hamachi",
    "Hamachi Toro",
    "Hirame",
    "Hokigai",
    "Hotate",
    "Ika",
    "Ikura",
    "Iwashi",
    "Kani",
    "Kanpachi",
    "Maguro",
    "Saba",
    "Sake",
    "Sake Toro",
    "Tai",
    "Tako",
    "Tamago",
    "Tobiko",
    "Toro",
    "Tsubugai",
    "Umi Masu",
    "Unagi",
    "Uni",
]


@model(
    "sushi.items",
    kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
    start="1 week ago",
    cron="@daily",
    columns={
        "id": "int",
        "name": "text",
        "price": "double",
        "ds": "text",
    },
    audits=[
        ("accepted_values", {"column": to_column("name"), "is_in": ITEMS}),
        ("not_null", {"columns": [to_column("name"), to_column("price")]}),
        ("assert_items_price_exceeds_threshold", {"price": 0}),
    ],
    table_properties={
        "format": "PARQUET",
        "bucket_count": 0,
        "orc_bloom_filter_fpp": 0.05,
        "auto_purge": False,
    },
    session_properties={
        "string_prop": "some_value",
        "int_prop": 1,
        "float_prop": 1.0,
        "bool_prop": True,
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
        num_items = random.randint(10, len(ITEMS))
        dfs.append(
            pd.DataFrame(
                {
                    "name": random.sample(ITEMS, num_items),
                    "price": np.random.uniform(3.0, 10.0, size=num_items).round(2),
                    "ds": to_ds(dt),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

    return pd.concat(dfs)
