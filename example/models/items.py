import random
from datetime import datetime

import numpy as np
import pandas as pd

from example.helper import iter_dates
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
    kind=IncrementalByTimeRangeKind(time_column="ds"),
    start="Jan 1 2022",
    cron="@daily",
    batch_size=30,
    columns={
        "id": "int",
        "name": "text",
        "price": "double",
        "ds": "text",
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
