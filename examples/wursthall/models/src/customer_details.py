import random
import typing as t
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from models.src.shared import DATA_START_DATE_STR, iter_dates, set_seed  # type: ignore

from sqlmesh import model
from sqlmesh.core.model import IncrementalByTimeRangeKind, TimeColumn


@dataclass(frozen=True)
class CustomerDetails:
    id: str
    name: str
    phone: str
    email: str
    register_ds: str


@model(
    "src.customer_details",
    kind=IncrementalByTimeRangeKind(
        time_column=TimeColumn(column="register_ds", format="%Y-%m-%d"),
        batch_size=200,
    ),
    start=DATA_START_DATE_STR,
    cron="@daily",
    columns={
        "id": "TEXT",
        "name": "TEXT",
        "email": "TEXT",
        "phone": "TEXT",
        "register_ds": "TEXT",
    },
)
def execute(
    start: datetime,
    end: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    customer_details = []
    for register_date in iter_dates(start, end):
        # Have the seed date be register date minus 10 years to make sure we get unique uuids
        seed_date = register_date - timedelta(weeks=520)
        set_seed(seed_date)
        faker = Faker()
        for _ in range(random.choice(range(10, 20))):
            customer_details.append(
                CustomerDetails(
                    id=str(faker.uuid4()),
                    name=faker.name(),
                    phone=faker.phone_number(),
                    email=faker.ascii_email(),
                    register_ds=register_date.strftime("%Y-%m-%d"),
                )
            )
    return pd.DataFrame(customer_details)
