from __future__ import annotations

import random
import typing as t
from datetime import date, timedelta

import numpy as np  # noqa: TID253
from faker import Faker

SEED = 99999999
DATA_START_DATE = date(2022, 6, 1)
DATA_START_DATE_STR = DATA_START_DATE.strftime("%Y-%m-%d")


def set_seed(seed_value: t.Optional[date | int] = None) -> None:
    """Set the seed for all the random number generators"""
    if isinstance(seed_value, date):
        seed_date = int(seed_value.strftime("%Y%m%d")) if seed_value else 0
        seed = seed_date + int(SEED)
    else:
        seed = seed_value or int(SEED)
    np.random.seed(seed)
    random.seed(seed)
    Faker.seed(seed)


def iter_dates(start: date, end: date) -> t.Generator[date, None, None]:
    """Iterate through dates and set a deterministic seed."""
    for i in range((end - start).days + 1):
        dt = start + timedelta(days=i)
        yield dt
