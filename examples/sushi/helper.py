import random
import typing as t
from datetime import datetime, timedelta

import numpy as np  # noqa: TID253


def set_seed(dt: datetime) -> None:
    """Helper function to set a seed so random choices are not so random."""
    ts = int(dt.timestamp())
    random.seed(ts)
    np.random.seed(ts)


def iter_dates(start: datetime, end: datetime) -> t.Generator[datetime, None, None]:
    """Iterate through dates and set a deterministic seed."""
    for i in range((end - start).days + 1):
        dt = start + timedelta(days=i)
        set_seed(dt)
        yield dt
