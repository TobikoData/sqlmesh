from __future__ import annotations

import typing as t
from datetime import datetime, timedelta
from functools import lru_cache

from croniter import croniter
from sqlglot.helper import first

from sqlmesh.utils.date import TimeLike, now, to_datetime


@lru_cache(maxsize=None)
def interval_seconds(cron: str) -> int:
    """Computes the interval seconds of a cron statement if it is deterministic.

    Args:
        cron: The cron string.

    Returns:
        The number of seconds that cron represents if it is stable, otherwise 0.
    """
    deltas = set()
    curr = to_datetime(croniter(cron).get_next() * 1000)

    for _ in range(5):
        prev = curr
        curr = to_datetime(croniter(cron, curr).get_next() * 1000)
        deltas.add(curr - prev)

        if len(deltas) > 1:
            return 0
    return int(first(deltas).total_seconds())


class CroniterCache:
    def __init__(self, cron: str, time: t.Optional[TimeLike] = None):
        self.cron = cron
        self.curr: datetime = to_datetime(now() if time is None else time)
        self.interval_seconds = interval_seconds(self.cron)

    def get_next(self, estimate: bool = False) -> datetime:
        if estimate and self.interval_seconds:
            self.curr = self.curr + timedelta(seconds=self.interval_seconds)
        else:
            self.curr = to_datetime(croniter(self.cron, self.curr).get_next() * 1000)
        return self.curr

    def get_prev(self, estimate: bool = False) -> datetime:
        if estimate and self.interval_seconds:
            self.curr = self.curr - timedelta(seconds=self.interval_seconds)
        else:
            self.curr = to_datetime(croniter(self.cron, self.curr).get_prev() * 1000)
        return self.curr
