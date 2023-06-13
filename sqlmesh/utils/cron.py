from __future__ import annotations

import typing as t

from croniter import croniter

from sqlmesh.utils import ttl_cache
from sqlmesh.utils.date import TimeLike, now, to_datetime


@ttl_cache()
def cron_next(cron: str, time: TimeLike) -> float:
    return croniter(cron, to_datetime(time)).get_next()


@ttl_cache()
def cron_prev(cron: str, time: TimeLike) -> float:
    return croniter(cron, to_datetime(time)).get_prev()


class CroniterCache:
    def __init__(self, cron: str, time: t.Optional[TimeLike] = None):
        self.cron = cron
        self.curr: TimeLike = time or now()

    def get_next(self) -> float:
        self.curr = cron_next(self.cron, self.curr)
        return t.cast(float, self.curr)

    def get_prev(self) -> float:
        self.curr = cron_prev(self.cron, self.curr)
        return t.cast(float, self.curr)
