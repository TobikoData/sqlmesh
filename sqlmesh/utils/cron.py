from __future__ import annotations

import typing as t
from datetime import timedelta

from croniter import croniter
from sqlglot.helper import first

from sqlmesh.utils import ttl_cache
from sqlmesh.utils.date import TimeLike, now, to_datetime


@ttl_cache()
def cron_next(cron: str, time: TimeLike) -> float:
    return croniter(cron, to_datetime(time)).get_next()


@ttl_cache()
def cron_prev(cron: str, time: TimeLike) -> float:
    return croniter(cron, to_datetime(time)).get_prev()


class CroniterCache:
    ESTIMATE_SAMPLES_NUM = 10

    def __init__(self, cron: str, time: t.Optional[TimeLike] = None):
        self.cron = cron
        self.curr: TimeLike = now() if time is None else time
        self._interval_seconds: t.Optional[int] = None

    @property
    def interval_seconds(self) -> int:
        """The estimated number of seconds between intervals of the given cron.

        This method takes a sample of crons and if they are all evenly spaced,
        than that number of seconds is returned. Otherwise, the sentinel value 0
        is returned indicating that there is no deterministic number of seconds
        to substitute for the cron call.
        """
        if self._interval_seconds is None:
            seconds = set()
            curr = to_datetime(self.curr)

            for _ in range(self.ESTIMATE_SAMPLES_NUM):
                prev = curr
                curr = to_datetime(cron_next(self.cron, curr))
                seconds.add(curr - prev)

            if len(seconds) == 1:
                self._interval_seconds = first(seconds).seconds
            else:
                self._interval_seconds = 0

        return self._interval_seconds

    def get_next(self, estimate: bool = False) -> float:
        if estimate and self.interval_seconds:
            self.curr = to_datetime(self.curr) + timedelta(seconds=self.interval_seconds)
        else:
            self.curr = cron_next(self.cron, self.curr)
        return t.cast(float, self.curr)

    def get_prev(self, estimate: bool = False) -> float:
        if estimate and self.interval_seconds:
            self.curr = to_datetime(self.curr) - timedelta(seconds=self.interval_seconds)
        else:
            self.curr = cron_prev(self.cron, self.curr)
        return t.cast(float, self.curr)
