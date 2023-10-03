from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

from croniter import croniter
from sqlglot.helper import first

from sqlmesh.utils.date import TimeLike, now, to_datetime


class CroniterCache:
    ESTIMATE_SAMPLES_NUM = 5

    def __init__(self, cron: str, time: t.Optional[TimeLike] = None):
        self.cron = cron
        self.curr: datetime = to_datetime(now() if time is None else time)
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
            curr = self.curr

            for _ in range(self.ESTIMATE_SAMPLES_NUM):
                prev = curr
                curr = to_datetime(croniter(self.cron, curr).get_next())
                seconds.add(curr - prev)

            if len(seconds) == 1:
                self._interval_seconds = int(first(seconds).total_seconds())
            else:
                self._interval_seconds = 0

        return self._interval_seconds

    def get_next(self, estimate: bool = False) -> datetime:
        if estimate and self.interval_seconds:
            self.curr = self.curr + timedelta(seconds=self.interval_seconds)
        else:
            self.curr = to_datetime(croniter(self.cron, self.curr).get_next())
        return self.curr

    def get_prev(self, estimate: bool = False) -> datetime:
        if estimate and self.interval_seconds:
            self.curr = self.curr - timedelta(seconds=self.interval_seconds)
        else:
            self.curr = to_datetime(croniter(self.cron, self.curr).get_prev())
        return self.curr
