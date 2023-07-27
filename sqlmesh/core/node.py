from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import Field, validator
from sqlglot import exp

from sqlmesh.utils.cron import CroniterCache
from sqlmesh.utils.date import TimeLike, to_datetime
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import Audit


class IntervalUnit(str, Enum):
    """IntervalUnit is the inferred granularity of an incremental model.

    IntervalUnit can be one of 5 types, YEAR, MONTH, DAY, HOUR, MINUTE. The unit is inferred
    based on the cron schedule of a model. The minimum time delta between a sample set of dates
    is used to determine which unit a model's schedule is.
    """

    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"

    @property
    def is_date_granularity(self) -> bool:
        return self in (IntervalUnit.YEAR, IntervalUnit.MONTH, IntervalUnit.DAY)

    @property
    def _cron_expr(self) -> str:
        if self == IntervalUnit.MINUTE:
            return "* * * * *"
        if self == IntervalUnit.HOUR:
            return "0 * * * *"
        if self == IntervalUnit.DAY:
            return "0 0 * * *"
        if self == IntervalUnit.MONTH:
            return "0 0 1 * *"
        if self == IntervalUnit.YEAR:
            return "0 0 1 1 *"
        return ""

    def croniter(self, value: TimeLike) -> CroniterCache:
        return CroniterCache(self._cron_expr, value)

    def cron_next(self, value: TimeLike) -> TimeLike:
        """
        Get the next timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the next run.
        """
        return self.croniter(value).get_next()

    def cron_prev(self, value: TimeLike) -> TimeLike:
        """
        Get the previous timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the previous run.
        """
        return self.croniter(value).get_prev()

    def cron_floor(self, value: TimeLike) -> TimeLike:
        """
        Get the floor timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp floor.
        """
        return self.croniter(self.cron_next(value)).get_prev()


class Node(PydanticModel):
    """
    Node is the core abstraction for entity that can be executed within the scheduler.

    Args:
        name: The name of the node.
        description: The optional node description.
        owner: The owner of the node.
        start: The earliest date that the node will be executed for. If this is None,
            then the date is inferred by taking the most recent start date of its ancestors.
            The start date can be a static datetime or a relative datetime like "1 year ago"
        cron: A cron string specifying how often the ndoe should be run, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        stamp: An optional arbitrary string sequence used to create new node versions without making
            changes to any of the functional components of the definition.
    """

    name: str
    description: t.Optional[str]
    owner: t.Optional[str]
    start: t.Optional[TimeLike]
    cron: str = "@daily"
    stamp: t.Optional[str]
    interval_unit_: t.Optional[IntervalUnit] = Field(alias="interval_unit", default=None)

    _croniter: t.Optional[CroniterCache] = None
    __inferred_interval_unit: t.Optional[IntervalUnit] = None

    @validator("name", pre=True)
    def _name_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> str:
        return v.meta.get("sql") or v.sql() if isinstance(v, exp.Expression) else str(v)

    @validator("start", pre=True)
    def _date_validator(cls, v: t.Any) -> t.Optional[TimeLike]:
        if isinstance(v, exp.Expression):
            v = v.name
        if v and not to_datetime(v):
            raise ConfigError(f"'{v}' needs to be time-like: https://pypi.org/project/dateparser")
        return v

    @validator("cron", pre=True)
    def _cron_validator(cls, v: t.Any) -> t.Optional[str]:
        cron = str_or_exp_to_str(v)
        if cron:
            from croniter import CroniterBadCronError, croniter

            try:
                croniter(cron)
            except CroniterBadCronError:
                raise ConfigError(f"Invalid cron expression '{cron}'")
        return cron

    @validator("owner", "description", "stamp", "interval_unit_", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @property
    def batch_size(self) -> t.Optional[int]:
        """The maximal number of units in a single task for a backfill."""
        return None

    @property
    def data_hash(self) -> str:
        """
        Computes the data hash for the node.

        Returns:
            The data hash for the node.
        """
        raise NotImplementedError

    @property
    def interval_unit(self) -> IntervalUnit:
        """Returns the interval unit using which data intervals are computed for this node."""
        if self.interval_unit_ is not None:
            return self.interval_unit_
        return self._inferred_interval_unit()

    def metadata_hash(self, audits: t.Dict[str, Audit]) -> str:
        """
        Computes the metadata hash for the node.

        Args:
            audits: Available audits by name.

        Returns:
            The metadata hash for the node.
        """
        raise NotImplementedError

    def croniter(self, value: TimeLike) -> CroniterCache:
        if self._croniter is None:
            self._croniter = CroniterCache(self.cron, value)
        else:
            self._croniter.curr = value
        return self._croniter

    def cron_next(self, value: TimeLike) -> TimeLike:
        """
        Get the next timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the next run.
        """
        return self.croniter(value).get_next()

    def cron_prev(self, value: TimeLike) -> TimeLike:
        """
        Get the previous timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the previous run.
        """
        return self.croniter(value).get_prev()

    def cron_floor(self, value: TimeLike) -> TimeLike:
        """
        Get the floor timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp floor.
        """
        return self.croniter(self.cron_next(value)).get_prev()

    def _inferred_interval_unit(self, sample_size: int = 10) -> IntervalUnit:
        """Infers the interval unit from the cron expression.

        The interval unit is used to determine the lag applied to start_date and end_date for model rendering and intervals.

        Args:
            sample_size: The number of samples to take from the cron to infer the unit.

        Returns:
            The IntervalUnit enum.
        """
        if not self.__inferred_interval_unit:
            croniter = CroniterCache(self.cron)
            samples = [croniter.get_next() for _ in range(sample_size)]
            min_interval = min(b - a for a, b in zip(samples, samples[1:]))
            if min_interval >= 31536000:
                self.__inferred_interval_unit = IntervalUnit.YEAR
            elif min_interval >= 2419200:
                self.__inferred_interval_unit = IntervalUnit.MONTH
            elif min_interval >= 86400:
                self.__inferred_interval_unit = IntervalUnit.DAY
            elif min_interval >= 3600:
                self.__inferred_interval_unit = IntervalUnit.HOUR
            else:
                self.__inferred_interval_unit = IntervalUnit.MINUTE
        return self.__inferred_interval_unit


def str_or_exp_to_str(v: t.Any) -> t.Optional[str]:
    if isinstance(v, exp.Expression):
        return v.name
    return str(v) if v is not None else None
