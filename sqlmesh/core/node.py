from __future__ import annotations

import typing as t
from datetime import datetime
from enum import Enum

from pydantic import Field
from sqlglot import exp

from sqlmesh.utils.cron import CroniterCache
from sqlmesh.utils.date import TimeLike, to_datetime
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import (
    PydanticModel,
    field_validator,
    model_validator,
    model_validator_v1_args,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import ModelAudit
    from sqlmesh.core.snapshot import Node


DEFAULT_SAMPLE_SIZE = 5


class IntervalUnit(str, Enum):
    """IntervalUnit is the inferred granularity of an incremental node.

    IntervalUnit can be one of 5 types, YEAR, MONTH, DAY, HOUR, MINUTE. The unit is inferred
    based on the cron schedule of a node. The minimum time delta between a sample set of dates
    is used to determine which unit a node's schedule is.
    """

    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    HALF_HOUR = "half_hour"
    QUARTER_HOUR = "quarter_hour"
    FIVE_MINUTE = "five_minute"

    @classmethod
    def from_cron(klass, cron: str, sample_size: int = DEFAULT_SAMPLE_SIZE) -> IntervalUnit:
        croniter = CroniterCache(cron)
        samples = [croniter.get_next() for _ in range(sample_size)]
        min_interval = min(b - a for a, b in zip(samples, samples[1:]))
        for unit, seconds in sorted(INTERVAL_SECONDS.items(), key=lambda x: x[1], reverse=True):
            if seconds <= min_interval.total_seconds():
                return unit
        raise ConfigError(f"Invalid cron '{cron}': must have a cadence of 5 minutes or more.")

    @property
    def is_date_granularity(self) -> bool:
        return self in (IntervalUnit.YEAR, IntervalUnit.MONTH, IntervalUnit.DAY)

    @property
    def is_year(self) -> bool:
        return self == IntervalUnit.YEAR

    @property
    def is_month(self) -> bool:
        return self == IntervalUnit.MONTH

    @property
    def is_day(self) -> bool:
        return self == IntervalUnit.DAY

    @property
    def is_hour(self) -> bool:
        return self == IntervalUnit.HOUR

    @property
    def is_minute(self) -> bool:
        return self in (IntervalUnit.FIVE_MINUTE, IntervalUnit.QUARTER_HOUR, IntervalUnit.HALF_HOUR)

    @property
    def _cron_expr(self) -> str:
        if self == IntervalUnit.FIVE_MINUTE:
            return "*/5 * * * *"
        if self == IntervalUnit.QUARTER_HOUR:
            return "*/15 * * * *"
        if self == IntervalUnit.HALF_HOUR:
            return "*/30 * * * *"
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

    def cron_next(self, value: TimeLike) -> datetime:
        """
        Get the next timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the next run.
        """
        return self.croniter(value).get_next()

    def cron_prev(self, value: TimeLike) -> datetime:
        """
        Get the previous timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the previous run.
        """
        return self.croniter(value).get_prev()

    def cron_floor(self, value: TimeLike) -> datetime:
        """
        Get the floor timestamp given a time-like value for this interval unit.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp floor.
        """
        return self.croniter(self.cron_next(value)).get_prev()

    @property
    def seconds(self) -> int:
        return INTERVAL_SECONDS[self]

    @property
    def milliseconds(self) -> int:
        return self.seconds * 1000


INTERVAL_SECONDS = {
    IntervalUnit.YEAR: 60 * 60 * 24 * 365,
    IntervalUnit.MONTH: 60 * 60 * 24 * 28,
    IntervalUnit.DAY: 60 * 60 * 24,
    IntervalUnit.HOUR: 60 * 60,
    IntervalUnit.HALF_HOUR: 60 * 30,
    IntervalUnit.QUARTER_HOUR: 60 * 15,
    IntervalUnit.FIVE_MINUTE: 60 * 5,
}


class _Node(PydanticModel):
    """
    Node is the core abstraction for entity that can be executed within the scheduler.

    Args:
        name: The name of the node.
        description: The name of the project this node belongs to, used in multi-repo deployments.
        description: The optional node description.
        owner: The owner of the node.
        start: The earliest date that the node will be executed for. If this is None,
            then the date is inferred by taking the most recent start date of its ancestors.
            The start date can be a static datetime or a relative datetime like "1 year ago"
        cron: A cron string specifying how often the node should be run, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        interval_unit: The duration of an interval for the node. By default, it is computed from the cron expression.
        stamp: An optional arbitrary string sequence used to create new node versions without making
            changes to any of the functional components of the definition.
        tags: A list of tags that can be used to filter nodes.
    """

    name: str
    project: str = ""
    description: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    cron: str = "@daily"
    interval_unit_: t.Optional[IntervalUnit] = Field(alias="interval_unit", default=None)
    tags: t.List[str] = []
    stamp: t.Optional[str] = None

    _croniter: t.Optional[CroniterCache] = None
    __inferred_interval_unit: t.Optional[IntervalUnit] = None

    @field_validator("name", mode="before")
    @classmethod
    def _name_validator(cls, v: t.Any) -> str:
        return v.meta.get("sql") or v.sql() if isinstance(v, exp.Expression) else str(v)

    @field_validator("start", mode="before")
    @classmethod
    def _date_validator(cls, v: t.Any) -> t.Optional[TimeLike]:
        if isinstance(v, exp.Expression):
            v = v.name
        if v and not to_datetime(v):
            raise ConfigError(f"'{v}' needs to be time-like: https://pypi.org/project/dateparser")
        return v

    @field_validator("cron", mode="before")
    @classmethod
    def _cron_validator(cls, v: t.Any) -> t.Optional[str]:
        cron = str_or_exp_to_str(v)
        if cron:
            from croniter import CroniterBadCronError, croniter

            try:
                croniter(cron)
            except CroniterBadCronError:
                raise ConfigError(f"Invalid cron expression '{cron}'")
        return cron

    @field_validator("owner", "description", "stamp", mode="before")
    @classmethod
    def _string_expr_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @field_validator("interval_unit_", mode="before")
    def _interval_unit_validator(cls, v: t.Any) -> t.Optional[t.Union[IntervalUnit, str]]:
        if isinstance(v, IntervalUnit):
            return v
        v = str_or_exp_to_str(v)
        if v:
            v = v.lower()
        return v

    @model_validator(mode="after")
    @model_validator_v1_args
    def _node_root_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        interval_unit = values.get("interval_unit_")
        if interval_unit:
            cron = values["cron"]
            max_interval_unit = IntervalUnit.from_cron(cron)
            if interval_unit.seconds > max_interval_unit.seconds:
                raise ConfigError(
                    f"Interval unit of '{interval_unit}' is larger than cron period of '{cron}'"
                )
        return values

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

    @property
    def depends_on(self) -> t.Set[str]:
        return set()

    def metadata_hash(self, audits: t.Dict[str, ModelAudit]) -> str:
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
            self._croniter.curr = to_datetime(value)
        return self._croniter

    def cron_next(self, value: TimeLike) -> datetime:
        """
        Get the next timestamp given a time-like value and the node's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the next run.
        """
        return self.croniter(value).get_next()

    def cron_prev(self, value: TimeLike) -> datetime:
        """
        Get the previous timestamp given a time-like value and the node's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the previous run.
        """
        return self.croniter(value).get_prev()

    def cron_floor(self, value: TimeLike) -> datetime:
        """
        Get the floor timestamp given a time-like value and the node's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp floor.
        """
        return self.croniter(self.cron_next(value)).get_prev()

    def text_diff(self, other: Node) -> str:
        """Produce a text diff against another node.

        Args:
            other: The node to diff against. Must be of the same type.

        Returns:
            A unified text diff showing additions and deletions.
        """
        raise NotImplementedError

    def _inferred_interval_unit(self, sample_size: int = DEFAULT_SAMPLE_SIZE) -> IntervalUnit:
        """Infers the interval unit from the cron expression.

        The interval unit is used to determine the lag applied to start_date and end_date for node rendering and intervals.

        Args:
            sample_size: The number of samples to take from the cron to infer the unit.

        Returns:
            The IntervalUnit enum.
        """
        if not self.__inferred_interval_unit:
            self.__inferred_interval_unit = IntervalUnit.from_cron(self.cron, sample_size)
        return self.__inferred_interval_unit

    @property
    def is_model(self) -> bool:
        """Return True if this is a model node"""
        return False

    @property
    def is_audit(self) -> bool:
        """Return True if this is an audit node"""
        return False


class NodeType(str, Enum):
    MODEL = "model"
    AUDIT = "audit"


def str_or_exp_to_str(v: t.Any) -> t.Optional[str]:
    if isinstance(v, exp.Expression):
        return v.name
    return str(v) if v is not None else None
