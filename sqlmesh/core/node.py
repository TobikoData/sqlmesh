from __future__ import annotations

import typing as t

from pydantic import Field
from sqlglot import exp

from sqlmesh.utils.date import CroniterCache, IntervalUnit, TimeLike, to_datetime
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import (
    PydanticModel,
    field_validator,
    model_validator,
    model_validator_v1_args,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import Audit


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
        cron: A cron string specifying how often the node should be run, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        interval_unit: The duration of an interval for the model. By default, it is computed from the cron expression.
        stamp: An optional arbitrary string sequence used to create new node versions without making
            changes to any of the functional components of the definition.
    """

    name: str
    description: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    cron: str = "@daily"
    interval_unit_: t.Optional[IntervalUnit] = Field(alias="interval_unit", default=None)
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
    def _interval_unit_validator(cls, v: t.Any) -> t.Optional[IntervalUnit]:
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
            self.__inferred_interval_unit = IntervalUnit.from_cron(self.cron, sample_size)
        return self.__inferred_interval_unit


def str_or_exp_to_str(v: t.Any) -> t.Optional[str]:
    if isinstance(v, exp.Expression):
        return v.name
    return str(v) if v is not None else None
