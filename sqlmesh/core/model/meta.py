from __future__ import annotations

import typing as t
from datetime import timedelta
from enum import Enum

from pydantic import Field, root_validator, validator
from sqlglot import exp

from sqlmesh.core import dialect as d
from sqlmesh.core.model.kind import (
    IncrementalByUniqueKeyKind,
    ModelKind,
    TimeColumn,
    ViewKind,
    _Incremental,
)
from sqlmesh.utils import unique
from sqlmesh.utils.cron import CroniterCache
from sqlmesh.utils.date import TimeLike, to_datetime
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


class IntervalUnit(str, Enum):
    """IntervalUnit is the inferred granularity of an incremental model.

    IntervalUnit can be one of 4 types, DAY, HOUR, MINUTE. The unit is inferred
    based on the cron schedule of a model. The minimum time delta between a sample set of dates
    is used to determine which unit a model's schedule is.
    """

    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"


AuditReference = t.Tuple[str, t.Dict[str, exp.Expression]]


class ModelMeta(PydanticModel):
    """Metadata for models which can be defined in SQL."""

    dialect: str = ""
    name: str
    kind: ModelKind = ViewKind()
    cron: str = "@daily"
    owner: t.Optional[str]
    description: t.Optional[str]
    stamp: t.Optional[str]
    start: t.Optional[TimeLike]
    retention: t.Optional[int]  # not implemented yet
    storage_format: t.Optional[str]
    partitioned_by_: t.List[str] = Field(default=[], alias="partitioned_by")
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_to_types_: t.Optional[t.Dict[str, exp.DataType]] = Field(default=None, alias="columns")
    column_descriptions_: t.Optional[t.Dict[str, str]]
    audits: t.List[AuditReference] = []
    tags: t.List[str] = []
    grain: t.List[str] = []
    hash_raw_query: bool = False

    _croniter: t.Optional[CroniterCache] = None
    _interval_unit: t.Optional[IntervalUnit] = None

    _model_kind_validator = ModelKind.field_validator()

    @validator("name", pre=True)
    def _name_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> str:
        return d.normalize_model_name(v, dialect=values.get("dialect"))

    @validator("audits", pre=True)
    def _audits_validator(cls, v: t.Any) -> t.Any:
        def extract(v: exp.Expression) -> t.Tuple[str, t.Dict[str, str]]:
            kwargs = {}

            if isinstance(v, exp.Anonymous):
                func = v.name
                args = v.expressions
            elif isinstance(v, exp.Func):
                func = v.sql_name()
                args = list(v.args.values())
            else:
                return v.name.lower(), {}

            for arg in args:
                if not isinstance(arg, exp.EQ):
                    raise ConfigError(
                        f"Function '{func}' must be called with key-value arguments like {func}(arg=value)."
                    )
                kwargs[arg.left.name] = arg.right
            return (func.lower(), kwargs)

        if isinstance(v, (exp.Tuple, exp.Array)):
            return [extract(i) for i in v.expressions]
        if isinstance(v, exp.Paren):
            return [extract(v.this)]
        if isinstance(v, exp.Expression):
            return [extract(v)]
        if isinstance(v, list):
            return [
                (
                    entry[0].lower(),
                    {
                        key: d.parse(value)[0] if isinstance(value, str) else value
                        for key, value in entry[1].items()
                    },
                )
                for entry in v
            ]
        return v

    @validator("partitioned_by_", "tags", "grain", pre=True)
    def _value_or_tuple_validator(cls, v: t.Any) -> t.Any:
        if isinstance(v, (exp.Tuple, exp.Array)):
            return [e.name for e in v.expressions]
        if isinstance(v, exp.Expression):
            return [v.name]
        return v

    @validator("dialect", "owner", "storage_format", "description", "stamp", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        if isinstance(v, exp.Expression):
            return v.name
        return str(v) if v is not None else None

    @validator("cron", pre=True)
    def _cron_validator(cls, v: t.Any) -> t.Optional[str]:
        cron = cls._string_validator(v)
        if cron:
            from croniter import CroniterBadCronError, croniter

            try:
                croniter(cron)
            except CroniterBadCronError:
                raise ConfigError(f"Invalid cron expression '{cron}'")
        return cron

    @validator("columns_to_types_", pre=True)
    def _columns_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        if isinstance(v, exp.Schema):
            return {column.name: column.args["kind"] for column in v.expressions}
        if isinstance(v, dict):
            return {
                k: exp.DataType.build(data_type, dialect=values.get("dialect"))
                for k, data_type in v.items()
            }
        return v

    @validator("depends_on_", pre=True)
    def _depends_on_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[t.Set[str]]:
        dialect = values.get("dialect")

        if isinstance(v, (exp.Array, exp.Tuple)):
            return {
                d.normalize_model_name(
                    table.name if table.is_string else table.sql(dialect=dialect), dialect=dialect
                )
                for table in v.expressions
            }
        if isinstance(v, exp.Expression):
            return {d.normalize_model_name(v.sql(dialect=dialect), dialect=dialect)}

        return v

    @validator("start", pre=True)
    def _date_validator(cls, v: t.Any) -> t.Optional[TimeLike]:
        if isinstance(v, exp.Expression):
            v = v.name
        if v and not to_datetime(v):
            raise ConfigError(f"'{v}' needs to be time-like: https://pypi.org/project/dateparser")
        return v

    @root_validator
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        kind = values.get("kind")
        if kind:
            if values.get("partitioned_by_") and not kind.is_materialized:
                raise ValueError(f"partitioned_by field cannot be set for {kind} models")

        return values

    @property
    def time_column(self) -> t.Optional[TimeColumn]:
        """The time column for incremental models."""
        return getattr(self.kind, "time_column", None)

    @property
    def unique_key(self) -> t.List[str]:
        if isinstance(self.kind, IncrementalByUniqueKeyKind):
            return self.kind.unique_key
        return []

    @property
    def partitioned_by(self) -> t.List[str]:
        time_column = [self.time_column.column] if self.time_column else []
        return unique([*time_column, *self.partitioned_by_])

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        """A dictionary of column names to annotation comments."""
        return self.column_descriptions_ or {}

    @property
    def lookback(self) -> int:
        """The incremental lookback window."""
        return (self.kind.lookback if isinstance(self.kind, _Incremental) else 0) or 0

    @property
    def lookback_delta(self) -> timedelta:
        """The incremental lookback time delta."""
        if isinstance(self.kind, _Incremental):
            interval_unit = self.interval_unit()
            if interval_unit == IntervalUnit.DAY:
                return timedelta(days=self.lookback)
            if interval_unit == IntervalUnit.HOUR:
                return timedelta(hours=self.lookback)
            if interval_unit == IntervalUnit.MINUTE:
                return timedelta(minutes=self.lookback)
        return timedelta()

    @property
    def batch_size(self) -> t.Optional[int]:
        """The maximal number of units in a single task for a backfill."""
        return getattr(self.kind, "batch_size", None)

    def interval_unit(self, sample_size: int = 10) -> IntervalUnit:
        """Returns the IntervalUnit of the model

        The interval unit is used to determine the lag applied to start_date and end_date for model rendering and intervals.

        Args:
            sample_size: The number of samples to take from the cron to infer the unit.

        Returns:
            The IntervalUnit enum.
        """
        if not self._interval_unit:
            croniter = CroniterCache(self.cron)
            samples = [croniter.get_next() for _ in range(sample_size)]
            min_interval = min(b - a for a, b in zip(samples, samples[1:]))
            if min_interval >= 86400:
                self._interval_unit = IntervalUnit.DAY
            elif min_interval >= 3600:
                self._interval_unit = IntervalUnit.HOUR
            else:
                self._interval_unit = IntervalUnit.MINUTE
        return self._interval_unit

    def normalized_cron(self) -> str:
        """Returns the UTC normalized cron based on sampling heuristics.

        SQLMesh supports 3 interval units, daily, hourly, and minutes. If a job is scheduled
        daily at 1PM, the actual intervals are shifted back to midnight UTC.

        Returns:
            The cron string representing either daily, hourly, or minutes.
        """
        unit = self.interval_unit()
        if unit == IntervalUnit.MINUTE:
            return "* * * * *"
        if unit == IntervalUnit.HOUR:
            return "0 * * * *"
        if unit == IntervalUnit.DAY:
            return "0 0 * * *"
        return ""

    def croniter(self, value: TimeLike) -> CroniterCache:
        if self._croniter is None:
            self._croniter = CroniterCache(self.normalized_cron(), value)
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
