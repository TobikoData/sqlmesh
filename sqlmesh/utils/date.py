from __future__ import annotations

import time
import typing as t
import warnings
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from croniter import croniter

from sqlmesh.utils.errors import ConfigError

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


import dateparser
from sqlglot import exp

from sqlmesh.utils import ttl_cache

UTC = timezone.utc
TimeLike = t.Union[date, datetime, str, int, float]
MILLIS_THRESHOLD = time.time() + 100 * 365 * 24 * 3600
DATE_INT_FMT = "%Y%m%d"

if t.TYPE_CHECKING:
    from sqlmesh.core.scheduler import Interval


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

    @classmethod
    def from_cron(klass, cron: str, sample_size: int = 10) -> IntervalUnit:
        croniter = CroniterCache(cron)
        samples = [croniter.get_next() for _ in range(sample_size)]
        min_interval = min(b - a for a, b in zip(samples, samples[1:]))
        for unit, seconds in sorted(INTERVAL_SECONDS.items(), key=lambda x: x[1], reverse=True):
            if seconds <= min_interval:
                return unit
        raise ConfigError(f"Invalid cron '{cron}': must have a cadence of 1 minute or more.")

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

    @property
    def seconds(self) -> int:
        return INTERVAL_SECONDS[self]


INTERVAL_SECONDS = {
    IntervalUnit.YEAR: 60 * 60 * 24 * 365,
    IntervalUnit.MONTH: 60 * 60 * 24 * 28,
    IntervalUnit.DAY: 60 * 60 * 24,
    IntervalUnit.HOUR: 60 * 60,
    IntervalUnit.MINUTE: 60,
}


def now(minute_floor: bool = True) -> datetime:
    """
    Current utc datetime with optional minute level accuracy / granularity.

    minute_floor is set to True by default

    Args:
        minute_floor: If true (default), removes the second and microseconds from the current datetime.

    Returns:
        A datetime object with tz utc.
    """
    now = datetime.utcnow()
    if minute_floor:
        return now.replace(second=0, microsecond=0, tzinfo=UTC)
    return now.replace(tzinfo=UTC)


def now_timestamp(minute_floor: bool = False) -> int:
    """
    Current utc timestamp.

    Args:
        minute_floor: If true, removes the second and microseconds from the current datetime.

    Returns:
        UTC epoch millis timestamp
    """
    return to_timestamp(now(minute_floor))


def now_ds() -> str:
    """
    Current utc ds.

    Returns:
        Today's ds string.
    """
    return to_ds(now())


def yesterday() -> datetime:
    """
    Yesterday utc datetime.

    Returns:
        A datetime object with tz utc representing yesterday's date
    """
    return to_datetime("yesterday")


def yesterday_ds() -> str:
    """
    Yesterday utc ds.

    Returns:
        Yesterday's ds string.
    """
    return to_ds("yesterday")


def yesterday_timestamp() -> int:
    """
    Yesterday utc timestamp.

    Returns:
        UTC epoch millis timestamp of yesterday
    """
    return to_timestamp(yesterday())


def to_timestamp(value: TimeLike, relative_base: t.Optional[datetime] = None) -> int:
    """
    Converts a value into an epoch millis timestamp.

    Args:
        value: A variety of date formats. If value is a string, it must be in iso format.
        relative_base: The datetime to reference for time expressions that are using relative terms

    Returns:
        Epoch millis timestamp.
    """
    return int(to_datetime(value, relative_base=relative_base).timestamp() * 1000)


@ttl_cache()
def to_datetime(value: TimeLike, relative_base: t.Optional[datetime] = None) -> datetime:
    """Converts a value into a UTC datetime object.

    Args:
        value: A variety of date formats. If the value is number-like, it is assumed to be millisecond epochs
        if it is larger than MILLIS_THRESHOLD.
        relative_base: The datetime to reference for time expressions that are using relative terms

    Raises:
        ValueError if value cannot be converted to a datetime.

    Returns:
        A datetime object with tz utc.
    """
    if isinstance(value, datetime):
        dt: t.Optional[datetime] = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    elif isinstance(value, exp.Expression):
        return to_datetime(value.name)
    else:
        try:
            epoch = float(value)
        except ValueError:
            epoch = None

        if epoch is None:
            dt = dateparser.parse(str(value), settings={"RELATIVE_BASE": relative_base or now()})
        else:
            try:
                dt = datetime.strptime(str(value), DATE_INT_FMT)
            except ValueError:
                dt = datetime.fromtimestamp(
                    epoch / 1000.0 if epoch > MILLIS_THRESHOLD else epoch, tz=UTC
                )

    if dt is None:
        raise ValueError(f"Could not convert `{value}` to datetime.")

    if dt.tzinfo:
        return dt.astimezone(UTC)
    return dt.replace(tzinfo=UTC)


def to_date(value: TimeLike, relative_base: t.Optional[datetime] = None) -> date:
    """Converts a value into a UTC date object
    Args:
        value: A variety of date formats. If the value is number-like, it is assumed to be millisecond epochs
        if it is larger than MILLIS_THRESHOLD.
        relative_base: The datetime to reference for time expressions that are using relative terms

    Raises:
        ValueError if value cannot be converted to a date.

    Returns:
        A date object with tz utc.
    """
    return to_datetime(value, relative_base).date()


def date_dict(
    execution_time: TimeLike, start: t.Optional[TimeLike], end: t.Optional[TimeLike]
) -> t.Dict[str, t.Union[str, datetime, date, float, int]]:
    """Creates a kwarg dictionary of datetime variables for use in SQL Contexts.

    Keys are like start_date, start_ds, end_date, end_ds...

    Args:
        execution_time: Execution time.
        start: Start time.
        end: End time.

    Returns:
        A dictionary with various keys pointing to datetime formats.
    """
    kwargs: t.Dict[str, t.Union[str, datetime, date, float, int]] = {}

    execution_dt = to_datetime(execution_time)
    prefixes = [
        ("latest", execution_dt),  # TODO: Preserved for backward compatibility. Remove in 1.0.0.
        ("execution", execution_dt),
    ]

    if start is not None:
        prefixes.append(("start", to_datetime(start)))
    if end is not None:
        prefixes.append(("end", to_datetime(end)))

    for prefix, time_like in prefixes:
        dt = to_datetime(time_like)
        millis = to_timestamp(time_like)
        kwargs[f"{prefix}_dt"] = dt
        kwargs[f"{prefix}_date"] = to_date(dt)
        kwargs[f"{prefix}_ds"] = to_ds(time_like)
        kwargs[f"{prefix}_ts"] = dt.isoformat()
        kwargs[f"{prefix}_epoch"] = millis / 1000
        kwargs[f"{prefix}_millis"] = millis
    return kwargs


def to_ds(obj: TimeLike) -> str:
    """Converts a TimeLike object into YYYY-MM-DD formatted string."""
    return to_datetime(obj).isoformat()[0:10]


def is_date(obj: TimeLike) -> bool:
    """Checks if a TimeLike object should be treated like a date."""
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return True

    try:
        time.strptime(str(obj).replace("-", ""), DATE_INT_FMT)
        return True
    except ValueError:
        return False


def make_inclusive(start: TimeLike, end: TimeLike) -> Interval:
    """Adjust start and end times to to become inclusive datetimes.

    SQLMesh treats start and end times as inclusive so that filters can be written as

    SELECT * FROM x WHERE ds BETWEEN @start_ds AND @end_ds.
    SELECT * FROM x WHERE ts BETWEEN @start_ts AND @end_ts.

    In the ds ('2020-01-01') case, because start_ds and end_ds are categorical, between works even if
    start_ds and end_ds are equivalent. However, when we move to ts ('2022-01-01 12:00:00'), because timestamps
    are numeric, using simple equality doesn't make sense. When the end is not a categorical date, then it is
    treated as an exclusive range and converted to inclusive by subtracting 1 millisecond.

    Args:
        start: Start timelike object.
        end: End timelike object.

    Example:
        >>> make_inclusive("2020-01-01", "2020-01-01")
        (datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), datetime.datetime(2020, 1, 1, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc))

    Returns:
        A tuple of inclusive datetime objects.
    """
    return (to_datetime(start), make_inclusive_end(end))


def make_inclusive_end(end: TimeLike) -> datetime:
    end_dt = to_datetime(end)
    if is_date(end):
        end_dt = end_dt + timedelta(days=1)
    return end_dt - timedelta(milliseconds=1)


def validate_date_range(
    start: t.Optional[TimeLike],
    end: t.Optional[TimeLike],
) -> None:
    if start and end and to_datetime(start) > to_datetime(end):
        raise ValueError(
            f"Start date / time ({start}) can't be greater than end date / time ({end})"
        )


def time_like_to_str(time_like: TimeLike) -> str:
    if isinstance(time_like, str):
        return time_like
    if is_date(time_like):
        return to_ds(time_like)
    return to_datetime(time_like).isoformat()


def to_end_date(
    end_and_units: t.Union[t.Tuple[int, IntervalUnit], t.List[t.Tuple[int, IntervalUnit]]]
) -> TimeLike:
    end_and_units = [end_and_units] if isinstance(end_and_units, tuple) else end_and_units

    end, unit = max(end_and_units)
    if unit == IntervalUnit.DAY:
        return to_date(make_inclusive_end(end))
    return end
