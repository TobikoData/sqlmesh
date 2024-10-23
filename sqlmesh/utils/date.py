from __future__ import annotations

import re
import time
import typing as t
import warnings

from pandas.api.types import is_datetime64_any_dtype  # type: ignore

from datetime import date, datetime, timedelta, timezone

import dateparser
import pandas as pd
from dateparser import freshness_date_parser as freshness_date_parser_module
from dateparser.freshness_date_parser import freshness_date_parser
from sqlglot import exp

from sqlmesh.utils import ttl_cache

UTC = timezone.utc
TimeLike = t.Union[date, datetime, str, int, float]
DATE_INT_FMT = "%Y%m%d"

if t.TYPE_CHECKING:
    from sqlmesh.core.scheduler import Interval

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


# The Freshness Date Data Parser doesn't support plural units so we add the `s?` to the expression
freshness_date_parser_module.PATTERN = re.compile(
    r"(\d+[.,]?\d*)\s*(%s)s?\b" % freshness_date_parser_module._UNITS,  # type: ignore
    re.I | re.S | re.U,  # type: ignore
)
DAY_SHORTCUT_EXPRESSIONS = {"today", "yesterday", "tomorrow"}
TIME_UNITS = {"hours", "minutes", "seconds"}
TEMPORAL_TZ_TYPES = {
    exp.DataType.Type.TIMETZ,
    exp.DataType.Type.TIMESTAMPTZ,
    exp.DataType.Type.TIMESTAMPLTZ,
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
    now = datetime.now(tz=UTC)
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


def to_timestamp(
    value: TimeLike,
    relative_base: t.Optional[datetime] = None,
    check_categorical_relative_expression: bool = True,
) -> int:
    """
    Converts a value into an epoch millis timestamp.

    Args:
        value: A variety of date formats. If value is a string, it must be in iso format.
        relative_base: The datetime to reference for time expressions that are using relative terms
        check_categorical_relative_expression: If True, takes into account the relative expressions that are categorical.

    Returns:
        Epoch millis timestamp.
    """
    return int(
        to_datetime(
            value,
            relative_base=relative_base,
            check_categorical_relative_expression=check_categorical_relative_expression,
        ).timestamp()
        * 1000
    )


@ttl_cache()
def to_datetime(
    value: TimeLike,
    relative_base: t.Optional[datetime] = None,
    check_categorical_relative_expression: bool = True,
) -> datetime:
    """Converts a value into a UTC datetime object.

    Args:
        value: A variety of date formats. If the value is number-like, it is assumed to be millisecond epochs.
        relative_base: The datetime to reference for time expressions that are using relative terms.
        check_categorical_relative_expression: If True, takes into account the relative expressions that are categorical.

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
            relative_base = relative_base or now()
            expression = str(value)
            if check_categorical_relative_expression and is_categorical_relative_expression(
                expression
            ):
                relative_base = relative_base.replace(hour=0, minute=0, second=0, microsecond=0)

            # note: we hardcode TIMEZONE: UTC to work around this bug: https://github.com/scrapinghub/dateparser/issues/896
            # where dateparser just silently fails if it cant interpret the contents of /etc/localtime
            # this works because SQLMesh only deals with UTC, there is no concept of user local time
            dt = dateparser.parse(
                expression, settings={"RELATIVE_BASE": relative_base, "TIMEZONE": "UTC"}
            )
        else:
            try:
                dt = datetime.strptime(str(value), DATE_INT_FMT)
            except ValueError:
                dt = datetime.fromtimestamp(epoch / 1000.0, tz=UTC)

    if dt is None:
        raise ValueError(f"Could not convert `{value}` to datetime.")

    if dt.tzinfo:
        return dt if dt.tzinfo == UTC else dt.astimezone(UTC)
    return dt.replace(tzinfo=UTC)


def to_date(value: TimeLike, relative_base: t.Optional[datetime] = None) -> date:
    """Converts a value into a UTC date object
    Args:
        value: A variety of date formats. If the value is number-like, it is assumed to be millisecond epochs.
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
        kwargs[f"{prefix}_ts"] = to_ts(dt)
        kwargs[f"{prefix}_tstz"] = to_tstz(dt)
        kwargs[f"{prefix}_epoch"] = millis / 1000
        kwargs[f"{prefix}_millis"] = millis
        kwargs[f"{prefix}_hour"] = dt.hour
    return kwargs


def to_ds(obj: TimeLike) -> str:
    """Converts a TimeLike object into YYYY-MM-DD formatted string."""
    return to_ts(obj)[0:10]


def to_ts(obj: TimeLike) -> str:
    """Converts a TimeLike object into YYYY-MM-DD HH:MM:SS formatted string."""
    return to_datetime(obj).replace(tzinfo=None).isoformat(sep=" ")


def to_tstz(obj: TimeLike) -> str:
    """Converts a TimeLike object into YYYY-MM-DD HH:MM:SS+00:00 formatted string."""
    return to_datetime(obj).isoformat(sep=" ")


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
    treated as an exclusive range and converted to inclusive by subtracting 1 microsecond.

    Args:
        start: Start timelike object.
        end: End timelike object.

    Example:
        >>> make_inclusive("2020-01-01", "2020-01-01")
        (datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), datetime.datetime(2020, 1, 1, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc))

    Returns:
        A tuple of inclusive datetime objects.
    """
    return (to_datetime(start), make_inclusive_end(end))


def make_inclusive_end(end: TimeLike) -> datetime:
    return make_exclusive(end) - timedelta(microseconds=1)


def make_exclusive(time: TimeLike) -> datetime:
    dt = to_datetime(time)
    if is_date(time):
        dt = dt + timedelta(days=1)
    return dt


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
    return to_ts(time_like)


def is_categorical_relative_expression(expression: str) -> bool:
    if expression.strip().lower() in DAY_SHORTCUT_EXPRESSIONS:
        return True
    grain_kwargs = freshness_date_parser.get_kwargs(expression)
    if not grain_kwargs:
        return False
    return not any(k in TIME_UNITS for k in grain_kwargs)


def to_time_column(
    time_column: t.Union[TimeLike, exp.Null],
    time_column_type: exp.DataType,
    dialect: str,
    time_column_format: t.Optional[str] = None,
    nullable: bool = False,
) -> exp.Expression:
    """Convert a TimeLike object to the same time format and type as the model's time column."""
    if dialect == "clickhouse" and time_column_type.is_type(
        *(exp.DataType.TEMPORAL_TYPES - {exp.DataType.Type.DATE, exp.DataType.Type.DATE32})
    ):
        if time_column_type.is_type(exp.DataType.Type.DATETIME64):
            if nullable:
                time_column_type.set("nullable", nullable)
        else:
            # Clickhouse will error if we pass fractional seconds to DateTime, so we always
            # use DateTime64 for timestamps.
            #
            # `datetime` objects have microsecond precision, so we specify the type precision as 6.
            # If a timezone is present in the passed type object, it is included in the DateTime64 type
            # via the `expressions` arg.
            time_column_type = exp.DataType.build(
                exp.DataType.Type.DATETIME64,
                expressions=[
                    exp.DataTypeParam(this=exp.Literal(this=6, is_string=False)),
                    *time_column_type.expressions,
                ],
                nullable=nullable or time_column_type.args.get("nullable", False),
            )

    if isinstance(time_column, exp.Null):
        return exp.cast(time_column, to=time_column_type)
    if time_column_type.is_type(exp.DataType.Type.DATE, exp.DataType.Type.DATE32):
        return exp.cast(exp.Literal.string(to_ds(time_column)), to="date")
    if time_column_type.is_type(*TEMPORAL_TZ_TYPES):
        return exp.cast(exp.Literal.string(to_tstz(time_column)), to=time_column_type)
    if time_column_type.is_type(*exp.DataType.TEMPORAL_TYPES):
        return exp.cast(exp.Literal.string(to_ts(time_column)), to=time_column_type)

    if time_column_format:
        time_column = to_datetime(time_column).strftime(time_column_format)
    if time_column_type.is_type(*exp.DataType.TEXT_TYPES):
        return exp.Literal.string(time_column)
    if time_column_type.is_type(*exp.DataType.NUMERIC_TYPES):
        return exp.Literal.number(time_column)
    return exp.convert(time_column)


def pandas_timestamp_to_pydatetime(
    df: pd.DataFrame, columns_to_types: t.Optional[t.Dict[str, exp.DataType]]
) -> pd.DataFrame:
    for column in df.columns:
        if is_datetime64_any_dtype(df.dtypes[column]):
            # We must use `pd.Series` and dtype or pandas will convert it back to pd.Timestamp during assignment
            # https://stackoverflow.com/a/68961834/1707525
            df[column] = pd.Series(df[column].dt.to_pydatetime(), dtype="object")

            if columns_to_types and columns_to_types[column].this in (
                exp.DataType.Type.DATE,
                exp.DataType.Type.DATE32,
            ):
                # Sometimes `to_pydatetime()` has already converted to date, so we only extract from datetime objects.
                df[column] = df[column].map(
                    lambda x: x.date() if type(x) is datetime and not pd.isna(x) else x
                )

    return df
