from datetime import date, datetime

import pytest
from freezegun import freeze_time

from sqlmesh.utils.date import (
    UTC,
    is_catagorical_relative_expression,
    make_inclusive,
    to_datetime,
    to_timestamp,
)


def test_to_datetime() -> None:
    target = datetime(2020, 1, 1).replace(tzinfo=UTC)
    assert to_datetime("2020-01-01 00:00:00") == target
    assert to_datetime("2020-01-01T00:00:00") == target
    assert to_datetime("2020-01-01T05:00:00+05:00") == target
    assert to_datetime("2020-01-01") == target
    assert to_datetime(datetime(2020, 1, 1)) == target
    assert to_datetime(1577836800000) == target
    assert to_datetime(20200101) == target
    assert to_datetime("20200101") == target
    assert to_datetime("0") == datetime(1970, 1, 1, tzinfo=UTC)

    target = datetime(1971, 1, 1).replace(tzinfo=UTC)
    assert to_datetime("1971-01-01") == target
    assert to_datetime("31536000000") == target


@pytest.mark.parametrize(
    "expression, result",
    [
        ("1 second ago", datetime(2023, 1, 20, 12, 29, 59, tzinfo=UTC)),
        ("1 minute ago", datetime(2023, 1, 20, 12, 29, 00, tzinfo=UTC)),
        ("1 hour ago", datetime(2023, 1, 20, 11, 30, 00, tzinfo=UTC)),
        ("1 day ago", datetime(2023, 1, 19, 00, 00, 00, tzinfo=UTC)),
        ("1 week ago", datetime(2023, 1, 13, 00, 00, 00, tzinfo=UTC)),
        ("1 month ago", datetime(2022, 12, 20, 00, 00, 00, tzinfo=UTC)),
        ("1 year ago", datetime(2022, 1, 20, 00, 00, 00, tzinfo=UTC)),
        ("1 decade ago", datetime(2013, 1, 20, 00, 00, 00, tzinfo=UTC)),
        ("3 days 2 hours ago", datetime(2023, 1, 17, 10, 30, 00, tzinfo=UTC)),
        ("2 years 5 second ago", datetime(2021, 1, 20, 12, 29, 55, tzinfo=UTC)),
        ("24 hours ago", datetime(2023, 1, 19, 12, 30, 00, tzinfo=UTC)),
        ("1 year 5 days ago", datetime(2022, 1, 15, 00, 00, 00, tzinfo=UTC)),
        ("yesterday", datetime(2023, 1, 19, 00, 00, 00, tzinfo=UTC)),
        ("today", datetime(2023, 1, 20, 00, 00, 00, tzinfo=UTC)),
        ("tomorrow", datetime(2023, 1, 21, 00, 00, 00, tzinfo=UTC)),
    ],
)
def test_to_datetime_with_expressions(expression, result) -> None:
    with freeze_time("2023-01-20 12:30:30"):
        assert to_datetime(expression) == result


def test_to_timestamp() -> None:
    assert to_timestamp("2020-01-01") == 1577836800000


@pytest.mark.parametrize(
    "start_in, end_in, start_out, end_out",
    [
        ("2020-01-01", "2020-01-01", "2020-01-01", "2020-01-01 23:59:59.999000"),
        ("2020-01-01", date(2020, 1, 1), "2020-01-01", "2020-01-01 23:59:59.999000"),
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
            "2020-01-01",
            "2020-01-01 23:59:59.999000",
        ),
        (
            "2020-01-01",
            "2020-01-01 12:00:00",
            "2020-01-01",
            "2020-01-01 11:59:59.99900",
        ),
        (
            "2020-01-01",
            to_datetime("2020-01-02"),
            "2020-01-01",
            "2020-01-01 23:59:59.999000",
        ),
    ],
)
def test_make_inclusive(start_in, end_in, start_out, end_out) -> None:
    assert make_inclusive(start_in, end_in) == (
        to_datetime(start_out),
        to_datetime(end_out),
    )


@pytest.mark.parametrize(
    "expression, result",
    [
        ("1 second ago", False),
        ("1 minute ago", False),
        ("1 hour ago", False),
        ("1 day ago", True),
        ("1 week ago", True),
        ("1 month ago", True),
        ("1 year ago", True),
        ("1 decade ago", True),
        ("3 hours ago", False),
        ("24 hours ago", False),
        ("1 day 5 hours ago", False),
        ("1 year 5 minutes ago", False),
        ("2023-01-01", False),
        ("2023-01-01 12:00:00", False),
        ("yesterday", True),
        ("today", True),
        ("tomorrow", True),
    ],
)
def test_is_catagorical_relative_expression(expression, result):
    assert is_catagorical_relative_expression(expression) == result
