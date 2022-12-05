from datetime import date, datetime

import pytest

from sqlmesh.utils.date import UTC, make_inclusive, to_datetime, to_timestamp


def test_to_datetime() -> None:
    target = datetime(2020, 1, 1).replace(tzinfo=UTC)
    assert to_datetime("2020-01-01 00:00:00") == target
    assert to_datetime("2020-01-01T00:00:00") == target
    assert to_datetime("2020-01-01T05:00:00+05:00") == target
    assert to_datetime("2020-01-01") == target
    assert to_datetime(datetime(2020, 1, 1)) == target
    assert to_datetime("1577836800") == target
    assert to_datetime(1577836800) == target
    assert to_datetime(1577836800.0) == target
    assert to_datetime(1577836800000) == target
    assert to_datetime(20200101) == target
    assert to_datetime("20200101") == target
    assert to_datetime("0") == datetime(1970, 1, 1, tzinfo=UTC)


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
