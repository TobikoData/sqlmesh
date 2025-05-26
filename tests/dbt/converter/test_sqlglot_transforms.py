from sqlglot import parse_one
import sqlmesh.dbt.converter.sqlglot_transforms as st
import pytest


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            "select * from foo where ds between '@start_dt' and '@end_dt'",
            "SELECT * FROM foo WHERE ds BETWEEN @start_dt AND @end_dt",
        ),
        (
            "select * from foo where bar <> '@unrelated'",
            "SELECT * FROM foo WHERE bar <> '@unrelated'",
        ),
    ],
)
def test_unwrap_macros_in_string_literals(input: str, expected: str) -> None:
    assert parse_one(input).transform(st.unwrap_macros_in_string_literals()).sql() == expected
