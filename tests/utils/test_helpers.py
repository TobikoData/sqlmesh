import pytest
from sqlglot import expressions

from sqlmesh.utils import columns_to_types_all_known


@pytest.mark.parametrize(
    "columns_to_types, expected",
    [
        ({"a": expressions.DataType.build("INT"), "b": expressions.DataType.build("INT")}, True),
        (
            {"a": expressions.DataType.build("UNKNOWN"), "b": expressions.DataType.build("INT")},
            False,
        ),
        ({"a": expressions.DataType.build("NULL"), "b": expressions.DataType.build("INT")}, False),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build(
                    "STRUCT<sub_a INT, sub_b INT, sub_c INT, sub_d INT>"
                ),
            },
            True,
        ),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build(
                    "ARRAY<STRUCT<sub_a INT, sub_b INT, sub_c INT, sub_d INT>>"
                ),
            },
            True,
        ),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build(
                    "ARRAY<STRUCT<sub_a INT, sub_b INT, sub_c INT, sub_d UNKNOWN>>"
                ),
            },
            False,
        ),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build(
                    "ARRAY<STRUCT<sub_a INT, sub_b INT, sub_c INT, sub_d UNKNOWN>>"
                ),
            },
            False,
        ),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build("MAP<INT, STRING>"),
            },
            True,
        ),
        (
            {
                "a": expressions.DataType.build("INT"),
                "b": expressions.DataType.build("MAP<INT, UNKNOWN>"),
            },
            False,
        ),
    ],
)
def test_columns_to_types_all_known(columns_to_types, expected) -> None:
    assert columns_to_types_all_known(columns_to_types) == expected
