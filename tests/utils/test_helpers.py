import pytest
from functools import wraps
from sqlglot import expressions
from sqlglot.optimizer.annotate_types import annotate_types

from sqlmesh.core.console import set_console, get_console, TerminalConsole

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
        (
            {"a": annotate_types(expressions.DataType.build("VARCHAR(MAX)", dialect="redshift"))},
            True,
        ),
    ],
)
def test_columns_to_types_all_known(columns_to_types, expected) -> None:
    assert columns_to_types_all_known(columns_to_types) == expected


def use_terminal_console(func):
    @wraps(func)
    def test_wrapper(*args, **kwargs):
        orig_console = get_console()
        try:
            set_console(TerminalConsole())
            func(*args, **kwargs)
        finally:
            set_console(orig_console)

    return test_wrapper
