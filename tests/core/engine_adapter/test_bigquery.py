# type: ignore
from unittest.mock import call

import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import BigQueryEngineAdapter


def test_insert_overwrite_by_time_partition(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, ds FROM tbl"),
        start="2022-01-01",
        end="2022-01-05",
        time_formatter=lambda x: exp.Literal.string(x.strftime("%Y-%m-%d")),
        time_column="ds",
        columns_to_types={
            "a": exp.DataType.build("int"),
            "ds": exp.DataType.build("string"),
        },
    )

    cursor_mock.execute.assert_has_calls(
        [
            call(
                "DELETE FROM `test_table` WHERE `ds` BETWEEN '2022-01-01' AND '2022-01-05'",
                job_config=None,
            ),
            call(
                "INSERT INTO `test_table` (`a`, `ds`) SELECT `a`, `ds` FROM `tbl`",
                job_config=None,
            ),
        ]
    )


def test_replace_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    cursor_mock.execute.assert_called_once_with(
        "CREATE OR REPLACE TABLE `test_table` AS SELECT `a` FROM `tbl`", job_config=None
    )


def test_replace_query_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    cursor_mock.execute.assert_called_once_with(
        "CREATE OR REPLACE TABLE `test_table` AS SELECT CAST(`a` AS int) AS `a`, CAST(`b` AS int) AS `b` FROM UNNEST([STRUCT(CAST(1 AS int) AS `a`, CAST(4 AS int) AS `b`), STRUCT(2 AS `a`, 5 AS `b`), STRUCT(3 AS `a`, 6 AS `b`)])",
        job_config=None,
    )
