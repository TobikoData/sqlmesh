# type: ignore
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
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
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
    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "DELETE FROM `test_table` WHERE `ds` BETWEEN '2022-01-01' AND '2022-01-05'",
        "INSERT INTO `test_table` (`a`, `ds`) SELECT `a`, `ds` FROM `tbl`",
    ]


def test_replace_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "CREATE OR REPLACE TABLE `test_table` AS SELECT `a` FROM `tbl`"
    ]


def test_replace_query_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "CREATE OR REPLACE TABLE `test_table` AS SELECT CAST(`a` AS INT64) AS `a`, CAST(`b` AS INT64) AS `b` FROM UNNEST([STRUCT(CAST(1 AS INT64) AS `a`, CAST(4 AS INT64) AS `b`), STRUCT(2 AS `a`, 5 AS `b`), STRUCT(3 AS `a`, 6 AS `b`)])"
    ]
