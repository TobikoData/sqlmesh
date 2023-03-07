# type: ignore
import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import BigQueryEngineAdapter
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.utils import AttributeDict


def test_insert_overwrite_by_time_partition_query(mocker: MockerFixture):
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
        "MERGE INTO `test_table` AS `__MERGE_TARGET__` USING (SELECT `a`, `ds` FROM `tbl`) AS __MERGE_SOURCE__ ON FALSE WHEN NOT MATCHED BY SOURCE AND `ds` BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)",
    ]


def test_insert_overwrite_by_time_partition_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    load_pandas_to_temp_table_mock = mocker.Mock()
    load_pandas_to_temp_table_mock.return_value = (AttributeDict({"errors": None}), "temp_table")
    adapter._BigQueryEngineAdapter__load_pandas_to_temp_table = load_pandas_to_temp_table_mock
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    drop_table_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.drop_table"
    )
    df = pd.DataFrame({"a": [1, 2, 3], "ds": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
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
        "MERGE INTO `test_table` AS `__MERGE_TARGET__` USING (SELECT `a`, `ds` FROM `temp_table`) AS __MERGE_SOURCE__ ON FALSE WHEN NOT MATCHED BY SOURCE AND `ds` BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)",
    ]
    drop_table_mock.assert_called_once_with("temp_table")


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
    assert sql_calls == ["CREATE OR REPLACE TABLE `test_table` AS SELECT `a` FROM `tbl`"]


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


def test_create_table_date_partition(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.create_table(
        "test_table",
        {"a": "int", "b": "int"},
        partitioned_by=["ds"],
        partition_interval_unit=IntervalUnit.DAY,
    )

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) PARTITION BY `ds`"
    ]


def test_create_table_time_partition(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.create_table(
        "test_table",
        {"a": "int", "b": "int"},
        partitioned_by=["ds"],
        partition_interval_unit=IntervalUnit.HOUR,
    )

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) PARTITION BY TIMESTAMP_TRUNC(`ds`, HOUR)"
    ]
