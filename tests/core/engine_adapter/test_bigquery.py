# type: ignore
import sys

import pandas as pd
import pytest
from google.cloud import bigquery
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import BigQueryEngineAdapter
from sqlmesh.core.node import IntervalUnit
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
        time_formatter=lambda x, _: exp.Literal.string(x.strftime("%Y-%m-%d")),
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
        "MERGE INTO `test_table` AS `__MERGE_TARGET__` USING (SELECT * FROM (SELECT `a`, `ds` FROM `tbl`) AS `_subquery` WHERE `ds` BETWEEN '2022-01-01' AND '2022-01-05') AS `__MERGE_SOURCE__` ON FALSE WHEN NOT MATCHED BY SOURCE AND `ds` BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)"
    ]


def test_insert_overwrite_by_time_partition_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    get_temp_bq_table = mocker.Mock()
    get_temp_bq_table.return_value = AttributeDict(
        {"project": "project", "dataset_id": "dataset", "table_id": "temp_table"}
    )
    adapter._BigQueryEngineAdapter__get_temp_bq_table = get_temp_bq_table
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    db_call_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._db_call"
    )
    retry_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._BigQueryEngineAdapter__retry"
    )
    retry_resp = mocker.MagicMock()
    retry_resp_call = mocker.MagicMock()
    retry_resp.return_value = retry_resp_call
    retry_resp_call.errors = None
    retry_mock.return_value = retry_resp
    db_call_mock.return_value = AttributeDict({"errors": None})
    df = pd.DataFrame({"a": [1, 2, 3], "ds": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
        start="2022-01-01",
        end="2022-01-05",
        time_formatter=lambda x, _: exp.Literal.string(x.strftime("%Y-%m-%d")),
        time_column="ds",
        columns_to_types={
            "a": exp.DataType.build("int"),
            "ds": exp.DataType.build("string"),
        },
    )
    assert execute_mock.call_count == 2
    assert retry_resp.call_count == 1
    assert db_call_mock.call_count == 1
    create_temp_table = db_call_mock.call_args_list[0]
    load_temp_table = retry_resp.call_args_list[0]
    merge, drop_temp_table = execute_mock.call_args_list
    merge_sql = merge[0][0]
    drop_temp_table_sql = drop_temp_table[0][0]
    if sys.version_info < (3, 8):
        create_temp_table.kwargs = create_temp_table[1]
        load_temp_table.kwargs = load_temp_table[1]
        drop_temp_table.kwargs = drop_temp_table[1]
    assert create_temp_table.kwargs == {
        "exists_ok": False,
        "table": get_temp_bq_table.return_value,
    }
    assert sorted(load_temp_table.kwargs) == [
        "df",
        "job_config",
        "table",
    ]
    assert load_temp_table.kwargs["df"].equals(df)
    assert load_temp_table.kwargs["table"] == get_temp_bq_table.return_value
    assert load_temp_table.kwargs["job_config"].write_disposition == None
    assert (
        merge_sql.sql(dialect="bigquery")
        == "MERGE INTO test_table AS __MERGE_TARGET__ USING (SELECT * FROM (SELECT a, ds FROM project.dataset.temp_table) AS _subquery WHERE ds BETWEEN '2022-01-01' AND '2022-01-05') AS __MERGE_SOURCE__ ON FALSE WHEN NOT MATCHED BY SOURCE AND ds BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (a, ds) VALUES (a, ds)"
    )
    assert (
        drop_temp_table_sql.sql(dialect="bigquery")
        == "DROP TABLE IF EXISTS project.dataset.temp_table"
    )


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
    get_bq_table = mocker.Mock()
    get_bq_table.return_value = AttributeDict(
        {"project": "project", "dataset_id": "dataset", "table_id": "test_table"}
    )
    adapter._BigQueryEngineAdapter__get_bq_table = get_bq_table
    db_call_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._db_call"
    )
    retry_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._BigQueryEngineAdapter__retry"
    )
    retry_resp = mocker.MagicMock()
    retry_resp_call = mocker.MagicMock()
    retry_resp.return_value = retry_resp_call
    retry_resp_call.errors = None
    retry_mock.return_value = retry_resp
    db_call_mock.return_value = AttributeDict({"errors": None})
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")}
    )

    assert db_call_mock.call_count == 1
    assert retry_resp.call_count == 1
    create_table = db_call_mock.call_args_list[0]
    load_table = retry_resp.call_args_list[0]
    if sys.version_info < (3, 8):
        create_table.kwargs = create_table[1]
        load_table.kwargs = load_table[1]
    assert create_table.kwargs == {
        "table": get_bq_table.return_value,
        "exists_ok": True,
    }
    assert sorted(load_table.kwargs) == [
        "df",
        "job_config",
        "table",
    ]
    assert load_table.kwargs["df"].equals(df)
    assert load_table.kwargs["table"] == get_bq_table.return_value
    assert (
        load_table.kwargs["job_config"].write_disposition
        == bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    assert load_table.kwargs["job_config"].schema == [
        bigquery.SchemaField("a", "INT64"),
        bigquery.SchemaField("b", "INT64"),
    ]


@pytest.mark.parametrize(
    "partition_by_cols, partition_by_statement",
    [
        ([exp.to_column("ds")], "`ds`"),
        ([d.parse_one("DATE_TRUNC(ds, MONTH)", dialect="bigquery")], "DATE_TRUNC(`ds`, MONTH)"),
    ],
)
def test_create_table_date_partition(
    partition_by_cols, partition_by_statement, mocker: MockerFixture
):
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
        partitioned_by=partition_by_cols,
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=["b"],
    )

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery", identify=True)
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery", identify=True)
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) PARTITION BY {partition_by_statement} CLUSTER BY `b`"
    ]


@pytest.mark.parametrize(
    "partition_by_cols, partition_by_statement",
    [
        ([exp.to_column("ds")], "TIMESTAMP_TRUNC(`ds`, HOUR)"),
        (
            [d.parse_one("TIMESTAMP_TRUNC(ds, HOUR)", dialect="bigquery")],
            "TIMESTAMP_TRUNC(`ds`, HOUR)",
        ),
    ],
)
def test_create_table_time_partition(
    partition_by_cols, partition_by_statement, mocker: MockerFixture
):
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
        partitioned_by=partition_by_cols,
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
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) PARTITION BY {partition_by_statement}"
    ]


def test_merge(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.merge(
        target_table="target",
        source_table="SELECT id, ts, val FROM source",
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=["id"],
    )
    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery")
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery")
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "MERGE INTO target AS __MERGE_TARGET__ USING (SELECT id, ts, val FROM source) AS __MERGE_SOURCE__ ON __MERGE_TARGET__.id = __MERGE_SOURCE__.id "
        "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.id = __MERGE_SOURCE__.id, __MERGE_TARGET__.ts = __MERGE_SOURCE__.ts, __MERGE_TARGET__.val = __MERGE_SOURCE__.val "
        "WHEN NOT MATCHED THEN INSERT (id, ts, val) VALUES (__MERGE_SOURCE__.id, __MERGE_SOURCE__.ts, __MERGE_SOURCE__.val)"
    ]

    execute_mock.reset_mock()
    get_temp_bq_table = mocker.Mock()
    get_temp_bq_table.return_value = AttributeDict(
        {"project": "project", "dataset_id": "dataset", "table_id": "temp_table"}
    )
    adapter._BigQueryEngineAdapter__get_temp_bq_table = get_temp_bq_table
    db_call_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._db_call"
    )
    retry_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter._BigQueryEngineAdapter__retry"
    )
    retry_resp = mocker.MagicMock()
    retry_resp_call = mocker.MagicMock()
    retry_resp.return_value = retry_resp_call
    retry_resp_call.errors = None
    retry_mock.return_value = retry_resp
    db_call_mock.return_value = AttributeDict({"errors": None})
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("INT"),
        },
        unique_key=["id"],
    )

    sql_calls = [
        # Python 3.7 support
        call[0][0].sql(dialect="bigquery")
        if isinstance(call[0], tuple)
        else call[0].sql(dialect="bigquery")
        for call in execute_mock.call_args_list
    ]
    assert sql_calls == [
        "MERGE INTO target AS __MERGE_TARGET__ USING (SELECT id, ts, val FROM project.dataset.temp_table) AS __MERGE_SOURCE__ ON __MERGE_TARGET__.id = __MERGE_SOURCE__.id "
        "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.id = __MERGE_SOURCE__.id, __MERGE_TARGET__.ts = __MERGE_SOURCE__.ts, __MERGE_TARGET__.val = __MERGE_SOURCE__.val "
        "WHEN NOT MATCHED THEN INSERT (id, ts, val) VALUES (__MERGE_SOURCE__.id, __MERGE_SOURCE__.ts, __MERGE_SOURCE__.val)",
        "DROP TABLE IF EXISTS project.dataset.temp_table",
    ]
    assert retry_resp.call_count == 1
    assert db_call_mock.call_count == 1
    create_temp_table = db_call_mock.call_args_list[0]
    load_temp_table = retry_resp.call_args_list[0]
    if sys.version_info < (3, 8):
        create_temp_table.kwargs = create_temp_table[1]
        load_temp_table.kwargs = load_temp_table[1]
    assert create_temp_table.kwargs == {
        "exists_ok": False,
        "table": get_temp_bq_table.return_value,
    }
    assert sorted(load_temp_table.kwargs) == [
        "df",
        "job_config",
        "table",
    ]
