# type: ignore
import sys
import typing as t

import pandas as pd
import pytest
from google.cloud import bigquery
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one
from sqlglot.helper import ensure_list

import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import BigQueryEngineAdapter
from sqlmesh.core.engine_adapter.bigquery import select_partitions_expr
from sqlmesh.core.node import IntervalUnit
from sqlmesh.utils import AttributeDict

pytestmark = [pytest.mark.bigquery, pytest.mark.engine]


def test_insert_overwrite_by_time_partition_query(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)
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
    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "MERGE INTO `test_table` AS `__MERGE_TARGET__` USING (SELECT `a`, `ds` FROM (SELECT `a`, `ds` FROM `tbl`) AS `_subquery` WHERE `ds` BETWEEN '2022-01-01' AND '2022-01-05') AS `__MERGE_SOURCE__` ON FALSE WHEN NOT MATCHED BY SOURCE AND `ds` BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)"
    ]


def test_insert_overwrite_by_partition_query(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter._default_catalog = "test_project"
    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_schema.test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    adapter.insert_overwrite_by_partition(
        table_name,
        parse_one("SELECT a, ds FROM tbl"),
        partitioned_by=[
            d.parse_one("DATETIME_TRUNC(ds, MONTH)"),
        ],
        columns_to_types={
            "a": exp.DataType.build("int"),
            "ds": exp.DataType.build("DATETIME"),
        },
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE SCHEMA IF NOT EXISTS `test_schema`",
        f"CREATE TABLE IF NOT EXISTS `test_schema`.`__temp_test_table_{temp_table_id}` PARTITION BY DATETIME_TRUNC(`ds`, MONTH) AS SELECT `a`, `ds` FROM `tbl`",
        f"DECLARE _sqlmesh_target_partitions_ ARRAY<DATETIME> DEFAULT (SELECT ARRAY_AGG(PARSE_DATETIME('%Y%m', partition_id)) FROM `test_project`.`test_schema`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = '__temp_test_table_{temp_table_id}' AND NOT partition_id IS NULL AND partition_id <> '__NULL__');",
        f"MERGE INTO `test_schema`.`test_table` AS `__MERGE_TARGET__` USING (SELECT `a`, `ds` FROM (SELECT * FROM `test_schema`.`__temp_test_table_{temp_table_id}`) AS `_subquery` WHERE DATETIME_TRUNC(`ds`, MONTH) IN UNNEST(`_sqlmesh_target_partitions_`)) AS `__MERGE_SOURCE__` ON FALSE WHEN NOT MATCHED BY SOURCE AND DATETIME_TRUNC(`ds`, MONTH) IN UNNEST(`_sqlmesh_target_partitions_`) THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)",
        f"DROP TABLE IF EXISTS `test_schema`.`__temp_test_table_{temp_table_id}`",
    ]


def test_insert_overwrite_by_partition_query_unknown_column_types(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    columns_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.columns"
    )
    columns_mock.return_value = {
        "a": exp.DataType.build("int"),
        "ds": exp.DataType.build("DATETIME"),
    }
    adapter._default_catalog = "test_project"
    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_schema.test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    adapter.insert_overwrite_by_partition(
        table_name,
        parse_one("SELECT a, ds FROM tbl"),
        partitioned_by=[
            d.parse_one("DATETIME_TRUNC(ds, MONTH)"),
        ],
        columns_to_types={
            "a": exp.DataType.build("unknown"),
            "ds": exp.DataType.build("UNKNOWN"),
        },
    )

    columns_mock.assert_called_once_with(
        exp.to_table(f"test_schema.__temp_test_table_{temp_table_id}")
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE SCHEMA IF NOT EXISTS `test_schema`",
        f"CREATE TABLE IF NOT EXISTS `test_schema`.`__temp_test_table_{temp_table_id}` PARTITION BY DATETIME_TRUNC(`ds`, MONTH) AS SELECT `a`, `ds` FROM `tbl`",
        f"DECLARE _sqlmesh_target_partitions_ ARRAY<DATETIME> DEFAULT (SELECT ARRAY_AGG(PARSE_DATETIME('%Y%m', partition_id)) FROM `test_project`.`test_schema`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = '__temp_test_table_{temp_table_id}' AND NOT partition_id IS NULL AND partition_id <> '__NULL__');",
        f"MERGE INTO `test_schema`.`test_table` AS `__MERGE_TARGET__` USING (SELECT `a`, `ds` FROM (SELECT * FROM `test_schema`.`__temp_test_table_{temp_table_id}`) AS `_subquery` WHERE DATETIME_TRUNC(`ds`, MONTH) IN UNNEST(`_sqlmesh_target_partitions_`)) AS `__MERGE_SOURCE__` ON FALSE WHEN NOT MATCHED BY SOURCE AND DATETIME_TRUNC(`ds`, MONTH) IN UNNEST(`_sqlmesh_target_partitions_`) THEN DELETE WHEN NOT MATCHED THEN INSERT (`a`, `ds`) VALUES (`a`, `ds`)",
        f"DROP TABLE IF EXISTS `test_schema`.`__temp_test_table_{temp_table_id}`",
    ]


def test_insert_overwrite_by_time_partition_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    temp_table_exists_counter = 0

    def temp_table_exists(table: exp.Table) -> bool:
        nonlocal temp_table_exists_counter
        temp_table_exists_counter += 1
        if table.sql() == "project.dataset.temp_table" and temp_table_exists_counter == 1:
            return False
        return True

    mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.table_exists",
        side_effect=temp_table_exists,
    )

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
    assert load_temp_table.kwargs["job_config"].write_disposition is None
    assert (
        merge_sql.sql(dialect="bigquery")
        == "MERGE INTO test_table AS __MERGE_TARGET__ USING (SELECT `a`, `ds` FROM (SELECT `a`, `ds` FROM project.dataset.temp_table) AS _subquery WHERE ds BETWEEN '2022-01-01' AND '2022-01-05') AS __MERGE_SOURCE__ ON FALSE WHEN NOT MATCHED BY SOURCE AND ds BETWEEN '2022-01-01' AND '2022-01-05' THEN DELETE WHEN NOT MATCHED THEN INSERT (a, ds) VALUES (a, ds)"
    )
    assert (
        drop_temp_table_sql.sql(dialect="bigquery")
        == "DROP TABLE IF EXISTS project.dataset.temp_table"
    )


def test_replace_query(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.replace_query(
        "test_table", parse_one("SELECT a FROM tbl"), {"a": exp.DataType.build("INT")}
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE OR REPLACE TABLE `test_table` AS SELECT CAST(`a` AS INT64) AS `a` FROM (SELECT `a` FROM `tbl`) AS `_subquery`"
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    get_bq_table_value = AttributeDict(
        {"project": "project", "dataset_id": "dataset", "table_id": "temp_table"}
    )
    get_bq_table = mocker.Mock()
    get_bq_table.return_value = get_bq_table_value
    adapter._BigQueryEngineAdapter__get_bq_table = get_bq_table
    temp_table_exists_counter = 0

    def temp_table_exists(table: exp.Table) -> bool:
        nonlocal temp_table_exists_counter
        temp_table_exists_counter += 1
        if table.sql() == "project.dataset.temp_table" and temp_table_exists_counter == 1:
            return False
        return True

    mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.table_exists",
        side_effect=temp_table_exists,
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

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")}
    )

    assert db_call_mock.call_count == 1
    assert retry_resp.call_count == 1
    create_table = db_call_mock.call_args_list[0]
    load_table = retry_resp.call_args_list[0]
    assert create_table.kwargs == {
        "table": get_bq_table_value,
        "exists_ok": False,
    }
    assert sorted(load_table.kwargs) == [
        "df",
        "job_config",
        "table",
    ]
    assert load_table.kwargs["df"].equals(df)
    assert load_table.kwargs["table"] == get_bq_table_value
    assert load_table.kwargs["job_config"].write_disposition is None
    assert load_table.kwargs["job_config"].schema == [
        bigquery.SchemaField("a", "INT64"),
        bigquery.SchemaField("b", "INT64"),
    ]
    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE OR REPLACE TABLE `test_table` AS SELECT CAST(`a` AS INT64) AS `a`, CAST(`b` AS INT64) AS `b` FROM (SELECT `a`, `b` FROM `project`.`dataset`.`temp_table`) AS `_subquery`",
        "DROP TABLE IF EXISTS `project`.`dataset`.`temp_table`",
    ]


@pytest.mark.parametrize(
    "partition_by_cols, partition_by_statement",
    [
        ([exp.to_column("ds")], "`ds`"),
        ([d.parse_one("DATE_TRUNC(ds, MONTH)", dialect="bigquery")], "DATE_TRUNC(`ds`, MONTH)"),
    ],
)
def test_create_table_date_partition(
    make_mocked_engine_adapter: t.Callable,
    partition_by_cols,
    partition_by_statement,
    mocker: MockerFixture,
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")},
        partitioned_by=partition_by_cols,
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=["b"],
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64, `b` INT64) PARTITION BY {partition_by_statement} CLUSTER BY `b`"
    ]


@pytest.mark.parametrize(
    "partition_by_cols, partition_column_type, partition_interval_unit, partition_by_statement",
    [
        ([exp.to_column("ds")], "date", IntervalUnit.FIVE_MINUTE, "`ds`"),
        ([exp.to_column("ds")], "date", IntervalUnit.HOUR, "`ds`"),
        ([exp.to_column("ds")], "date", IntervalUnit.DAY, "`ds`"),
        ([exp.to_column("ds")], "date", IntervalUnit.MONTH, "DATE_TRUNC(`ds`, MONTH)"),
        ([exp.to_column("ds")], "date", IntervalUnit.YEAR, "DATE_TRUNC(`ds`, YEAR)"),
        ([exp.to_column("ds")], "datetime", IntervalUnit.HOUR, "DATETIME_TRUNC(`ds`, HOUR)"),
        ([exp.to_column("ds")], "datetime", IntervalUnit.DAY, "DATETIME_TRUNC(`ds`, DAY)"),
        ([exp.to_column("ds")], "datetime", IntervalUnit.MONTH, "DATETIME_TRUNC(`ds`, MONTH)"),
        ([exp.to_column("ds")], "datetime", IntervalUnit.YEAR, "DATETIME_TRUNC(`ds`, YEAR)"),
        ([exp.to_column("ds")], "timestamp", IntervalUnit.HOUR, "TIMESTAMP_TRUNC(`ds`, HOUR)"),
        ([exp.to_column("ds")], "timestamp", IntervalUnit.DAY, "TIMESTAMP_TRUNC(`ds`, DAY)"),
        ([exp.to_column("ds")], "timestamp", IntervalUnit.MONTH, "TIMESTAMP_TRUNC(`ds`, MONTH)"),
        ([exp.to_column("ds")], "timestamp", IntervalUnit.YEAR, "TIMESTAMP_TRUNC(`ds`, YEAR)"),
        (
            [d.parse_one("TIMESTAMP_TRUNC(ds, HOUR)", dialect="bigquery")],
            "timestamp",
            IntervalUnit.HOUR,
            "TIMESTAMP_TRUNC(`ds`, HOUR)",
        ),
        (
            [d.parse_one("TIMESTAMP_TRUNC(ds, HOUR)", dialect="bigquery")],
            "timestamp",
            IntervalUnit.DAY,
            "TIMESTAMP_TRUNC(`ds`, HOUR)",
        ),
        (
            [d.parse_one("TIMESTAMP_TRUNC(ds, DAY)", dialect="bigquery")],
            "timestamp",
            IntervalUnit.FIVE_MINUTE,
            "TIMESTAMP_TRUNC(`ds`, DAY)",
        ),
    ],
)
def test_create_table_time_partition(
    make_mocked_engine_adapter: t.Callable,
    partition_by_cols,
    partition_column_type,
    partition_interval_unit,
    partition_by_statement,
    mocker: MockerFixture,
):
    partition_column_sql_type = exp.DataType.build(partition_column_type, dialect="bigquery")

    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.create_table(
        "test_table",
        {
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
            "ds": partition_column_sql_type,
        },
        partitioned_by=partition_by_cols,
        partition_interval_unit=partition_interval_unit,
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64, `b` INT64, `ds` {partition_column_sql_type.sql(dialect='bigquery')}) PARTITION BY {partition_by_statement}"
    ]


def test_ctas_time_partition(
    make_mocked_engine_adapter: t.Callable,
    mocker: MockerFixture,
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.ctas(
        "test_table",
        exp.select("*").from_("a"),
        partitioned_by=[exp.column("ds")],
        partition_interval_unit=IntervalUnit.HOUR,
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` PARTITION BY `ds` AS SELECT * FROM `a`",
    ]


def test_merge(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )
    adapter.merge(
        target_table="target",
        source_table=parse_one("SELECT id, ts, val FROM source"),
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=[exp.to_identifier("id")],
    )
    sql_calls = _to_sql_calls(execute_mock, identify=False)
    assert sql_calls == [
        "MERGE INTO target AS __MERGE_TARGET__ USING (SELECT id, ts, val FROM source) AS __MERGE_SOURCE__ ON __MERGE_TARGET__.id = __MERGE_SOURCE__.id "
        "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.id = __MERGE_SOURCE__.id, __MERGE_TARGET__.ts = __MERGE_SOURCE__.ts, __MERGE_TARGET__.val = __MERGE_SOURCE__.val "
        "WHEN NOT MATCHED THEN INSERT (id, ts, val) VALUES (__MERGE_SOURCE__.id, __MERGE_SOURCE__.ts, __MERGE_SOURCE__.val)"
    ]


def test_merge_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    temp_table_exists_counter = 0

    def temp_table_exists(table: exp.Table) -> bool:
        nonlocal temp_table_exists_counter
        temp_table_exists_counter += 1
        if table.sql() == "project.dataset.temp_table" and temp_table_exists_counter == 1:
            return False
        return True

    mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.table_exists",
        side_effect=temp_table_exists,
    )

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

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
        unique_key=[exp.to_identifier("id")],
    )

    sql_calls = _to_sql_calls(execute_mock, identify=False)
    assert sql_calls == [
        "MERGE INTO target AS __MERGE_TARGET__ USING (SELECT `id`, `ts`, `val` FROM project.dataset.temp_table) AS __MERGE_SOURCE__ ON __MERGE_TARGET__.id = __MERGE_SOURCE__.id "
        "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.id = __MERGE_SOURCE__.id, __MERGE_TARGET__.ts = __MERGE_SOURCE__.ts, __MERGE_TARGET__.val = __MERGE_SOURCE__.val "
        "WHEN NOT MATCHED THEN INSERT (id, ts, val) VALUES (__MERGE_SOURCE__.id, __MERGE_SOURCE__.ts, __MERGE_SOURCE__.val)",
        "DROP TABLE IF EXISTS project.dataset.temp_table",
    ]
    assert retry_resp.call_count == 1
    assert db_call_mock.call_count == 1
    create_temp_table = db_call_mock.call_args_list[0]
    load_temp_table = retry_resp.call_args_list[0]
    assert create_temp_table.kwargs == {
        "exists_ok": False,
        "table": get_temp_bq_table.return_value,
    }
    assert sorted(load_temp_table.kwargs) == [
        "df",
        "job_config",
        "table",
    ]


def test_begin_end_session(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    cursor_mock.connection = connection_mock
    connection_mock.cursor.return_value = cursor_mock

    query_result_mock = mocker.Mock()
    query_result_mock.total_rows = 0
    job_mock = mocker.Mock()
    job_mock.result.return_value = query_result_mock
    connection_mock._client.query.return_value = job_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock, job_retries=0)

    with adapter.session({}):
        assert adapter._connection_pool.get_attribute("session_id") is not None
        adapter.execute("SELECT 2;")

    assert adapter._connection_pool.get_attribute("session_id") is None
    adapter.execute("SELECT 3;")

    begin_session_call = connection_mock._client.query.call_args_list[0]
    assert begin_session_call[0][0] == "SELECT 1;"

    execute_a_call = connection_mock._client.query.call_args_list[1]
    assert execute_a_call[1]["query"] == "SELECT 2;"
    assert len(execute_a_call[1]["job_config"].connection_properties) == 1
    assert execute_a_call[1]["job_config"].connection_properties[0].key == "session_id"
    assert execute_a_call[1]["job_config"].connection_properties[0].value

    execute_b_call = connection_mock._client.query.call_args_list[2]
    assert execute_b_call[1]["query"] == "SELECT 3;"
    assert not execute_b_call[1]["job_config"].connection_properties


def _to_sql_calls(execute_mock: t.Any, identify: bool = True) -> t.List[str]:
    output = []
    for call in execute_mock.call_args_list:
        value = call[0][0]
        sql = (
            value.sql(dialect="bigquery", identify=identify)
            if isinstance(value, exp.Expression)
            else str(value)
        )
        output.append(sql)
    return output


def test_create_table_table_options(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")},
        table_properties={
            "partition_expiration_days": exp.convert(7),
        },
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64, `b` INT64) OPTIONS (partition_expiration_days=7)"
    ]


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    allowed_table_comment_length = BigQueryEngineAdapter.MAX_TABLE_COMMENT_LENGTH
    truncated_table_comment = "a" * allowed_table_comment_length
    long_table_comment = truncated_table_comment + "b"

    allowed_column_comment_length = BigQueryEngineAdapter.MAX_COLUMN_COMMENT_LENGTH
    truncated_column_comment = "c" * allowed_column_comment_length
    long_column_comment = truncated_column_comment + "d"

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description="\\",
        column_descriptions={"a": "\\"},
    )

    adapter.ctas(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.create_view(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        table_description=long_table_comment,
    )

    adapter._create_table_comment(
        "test_table",
        long_table_comment,
    )

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64 OPTIONS (description='{truncated_column_comment}'), `b` INT64) OPTIONS (description='{truncated_table_comment}')",
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64 OPTIONS (description='\\\\'), `b` INT64) OPTIONS (description='\\\\')",
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` INT64 OPTIONS (description='{truncated_column_comment}'), `b` INT64) OPTIONS (description='{truncated_table_comment}') AS SELECT CAST(`a` AS INT64) AS `a`, CAST(`b` AS INT64) AS `b` FROM (SELECT `a`, `b` FROM `source_table`) AS `_subquery`",
        f"CREATE OR REPLACE VIEW `test_table` OPTIONS (description='{truncated_table_comment}') AS SELECT `a`, `b` FROM `source_table`",
        f"ALTER TABLE `test_table` SET OPTIONS(description = '{truncated_table_comment}')",
    ]


def test_select_partitions_expr():
    assert (
        select_partitions_expr(
            "{{ adapter.resolve_schema(this) }}",
            "{{ adapter.resolve_identifier(this) }}",
            "date",
            granularity="day",
            catalog="{{ target.database }}",
        )
        == "SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) FROM `{{ target.database }}`.`{{ adapter.resolve_schema(this) }}`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = '{{ adapter.resolve_identifier(this) }}' AND NOT partition_id IS NULL AND partition_id <> '__NULL__'"
    )

    assert (
        select_partitions_expr(
            "test_schema",
            "test_table",
            "int64",
        )
        == "SELECT MAX(CAST(partition_id AS INT64)) FROM `test_schema`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = 'test_table' AND NOT partition_id IS NULL AND partition_id <> '__NULL__'"
    )


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {
                "schema_name": "test_schema",
            },
            "DROP SCHEMA IF EXISTS `test_schema`",
        ),
        (
            {
                "schema_name": "test_schema",
                "ignore_if_not_exists": False,
            },
            "DROP SCHEMA `test_schema`",
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
            },
            "DROP SCHEMA IF EXISTS `test_schema` CASCADE",
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
                "ignore_if_not_exists": False,
            },
            "DROP SCHEMA `test_schema` CASCADE",
        ),
        (
            {
                "schema_name": "test_catalog.test_schema",
                "ignore_if_not_exists": True,
                "cascade": True,
            },
            "DROP SCHEMA IF EXISTS `test_catalog`.`test_schema` CASCADE",
        ),
    ],
)
def test_drop_schema(
    kwargs, expected, make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)

    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    adapter.drop_schema(**kwargs)

    sql_calls = _to_sql_calls(execute_mock)

    assert sql_calls == ensure_list(expected)


def test_view_properties(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    adapter.create_view(
        "test_table",
        parse_one("SELECT 1"),
        table_description="some description",
        view_properties={
            "labels": exp.array("('test-label', 'label-value')"),
        },
    )

    adapter.create_view(
        "test_table",
        parse_one("SELECT 1"),
        table_description="some description",
        view_properties={
            "labels": exp.array("('test-view-label', 'label-view-value')"),
        },
    )

    adapter.create_view("test_table", parse_one("SELECT 1"), view_properties={})

    sql_calls = _to_sql_calls(execute_mock)
    assert sql_calls == [
        "CREATE OR REPLACE VIEW `test_table` OPTIONS (description='some description', labels=[('test-label', 'label-value')]) AS SELECT 1",
        "CREATE OR REPLACE VIEW `test_table` OPTIONS (description='some description', labels=[('test-view-label', 'label-view-value')]) AS SELECT 1",
        "CREATE OR REPLACE VIEW `test_table` AS SELECT 1",
    ]


def test_materialized_view_properties(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(BigQueryEngineAdapter)
    execute_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.bigquery.BigQueryEngineAdapter.execute"
    )

    adapter.create_view(
        "test_table",
        parse_one("SELECT 1"),
        materialized=True,
        materialized_properties={
            "partitioned_by": [exp.column("ds")],
            "clustered_by": ["a"],
            "partition_interval_unit": IntervalUnit.DAY,
        },
    )

    sql_calls = _to_sql_calls(execute_mock)
    # https://cloud.google.com/bigquery/docs/materialized-views-create#example_1
    assert sql_calls == [
        "CREATE OR REPLACE MATERIALIZED VIEW `test_table` PARTITION BY `ds` CLUSTER BY `a` AS SELECT 1",
    ]
