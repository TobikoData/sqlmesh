# type: ignore
import typing as t

import pandas as pd  # noqa: TID253
import pytest
from pytest_mock import MockFixture
from sqlglot import exp, parse_one

from sqlmesh.core import dialect as d
from sqlmesh.core.engine_adapter import DatabricksEngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType
from sqlmesh.core.node import IntervalUnit
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.databricks, pytest.mark.engine]


def test_replace_query_not_exists(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    adapter.replace_query(
        "test_table", parse_one("SELECT a FROM tbl"), {"a": exp.DataType.build("INT")}
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` AS SELECT CAST(`a` AS INT) AS `a` FROM (SELECT `a` FROM `tbl`) AS `_subquery`",
    ]


def test_replace_query_exists(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.table_exists",
        return_value=True,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    mocker.patch.object(
        adapter,
        "_get_data_objects",
        return_value=[DataObject(schema="", name="test_table", type="table")],
    )
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    assert to_sql_calls(adapter) == [
        "INSERT INTO `test_table` REPLACE WHERE TRUE SELECT `a` FROM `tbl`",
    ]


def test_replace_query_pandas_not_exists(
    mocker: MockFixture, make_mocked_engine_adapter: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` AS SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `t`(`a`, `b`)) AS `_subquery`",
    ]


def test_replace_query_pandas_exists(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.table_exists",
        return_value=True,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    mocker.patch.object(
        adapter,
        "_get_data_objects",
        return_value=[DataObject(schema="", name="test_table", type="table")],
    )
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")}
    )

    assert to_sql_calls(adapter) == [
        "INSERT INTO `test_table` REPLACE WHERE TRUE SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `t`(`a`, `b`)",
    ]


def test_clone_table(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    adapter.clone_table("target_table", "source_table")
    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `target_table` SHALLOW CLONE `source_table`"
    )


def test_set_current_catalog(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    adapter.set_current_catalog("test_catalog2")

    assert to_sql_calls(adapter) == ["USE CATALOG `test_catalog2`"]


def test_get_current_catalog(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    adapter.cursor.fetchone.return_value = ("test_catalog",)

    assert adapter.get_current_catalog() == "test_catalog"
    assert to_sql_calls(adapter) == ["SELECT CURRENT_CATALOG()"]


def test_get_current_database(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")
    adapter.cursor.fetchone.return_value = ("test_database",)

    assert adapter.get_current_database() == "test_database"
    assert to_sql_calls(adapter) == ["SELECT CURRENT_DATABASE()"]


def test_insert_overwrite_by_partition_query(
    make_mocked_engine_adapter: t.Callable, mocker: MockFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_schema.test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    adapter.insert_overwrite_by_partition(
        table_name,
        parse_one("SELECT a, ds, b FROM tbl"),
        partitioned_by=[
            d.parse_one("DATETIME_TRUNC(ds, MONTH)"),
            d.parse_one("b"),
        ],
        target_columns_to_types={
            "a": exp.DataType.build("int"),
            "ds": exp.DataType.build("DATETIME"),
            "b": exp.DataType.build("boolean"),
        },
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "CREATE TABLE `test_schema`.`temp_test_table_abcdefgh` AS SELECT CAST(`a` AS INT) AS `a`, CAST(`ds` AS TIMESTAMP) AS `ds`, CAST(`b` AS BOOLEAN) AS `b` FROM (SELECT `a`, `ds`, `b` FROM `tbl`) AS `_subquery`",
        "INSERT INTO `test_schema`.`test_table` REPLACE WHERE CONCAT_WS('__SQLMESH_DELIM__', DATE_TRUNC('MONTH', `ds`), `b`) IN (SELECT DISTINCT CONCAT_WS('__SQLMESH_DELIM__', DATE_TRUNC('MONTH', `ds`), `b`) FROM `test_schema`.`temp_test_table_abcdefgh`) SELECT `a`, `ds`, `b` FROM `test_schema`.`temp_test_table_abcdefgh`",
        "DROP TABLE IF EXISTS `test_schema`.`temp_test_table_abcdefgh`",
    ]


def test_materialized_view_properties(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")

    adapter.create_view(
        "test_table",
        parse_one("SELECT 1"),
        materialized=True,
        materialized_properties={
            "partitioned_by": [exp.column("ds")],
            # Clustered by is not supported so we are confirming it is ignored
            "clustered_by": [exp.column("a")],
            "partition_interval_unit": IntervalUnit.DAY,
        },
    )

    sql_calls = to_sql_calls(adapter)
    # https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view.html#syntax
    assert sql_calls == [
        "CREATE OR REPLACE MATERIALIZED VIEW `test_table` PARTITIONED BY (`ds`) AS SELECT 1",
    ]


def test_create_table_clustered_by(mocker: MockFixture, make_mocked_engine_adapter: t.Callable):
    mocker.patch(
        "sqlmesh.core.engine_adapter.databricks.DatabricksEngineAdapter.set_current_catalog"
    )
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter, default_catalog="test_catalog")

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        clustered_by=[exp.column("cola")],
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING) CLUSTER BY (`cola`)",
    ]


def test_get_data_objects_distinguishes_view_types(mocker):
    adapter = DatabricksEngineAdapter(lambda: None, default_catalog="test_catalog")

    # (Databricks requires DBSQL Serverless or Pro warehouse to test materialized views which we do not have setup)
    # so this mocks the fetchdf call to simulate the response we would expect from the correct SQL query
    mock_df = pd.DataFrame(
        [
            {
                "name": "regular_view",
                "schema": "test_schema",
                "catalog": "test_catalog",
                "type": "view",
            },
            {
                "name": "mat_view",
                "schema": "test_schema",
                "catalog": "test_catalog",
                "type": "materialized_view",
            },
            {
                "name": "regular_table",
                "schema": "test_schema",
                "catalog": "test_catalog",
                "type": "table",
            },
        ]
    )

    mocker.patch.object(adapter, "fetchdf", return_value=mock_df)

    data_objects = adapter._get_data_objects(
        schema_name=exp.Table(db="test_schema", catalog="test_catalog")
    )

    adapter.fetchdf.assert_called_once()
    call_args = adapter.fetchdf.call_args
    sql_query_exp = call_args[0][0]

    # _get_data_objects query should distinguish between VIEW and MATERIALIZED_VIEW types
    sql_query = sql_query_exp.sql(dialect="databricks")
    assert (
        "CASE table_type WHEN 'VIEW' THEN 'view' WHEN 'MATERIALIZED_VIEW' THEN 'materialized_view' ELSE 'table' END AS type"
        in sql_query
    )

    objects_by_name = {obj.name: obj for obj in data_objects}
    assert objects_by_name["regular_view"].type == DataObjectType.VIEW
    assert objects_by_name["mat_view"].type == DataObjectType.MATERIALIZED_VIEW
    assert objects_by_name["regular_table"].type == DataObjectType.TABLE


def test_drop_data_object_materialized_view_calls_correct_drop(mocker: MockFixture):
    adapter = DatabricksEngineAdapter(lambda: None, default_catalog="test_catalog")

    mv_data_object = DataObject(
        catalog="test_catalog",
        schema="test_schema",
        name="test_mv",
        type=DataObjectType.MATERIALIZED_VIEW,
    )

    drop_view_mock = mocker.patch.object(adapter, "drop_view")
    adapter.drop_data_object(mv_data_object)

    # Ensure drop_view is called with materialized=True
    drop_view_mock.assert_called_once_with(
        mv_data_object.to_table(), ignore_if_not_exists=True, materialized=True
    )
