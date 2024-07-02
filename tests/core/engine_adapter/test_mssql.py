# type: ignore
import typing as t
from datetime import date
from unittest import mock

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
)
from sqlmesh.utils.date import to_ds
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.mssql]


def test_columns(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter.cursor.fetchall.return_value = [
        ("decimal_ps", "decimal", None, 5, 4),
        ("decimal", "decimal", None, 18, 0),
        ("numeric_ps", "numeric", None, 5, 4),
        ("numeric", "numeric", None, 18, 0),
        ("float_n", "real", None, 24, None),
        ("float", "float", None, 53, None),
        ("binary_n", "binary", 10, None, None),
        ("binary", "binary", 1, None, None),
        ("var_binary_n", "varbinary", 10, None, None),
        ("var_binary_max", "varbinary", -1, None, None),
        ("var_binary", "varbinary", 1, None, None),
        ("char_n", "char", 10, None, None),
        ("char", "char", 1, None, None),
        ("varchar_n", "varchar", 10, None, None),
        ("varchar_max", "varchar", -1, None, None),
        ("varchar", "varchar", 1, None, None),
        ("nchar_n", "nchar", 10, None, None),
        ("nchar", "nchar", 1, None, None),
        ("nvarchar_n", "nvarchar", 10, None, None),
        ("nvarchar", "nvarchar", 1, None, None),
        ("nvarchar_max", "nvarchar", -1, None, None),
    ]

    assert adapter.columns("db.table") == {
        "decimal_ps": exp.DataType.build("decimal(5, 4)", dialect=adapter.dialect),
        "decimal": exp.DataType.build("decimal(18, 0)", dialect=adapter.dialect),
        "numeric_ps": exp.DataType.build("numeric(5, 4)", dialect=adapter.dialect),
        "numeric": exp.DataType.build("numeric(18, 0)", dialect=adapter.dialect),
        "float_n": exp.DataType.build("real", dialect=adapter.dialect),
        "float": exp.DataType.build("float(53)", dialect=adapter.dialect),
        "binary_n": exp.DataType.build("binary(10)", dialect=adapter.dialect),
        "binary": exp.DataType.build("binary(1)", dialect=adapter.dialect),
        "var_binary_n": exp.DataType.build("varbinary(10)", dialect=adapter.dialect),
        "var_binary_max": exp.DataType.build("varbinary(max)", dialect=adapter.dialect),
        "var_binary": exp.DataType.build("varbinary(1)", dialect=adapter.dialect),
        "char_n": exp.DataType.build("char(10)", dialect=adapter.dialect),
        "char": exp.DataType.build("char(1)", dialect=adapter.dialect),
        "varchar_n": exp.DataType.build("varchar(10)", dialect=adapter.dialect),
        "varchar_max": exp.DataType.build("varchar(max)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("varchar(1)", dialect=adapter.dialect),
        "nchar_n": exp.DataType.build("nchar(10)", dialect=adapter.dialect),
        "nchar": exp.DataType.build("nchar(1)", dialect=adapter.dialect),
        "nvarchar_n": exp.DataType.build("nvarchar(10)", dialect=adapter.dialect),
        "nvarchar": exp.DataType.build("nvarchar(1)", dialect=adapter.dialect),
        "nvarchar_max": exp.DataType.build("nvarchar(max)", dialect=adapter.dialect),
    }

    adapter.cursor.execute.assert_called_once_with(
        """SELECT [column_name], [data_type], [character_maximum_length], [numeric_precision], [numeric_scale] FROM [information_schema].[columns] WHERE [table_name] = 'table' AND [table_schema] = 'db';"""
    )


def test_varchar_workaround_to_max(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns = {
        "binary1": exp.DataType.build("BINARY(1)", dialect=adapter.dialect),
        "varbinary": exp.DataType.build("VARBINARY", dialect=adapter.dialect),
        "varbinary1": exp.DataType.build("VARBINARY(1)", dialect=adapter.dialect),
        "varbinary2": exp.DataType.build("VARBINARY(2)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("VARCHAR", dialect=adapter.dialect),
        "varchar1": exp.DataType.build("VARCHAR(1)", dialect=adapter.dialect),
        "varchar2": exp.DataType.build("VARCHAR(2)", dialect=adapter.dialect),
        "nvarchar": exp.DataType.build("NVARCHAR", dialect=adapter.dialect),
        "nvarchar1": exp.DataType.build("NVARCHAR(1)", dialect=adapter.dialect),
        "nvarchar2": exp.DataType.build("NVARCHAR(2)", dialect=adapter.dialect),
    }

    assert adapter._default_precision_to_max(columns) == {
        "binary1": exp.DataType.build("BINARY(1)", dialect=adapter.dialect),
        "varbinary": exp.DataType.build("VARBINARY", dialect=adapter.dialect),
        "varbinary1": exp.DataType.build("VARBINARY(max)", dialect=adapter.dialect),
        "varbinary2": exp.DataType.build("VARBINARY(2)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("VARCHAR", dialect=adapter.dialect),
        "varchar1": exp.DataType.build("VARCHAR(max)", dialect=adapter.dialect),
        "varchar2": exp.DataType.build("VARCHAR(2)", dialect=adapter.dialect),
        "nvarchar": exp.DataType.build("NVARCHAR", dialect=adapter.dialect),
        "nvarchar1": exp.DataType.build("NVARCHAR(max)", dialect=adapter.dialect),
        "nvarchar2": exp.DataType.build("NVARCHAR(2)", dialect=adapter.dialect),
    }

    mocker.patch(
        "sqlmesh.core.engine_adapter.base.random_id",
        return_value="test_random_id",
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=True,
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.columns",
        return_value=columns,
    )

    adapter.ctas(
        table_name="test_schema.test_table",
        query_or_df=parse_one(
            "SELECT binary, varbinary1 + 1 AS varbinary1, varbinary2 AS varbinary2, varchar, varchar1, varchar2, nvarchar, nvarchar1, nvarchar2 FROM (SELECT * FROM table WHERE FALSE LIMIT 0) WHERE d > 0 AND FALSE LIMIT 0"
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        "CREATE VIEW [__temp_ctas_test_random_id] AS SELECT [binary], [varbinary1] + 1 AS [varbinary1], [varbinary2] AS [varbinary2], [varchar], [varchar1], [varchar2], [nvarchar], [nvarchar1], [nvarchar2] FROM (SELECT * FROM [table]);",
        "DROP VIEW IF EXISTS [__temp_ctas_test_random_id];",
        "CREATE TABLE [test_schema].[test_table] ([binary1] BINARY(1), [varbinary] VARBINARY, [varbinary1] VARBINARY(max), [varbinary2] VARBINARY(2), [varchar] VARCHAR, [varchar1] VARCHAR(max), [varchar2] VARCHAR(2), [nvarchar] NVARCHAR, [nvarchar1] NVARCHAR(max), [nvarchar2] NVARCHAR(2));",
    ]


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    resp = adapter.table_exists("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 """
        """FROM [information_schema].[tables] """
        """WHERE [table_name] = 'table' AND [table_schema] = 'db';"""
    )
    assert resp
    adapter.cursor.fetchone.return_value = None
    resp = adapter.table_exists("db.table")
    assert not resp


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    table_name = "test_table"
    temp_table_id = "abcdefgh"
    mocker.patch(
        "sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table",
        return_value=make_temp_table_name(table_name, temp_table_id),
    )

    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter.insert_overwrite_by_time_partition(
        table_name,
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        time_column="ds",
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_test_table_{temp_table_id}", [(1, "2022-01-01"), (2, "2022-01-02")]
    )
    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_id}') EXEC('CREATE TABLE [__temp_test_table_{temp_table_id}] ([a] INTEGER, [ds] VARCHAR(MAX))');""",
        f"""MERGE INTO [test_table] AS [__MERGE_TARGET__] USING (SELECT [a] AS [a], [ds] AS [ds] FROM (SELECT CAST([a] AS INTEGER) AS [a], CAST([ds] AS VARCHAR(MAX)) AS [ds] FROM [__temp_test_table_{temp_table_id}]) AS [_subquery] WHERE [ds] BETWEEN '2022-01-01' AND '2022-01-02') AS [__MERGE_SOURCE__] ON (1 = 0) WHEN NOT MATCHED BY SOURCE AND [ds] BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ([a], [ds]) VALUES ([a], [ds]);""",
        f"DROP TABLE IF EXISTS [__temp_test_table_{temp_table_id}];",
    ]


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=True,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    table_name = "test_table"
    temp_table_id = "abcdefgh"
    mocker.patch(
        "sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table",
        return_value=make_temp_table_name(table_name, temp_table_id),
    )

    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter.insert_overwrite_by_time_partition(
        table_name,
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        time_column="ds",
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )
    assert to_sql_calls(adapter) == [
        f"""MERGE INTO [test_table] AS [__MERGE_TARGET__] USING (SELECT [a] AS [a], [ds] AS [ds] FROM (SELECT CAST([a] AS INTEGER) AS [a], CAST([ds] AS VARCHAR(MAX)) AS [ds] FROM [__temp_test_table_{temp_table_id}]) AS [_subquery] WHERE [ds] BETWEEN '2022-01-01' AND '2022-01-02') AS [__MERGE_SOURCE__] ON (1 = 0) WHEN NOT MATCHED BY SOURCE AND [ds] BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ([a], [ds]) VALUES ([a], [ds]);""",
        f"DROP TABLE IF EXISTS [__temp_test_table_{temp_table_id}];",
    ]


def test_insert_overwrite_by_time_partition_replace_where_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter.insert_overwrite_by_time_partition(
        table_name,
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        time_column="ds",
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_test_table_{temp_table_id}", [(1, "2022-01-01"), (2, "2022-01-02")]
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_id}') EXEC('CREATE TABLE [__temp_test_table_{temp_table_id}] ([a] INTEGER, [ds] VARCHAR(MAX))');""",
        f"""MERGE INTO [test_table] AS [__MERGE_TARGET__] USING (SELECT [a] AS [a], [ds] AS [ds] FROM (SELECT CAST([a] AS INTEGER) AS [a], CAST([ds] AS VARCHAR(MAX)) AS [ds] FROM [__temp_test_table_{temp_table_id}]) AS [_subquery] WHERE [ds] BETWEEN '2022-01-01' AND '2022-01-02') AS [__MERGE_SOURCE__] ON (1 = 0) WHEN NOT MATCHED BY SOURCE AND [ds] BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ([a], [ds]) VALUES ([a], [ds]);""",
        f"DROP TABLE IF EXISTS [__temp_test_table_{temp_table_id}];",
    ]


def test_insert_append_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.insert_append(
        table_name,
        df,
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("INT"),
        },
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_test_table_{temp_table_id}", [(1, 4), (2, 5), (3, 6)]
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_id}') EXEC('CREATE TABLE [__temp_test_table_{temp_table_id}] ([a] INTEGER, [b] INTEGER)');""",
        f"INSERT INTO [test_table] ([a], [b]) SELECT CAST([a] AS INTEGER) AS [a], CAST([b] AS INTEGER) AS [b] FROM [__temp_test_table_{temp_table_id}];",
        f"DROP TABLE IF EXISTS [__temp_test_table_{temp_table_id}];",
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types)

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE [test_table] ([cola] INTEGER, [colb] VARCHAR(MAX))');"""
    )


def test_create_physical_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        storage_format="ICEBERG",
    )

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE [test_table] ([cola] INTEGER, [colb] VARCHAR(MAX))');"""
    )


def test_merge_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "target"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    df = pd.DataFrame({"id": [1, 2, 3], "ts": [1, 2, 3], "val": [4, 5, 6]})
    adapter.merge(
        target_table=table_name,
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("id")],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_target_{temp_table_id}", [(1, 1, 4), (2, 2, 5), (3, 3, 6)]
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_id}') EXEC('CREATE TABLE [__temp_target_{temp_table_id}] ([id] INTEGER, [ts] DATETIME2, [val] INTEGER)');""",
        f"MERGE INTO [target] AS [__MERGE_TARGET__] USING (SELECT CAST([id] AS INTEGER) AS [id], CAST([ts] AS DATETIME2) AS [ts], CAST([val] AS INTEGER) AS [val] FROM [__temp_target_{temp_table_id}]) AS [__MERGE_SOURCE__] ON [__MERGE_TARGET__].[id] = [__MERGE_SOURCE__].[id] WHEN MATCHED THEN UPDATE SET [__MERGE_TARGET__].[id] = [__MERGE_SOURCE__].[id], [__MERGE_TARGET__].[ts] = [__MERGE_SOURCE__].[ts], [__MERGE_TARGET__].[val] = [__MERGE_SOURCE__].[val] WHEN NOT MATCHED THEN INSERT ([id], [ts], [val]) VALUES ([__MERGE_SOURCE__].[id], [__MERGE_SOURCE__].[ts], [__MERGE_SOURCE__].[val]);",
        f"DROP TABLE IF EXISTS [__temp_target_{temp_table_id}];",
    ]

    adapter.cursor.reset_mock()
    adapter._connection_pool.get().reset_mock()
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)
    adapter.merge(
        target_table=table_name,
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("id"), exp.to_column("ts")],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_target_{temp_table_id}", [(1, 1, 4), (2, 2, 5), (3, 3, 6)]
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_id}') EXEC('CREATE TABLE [__temp_target_{temp_table_id}] ([id] INTEGER, [ts] DATETIME2, [val] INTEGER)');""",
        f"MERGE INTO [target] AS [__MERGE_TARGET__] USING (SELECT CAST([id] AS INTEGER) AS [id], CAST([ts] AS DATETIME2) AS [ts], CAST([val] AS INTEGER) AS [val] FROM [__temp_target_{temp_table_id}]) AS [__MERGE_SOURCE__] ON [__MERGE_TARGET__].[id] = [__MERGE_SOURCE__].[id] AND [__MERGE_TARGET__].[ts] = [__MERGE_SOURCE__].[ts] WHEN MATCHED THEN UPDATE SET [__MERGE_TARGET__].[id] = [__MERGE_SOURCE__].[id], [__MERGE_TARGET__].[ts] = [__MERGE_SOURCE__].[ts], [__MERGE_TARGET__].[val] = [__MERGE_SOURCE__].[val] WHEN NOT MATCHED THEN INSERT ([id], [ts], [val]) VALUES ([__MERGE_SOURCE__].[id], [__MERGE_SOURCE__].[ts], [__MERGE_SOURCE__].[val]);",
        f"DROP TABLE IF EXISTS [__temp_target_{temp_table_id}];",
    ]


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM [information_schema].[tables] WHERE [table_name] = 'test_table';""",
        "MERGE INTO [test_table] AS [__MERGE_TARGET__] USING (SELECT [a] AS [a] FROM [tbl]) AS [__MERGE_SOURCE__] ON (1 = 0) WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED THEN INSERT ([a]) VALUES ([a]);",
    ]


def test_replace_query_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    temp_table_exists_counter = 0

    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test_table"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)
    temp_table_name = temp_table_mock.return_value.sql()

    def temp_table_exists(table: exp.Table) -> bool:
        nonlocal temp_table_exists_counter
        nonlocal temp_table_name
        temp_table_exists_counter += 1
        if table.sql() == temp_table_name and temp_table_exists_counter == 1:
            return False
        return True

    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        side_effect=temp_table_exists,
    )

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        table_name, df, {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    )

    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f"__temp_test_table_{temp_table_id}", [(1, 4), (2, 5), (3, 6)]
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{temp_table_name}') EXEC('CREATE TABLE [{temp_table_name}] ([a] INTEGER, [b] INTEGER)');""",
        "MERGE INTO [test_table] AS [__MERGE_TARGET__] USING (SELECT CAST([a] AS INTEGER) AS [a], CAST([b] AS INTEGER) AS [b] FROM [__temp_test_table_abcdefgh]) AS [__MERGE_SOURCE__] ON (1 = 0) WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED THEN INSERT ([a], [b]) VALUES ([a], [b]);",
        f"DROP TABLE IF EXISTS [{temp_table_name}];",
    ]


def test_create_table_primary_key(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE [test_table] ([cola] INTEGER, [colb] VARCHAR(MAX), PRIMARY KEY ([cola], [colb]))');"""
    )


def test_create_index(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.SUPPORTS_INDEXES = True

    adapter.create_index("test_table", "test_index", ("cola", "colb"))
    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('test_table') AND name = 'test_index') EXEC('CREATE INDEX [test_index] ON [test_table]([cola], [colb])');"""
    )


def test_drop_schema_with_catalog(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter.get_current_catalog = mocker.MagicMock(return_value="other_catalog")

    adapter.drop_schema("catalog.schema")

    assert to_sql_calls(adapter) == [
        "USE [catalog];",
        "DROP SCHEMA IF EXISTS [schema];",
        "USE [other_catalog];",
    ]


def test_get_data_objects_catalog(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    original_set_current_catalog = adapter.set_current_catalog
    local_state = {}

    def set_local_catalog(catalog, local_state):
        original_set_current_catalog(catalog)
        local_state["catalog"] = catalog

    adapter.get_current_catalog = mocker.MagicMock(
        side_effect=lambda: local_state.get("catalog", "other_catalog")
    )
    adapter.set_current_catalog = mocker.MagicMock(
        side_effect=lambda x: set_local_catalog(x, local_state)
    )
    adapter.cursor.fetchall.return_value = [("test_catalog", "test_table", "test_schema", "TABLE")]
    adapter.cursor.description = [["catalog_name"], ["name"], ["schema_name"], ["type"]]
    result = adapter.get_data_objects("test_catalog.test_schema")

    assert result == [
        DataObject(
            catalog="test_catalog",
            schema="test_schema",
            name="test_table",
            type=DataObjectType.from_str("TABLE"),
        )
    ]

    assert to_sql_calls(adapter) == [
        "USE [test_catalog];",
        "SELECT TABLE_NAME AS name, TABLE_SCHEMA AS schema_name, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'test_schema';",
        "USE [other_catalog];",
    ]


def test_drop_schema(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter._get_data_objects = mock.Mock()
    adapter._get_data_objects.return_value = [
        DataObject(
            catalog="test_catalog",
            schema="test_schema",
            name="test_view",
            type=DataObjectType.from_str("VIEW"),
        )
    ]

    adapter.drop_schema("test_schema", cascade=True)

    assert to_sql_calls(adapter) == [
        """DROP VIEW IF EXISTS [test_schema].[test_view];""",
        """DROP SCHEMA IF EXISTS [test_schema];""",
    ]


def test_df_dates(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "date": exp.DataType.build("DATE"),
        "timestamp_tz": exp.DataType.build("TIMESTAMPTZ"),
        "timestamp": exp.DataType.build("TIMESTAMP"),
    }

    df = pd.DataFrame(
        {
            "date": [date(2023, 1, 1)],
            "timestamp_tz": [pd.Timestamp("2023-01-01 12:00:00.1+0000")],
            "timestamp": [pd.Timestamp("2023-01-01 12:00:00.1")],
        }
    )

    adapter._convert_df_datetime(df, columns_to_types)

    assert columns_to_types == {
        "date": exp.DataType.build("DATE"),
        "timestamp_tz": exp.DataType.build("TEXT"),
        "timestamp": exp.DataType.build("TIMESTAMP"),
    }

    assert all(
        df
        == pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "timestamp_tz": ["2023-01-01 12:00:00.100000+00:00"],
                "timestamp": ["2023-01-01 12:00:00.100000"],
            }
        )
    )


def test_rename_table(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter.rename_table("test_schema.old_name", "new_name")
    adapter.rename_table("old_name", "new_name")

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "EXEC sp_rename 'test_schema.old_name', 'new_name';",
        "EXEC sp_rename 'old_name', 'new_name';",
    ]


def test_create_table_from_query(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.base.random_id",
        return_value="test_random_id",
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.table_exists",
        return_value=False,
    )

    columns_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.mssql.MSSQLEngineAdapter.columns",
        return_value={
            "a": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "b": exp.DataType.build("VARCHAR(60)", dialect="redshift"),
            "c": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "d": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "e": exp.DataType.build("TIMESTAMP", dialect="redshift"),
        },
    )

    adapter.ctas(
        table_name="test_schema.test_table",
        query_or_df=parse_one(
            "SELECT a, b, x + 1 AS c, d AS d, e FROM (SELECT * FROM table WHERE FALSE LIMIT 0) WHERE d > 0 AND FALSE LIMIT 0"
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        "CREATE VIEW [__temp_ctas_test_random_id] AS SELECT [a], [b], [x] + 1 AS [c], [d] AS [d], [e] FROM (SELECT * FROM [table]);",
        "DROP VIEW IF EXISTS [__temp_ctas_test_random_id];",
        "CREATE TABLE [test_schema].[test_table] ([a] VARCHAR(MAX), [b] VARCHAR(60), [c] VARCHAR(MAX), [d] VARCHAR(MAX), [e] DATETIME2);",
    ]

    columns_mock.assert_called_once_with(exp.table_("__temp_ctas_test_random_id", quoted=True))
