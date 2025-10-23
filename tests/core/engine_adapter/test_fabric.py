# type: ignore

import typing as t

import pytest
from pytest_mock import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import FabricEngineAdapter
from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.engine_adapter.shared import DataObject

pytestmark = [pytest.mark.engine, pytest.mark.fabric]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> FabricEngineAdapter:
    return make_mocked_engine_adapter(FabricEngineAdapter)


def test_columns(adapter: FabricEngineAdapter):
    adapter.cursor.fetchall.return_value = [
        ("decimal_ps", "decimal", None, 5, 4),
        ("decimal", "decimal", None, 18, 0),
        ("float", "float", None, 53, None),
        ("char_n", "char", 10, None, None),
        ("varchar_n", "varchar", 10, None, None),
        ("nvarchar_max", "nvarchar", -1, None, None),
    ]

    assert adapter.columns("db.table") == {
        "decimal_ps": exp.DataType.build("decimal(5, 4)", dialect=adapter.dialect),
        "decimal": exp.DataType.build("decimal(18, 0)", dialect=adapter.dialect),
        "float": exp.DataType.build("float(53)", dialect=adapter.dialect),
        "char_n": exp.DataType.build("char(10)", dialect=adapter.dialect),
        "varchar_n": exp.DataType.build("varchar(10)", dialect=adapter.dialect),
        "nvarchar_max": exp.DataType.build("nvarchar(max)", dialect=adapter.dialect),
    }

    # Verify that the adapter queries the uppercase INFORMATION_SCHEMA
    adapter.cursor.execute.assert_called_once_with(
        """SELECT [COLUMN_NAME], [DATA_TYPE], [CHARACTER_MAXIMUM_LENGTH], [NUMERIC_PRECISION], [NUMERIC_SCALE] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME] = 'table' AND [TABLE_SCHEMA] = 'db';"""
    )


def test_table_exists(adapter: FabricEngineAdapter):
    adapter.cursor.fetchone.return_value = (1,)
    assert adapter.table_exists("db.table")
    # Verify that the adapter queries the uppercase INFORMATION_SCHEMA
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = 'table' AND [TABLE_SCHEMA] = 'db';"""
    )

    adapter.cursor.fetchone.return_value = None
    assert not adapter.table_exists("db.table")


def test_insert_overwrite_by_time_partition(adapter: FabricEngineAdapter):
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(x.strftime("%Y-%m-%d")),
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    # Fabric adapter should use DELETE/INSERT strategy, not MERGE.
    assert to_sql_calls(adapter) == [
        """DELETE FROM [test_table] WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';""",
        """INSERT INTO [test_table] ([a], [b]) SELECT [a], [b] FROM (SELECT [a] AS [a], [b] AS [b] FROM [tbl]) AS [_subquery] WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';""",
    ]


def test_replace_query(adapter: FabricEngineAdapter, mocker: MockerFixture):
    mocker.patch.object(
        adapter,
        "_get_data_objects",
        return_value=[DataObject(schema="", name="test_table", type="table")],
    )
    adapter.replace_query(
        "test_table", parse_one("SELECT a FROM tbl"), {"a": exp.DataType.build("int")}


def test_alter_table_column_type_workaround(adapter: FabricEngineAdapter, mocker: MockerFixture):
    """
    Tests the alter_table method's workaround for changing a column's data type.
    """
    # Mock set_current_catalog to avoid connection pool side effects
    set_catalog_mock = mocker.patch.object(adapter, "set_current_catalog")
    # Mock random_id to have a predictable temporary column name
    mocker.patch("sqlmesh.core.engine_adapter.fabric.random_id", return_value="abcdef")

    alter_expression = exp.Alter(
        this=exp.to_table("my_db.my_schema.my_table"),
        actions=[
            exp.AlterColumn(
                this=exp.to_column("col_a"),
                dtype=exp.DataType.build("BIGINT"),
            )
        ],
    )

    adapter.alter_table([alter_expression])

    set_catalog_mock.assert_called_once_with("my_db")

    expected_calls = [
        "ALTER TABLE [my_schema].[my_table] ADD [col_a__abcdef] BIGINT;",
        "UPDATE [my_schema].[my_table] SET [col_a__abcdef] = CAST([col_a] AS BIGINT);",
        "ALTER TABLE [my_schema].[my_table] DROP COLUMN [col_a];",
        "EXEC sp_rename 'my_schema.my_table.col_a__abcdef', 'col_a', 'COLUMN'",
    ]

    assert to_sql_calls(adapter) == expected_calls


def test_alter_table_direct_alteration(adapter: FabricEngineAdapter, mocker: MockerFixture):
    """
    Tests the alter_table method for direct alterations like adding a column.
    """
    set_catalog_mock = mocker.patch.object(adapter, "set_current_catalog")

    alter_expression = exp.Alter(
        this=exp.to_table("my_db.my_schema.my_table"),
        actions=[exp.ColumnDef(this=exp.to_column("new_col"), kind=exp.DataType.build("INT"))],
    )

    adapter.alter_table([alter_expression])

    set_catalog_mock.assert_called_once_with("my_db")

    expected_calls = [
        "ALTER TABLE [my_schema].[my_table] ADD [new_col] INT;",
    ]

    assert to_sql_calls(adapter) == expected_calls
