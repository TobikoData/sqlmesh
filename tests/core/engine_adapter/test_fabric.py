# type: ignore

import typing as t

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import FabricAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.fabric]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> FabricAdapter:
    return make_mocked_engine_adapter(FabricAdapter)


def test_columns(adapter: FabricAdapter):
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


def test_table_exists(adapter: FabricAdapter):
    adapter.cursor.fetchone.return_value = (1,)
    assert adapter.table_exists("db.table")
    # Verify that the adapter queries the uppercase INFORMATION_SCHEMA
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = 'table' AND [TABLE_SCHEMA] = 'db';"""
    )

    adapter.cursor.fetchone.return_value = None
    assert not adapter.table_exists("db.table")


def test_insert_overwrite_by_time_partition(
    adapter: FabricAdapter, assert_exp_eq
):  # Add assert_exp_eq fixture
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(x.strftime("%Y-%m-%d")),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    # Get the list of generated SQL strings
    actual_sql_calls = to_sql_calls(adapter)

    # There should be two calls: DELETE and INSERT
    assert len(actual_sql_calls) == 2

    # Assert the DELETE statement is correct (string comparison is fine for this simple one)
    assert (
        actual_sql_calls[0]
        == "DELETE FROM [test_table] WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';"
    )

    # Assert the INSERT statement is semantically correct
    expected_insert_sql = """
        INSERT INTO [test_table] ([a], [b])
        SELECT [a], [b] FROM (SELECT [a], [b] FROM [tbl]) AS [_subquery]
        WHERE [b] BETWEEN '2022-01-01' AND '2022-01-02';
    """

    # Use assert_exp_eq to compare the parsed SQL expressions
    assert_exp_eq(actual_sql_calls[1], expected_insert_sql)


def test_replace_query(adapter: FabricAdapter):
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    # This behavior is inherited from MSSQLEngineAdapter and should be TRUNCATE + INSERT
    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_NAME] = 'test_table';""",
        "TRUNCATE TABLE [test_table];",
        "INSERT INTO [test_table] ([a]) SELECT [a] FROM [tbl];",
    ]
