# type: ignore
import typing as t

import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import RedshiftEngineAdapter
from tests.core.engine_adapter import to_sql_calls


def test_columns(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    adapter.cursor.fetchall.return_value = [("col", "INT")]
    resp = adapter.columns("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT "column_name", "data_type" FROM "SVV_COLUMNS" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_create_table_from_query_exists_no_if_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )

    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_exists_and_if_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_not_called()


def test_create_table_from_query_not_exists_if_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_not_exists_no_if_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_values_to_sql(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = adapter._values_to_sql(
        values=list(df.itertuples(index=False, name=None)),
        columns_to_types={"a": "int", "b": "int"},
        batch_start=0,
        batch_end=2,
    )
    # 3,6 is missing since the batch range excluded it
    assert (
        result.sql(dialect="redshift")
        == "SELECT CAST(a AS INTEGER) AS a, CAST(b AS INTEGER) AS b FROM (SELECT 1 AS a, 4 AS b UNION ALL SELECT 2, 5) AS t"
    )


def test_replace_query_with_query(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.columns",
        return_value={"cola": exp.DataType(this=exp.DataType.Type.INT)},
    )

    adapter.replace_query(table_name="test_table", query_or_df=parse_one("SELECT cola FROM table"))

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"',
    ]


def test_replace_query_with_df_table_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter._short_hash",
        return_value="1234",
    )
    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table_temp_1234" ("a" INTEGER, "b" INTEGER)',
        'INSERT INTO "test_table_temp_1234" ("a", "b") SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
        'ALTER TABLE "test_table" RENAME TO "test_table_old_1234"',
        'ALTER TABLE "test_table_temp_1234" RENAME TO "test_table"',
        'DROP TABLE IF EXISTS "test_table_old_1234"',
    ]


def test_replace_query_with_df_table_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
    ]


def test_table_exists_db_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = (1,)

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert adapter.table_exists(table_name=exp.to_table("some_db.some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table' AND "table_schema" = 'some_db'"""
    )


def test_table_exists_table_only(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = None

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert not adapter.table_exists(table_name=exp.to_table("some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table'"""
    )
