# type: ignore
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import RedshiftEngineAdapter


def test_columns(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchall.return_value = [("col", "INT")]

    adapter = RedshiftEngineAdapter(lambda: connection_mock)

    resp = adapter.columns("db.table")
    cursor_mock.execute.assert_called_once_with(
        """SELECT "column_name", "data_type" FROM "SVV_COLUMNS" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_create_view_from_dataframe(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    pytest.raises(
        NotImplementedError,
        adapter.create_view,
        view_name="test_view",
        query_or_df=df,
        columns_to_types={"a": "int", "b": "int"},
    )


def test_create_table_from_query_exists_no_if_not_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )

    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_exists_and_if_not_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    cursor_mock.execute.assert_not_called()


def test_create_table_from_query_not_exists_if_not_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_not_exists_no_if_not_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_pandas_to_sql(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    results = list(adapter._pandas_to_sql(df=df, columns_to_types={"a": "int", "b": "int"}))
    assert len(results) == 1
    assert results[0].sql(dialect="redshift") == "VALUES (1, 4), (2, 5), (3, 6)"


def test_replace_query_with_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.replace_query(table_name="test_table", query_or_df=parse_one("SELECT cola FROM table"))

    cursor_mock.execute.assert_has_calls(
        [
            mocker.call('DROP TABLE IF EXISTS "test_table"'),
            mocker.call('CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'),
        ]
    )


def test_replace_query_with_df_table_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
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

    cursor_mock.begin.assert_called_once()
    cursor_mock.commit.assert_called_once()

    cursor_mock.execute.assert_has_calls(
        [
            call('CREATE TABLE "test_table_temp_1234" ("a" INTEGER, "b" INTEGER)'),
            call('INSERT INTO "test_table_temp_1234" ("a", "b") VALUES (1, 4), (2, 5), (3, 6)'),
            call('ALTER TABLE "test_table" RENAME TO "test_table_old_1234"'),
            call('ALTER TABLE "test_table_temp_1234" RENAME TO "test_table"'),
            call('DROP TABLE IF EXISTS "test_table_old_1234"'),
        ]
    )


def test_replace_query_with_df_table_not_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
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

    cursor_mock.execute.assert_has_calls(
        [
            call('CREATE TABLE "test_table" ("a" INTEGER, "b" INTEGER)'),
            call('INSERT INTO "test_table" ("a", "b") VALUES (1, 4), (2, 5), (3, 6)'),
        ]
    )


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
