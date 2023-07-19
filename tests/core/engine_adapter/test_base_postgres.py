# type: ignore
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter


def test_columns(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchall.return_value = [("col", "INT")]

    adapter = BasePostgresEngineAdapter(lambda: connection_mock, "postgres")

    resp = adapter.columns("db.table")
    cursor_mock.execute.assert_called_once_with(
        """SELECT "column_name", "data_type" FROM "information_schema"."columns" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_table_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchone.return_value = (1,)

    adapter = BasePostgresEngineAdapter(lambda: connection_mock, "postgres")

    resp = adapter.table_exists("db.table")
    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp
    cursor_mock.fetchone.return_value = None
    resp = adapter.table_exists("db.table")
    assert not resp


def test_create_view(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BasePostgresEngineAdapter(lambda: connection_mock, "postgres")

    adapter.create_view("db.view", parse_one("SELECT 1"), replace=True)
    adapter.create_view("db.view", parse_one("SELECT 1"), replace=False)

    cursor_mock.execute.assert_has_calls(
        [
            # 1st call
            call('DROP VIEW IF EXISTS "db"."view"'),
            call('CREATE VIEW "db"."view" AS SELECT 1'),
            # 2nd call
            call('CREATE VIEW "db"."view" AS SELECT 1'),
        ]
    )
