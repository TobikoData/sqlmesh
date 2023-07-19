# type: ignore
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import PostgresEngineAdapter


def test_replace_query_already_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchone.return_value = (1,)

    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.PostgresEngineAdapter.table_exists",
        return_value=True,
    )

    adapter = PostgresEngineAdapter(lambda: connection_mock, "postgres")

    adapter.replace_query("db.table", parse_one("SELECT col FROM db.other_table"))
    cursor_mock.execute.assert_has_calls(
        [
            call('TRUNCATE "db"."table"'),
            call('INSERT INTO "db"."table" SELECT "col" FROM "db"."other_table"'),
        ]
    )


def test_replace_query_does_not_exist(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchone.return_value = (1,)

    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.PostgresEngineAdapter.table_exists",
        return_value=False,
    )

    adapter = PostgresEngineAdapter(lambda: connection_mock, "postgres")

    adapter.replace_query("db.table", parse_one("SELECT col FROM db.other_table"))
    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE "db"."table" AS SELECT "col" FROM "db"."other_table"'
    )
