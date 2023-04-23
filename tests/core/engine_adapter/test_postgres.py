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
            call("""TRUNCATE db.table"""),
            call("""INSERT INTO db.table SELECT col FROM db.other_table"""),
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
        """CREATE TABLE db.table AS SELECT col FROM db.other_table"""
    )


def test_merge_does_not_qualify_target_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = PostgresEngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.merge(
        target_table="target",
        source_table="SELECT id, ts, val FROM source",
        column_names=["id", "ts", "val"],
        unique_key=["id", "ts"],
    )
    cursor_mock.execute.assert_called_once_with(
        "MERGE INTO target AS __MERGE_TARGET__ USING (SELECT id, ts, val FROM source) AS __MERGE_SOURCE__ ON __MERGE_TARGET__.id = __MERGE_SOURCE__.id AND __MERGE_TARGET__.ts = __MERGE_SOURCE__.ts "
        "WHEN MATCHED THEN UPDATE SET id = __MERGE_SOURCE__.id, ts = __MERGE_SOURCE__.ts, val = __MERGE_SOURCE__.val "
        "WHEN NOT MATCHED THEN INSERT (id, ts, val) VALUES (__MERGE_SOURCE__.id, __MERGE_SOURCE__.ts, __MERGE_SOURCE__.val)"
    )
