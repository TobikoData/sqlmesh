# type: ignore
import typing as t

from pytest_mock.plugin import MockerFixture

from sqlmesh.core.engine_adapter import MySQLEngineAdapter
from tests.core.engine_adapter import to_sql_calls


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MySQLEngineAdapter)

    fetchone_mock = mocker.patch("sqlmesh.core.engine_adapter.mysql.MySQLEngineAdapter.fetchone")
    fetchone_mock.return_value = ["test_table", "CREATE TABLE test_table (a INT)"]

    adapter._create_table_comment(
        "test_table",
        "test description",
    )

    adapter._create_column_comments(
        "test_table",
        {"a": "a description"},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "ALTER TABLE `test_table` COMMENT = 'test description'",
        "ALTER TABLE `test_table` MODIFY `a` INT COMMENT 'a description'",
    ]
