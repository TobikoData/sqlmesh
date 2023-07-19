# type: ignore
import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import DatabricksEngineAdapter


def test_replace_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = DatabricksEngineAdapter(lambda: connection_mock)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    cursor_mock.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT * FROM (SELECT `a` FROM `tbl`) AS `_subquery` WHERE 1 = 1"
    )


def test_replace_query_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = DatabricksEngineAdapter(lambda: connection_mock)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    cursor_mock.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`, `b`) SELECT * FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `test_table`(`a`, `b`)) AS `_subquery` WHERE 1 = 1"
    )
