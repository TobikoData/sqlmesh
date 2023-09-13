# type: ignore
import typing as t

import pandas as pd
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import DatabricksEngineAdapter
from tests.core.engine_adapter import to_sql_calls


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int)",
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT `a` FROM (SELECT `a` FROM `tbl`) AS `_subquery` WHERE TRUE",
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int)",
        "INSERT OVERWRITE TABLE `test_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `t`(`a`, `b`)) AS `_subquery` WHERE TRUE",
    ]


def test_clone_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter)
    adapter.clone_table("target_table", "source_table")
    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE `target_table` SHALLOW CLONE `source_table`"
    )
