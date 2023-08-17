# type: ignore
import typing as t

import pandas as pd
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import DatabricksEngineAdapter


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    adapter.cursor.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT * FROM (SELECT `a` FROM `tbl`) AS `_subquery` WHERE 1 = 1"
    )


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(DatabricksEngineAdapter)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    adapter.cursor.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`, `b`) SELECT * FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `test_table`(`a`, `b`)) AS `_subquery` WHERE 1 = 1"
    )
