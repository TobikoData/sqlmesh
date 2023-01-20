from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class DuckDBEngineAdapter(EngineAdapter):
    DIALECT = "duckdb"

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self.execute(
            exp.Insert(
                this=self._insert_into_expression(table_name, columns_to_types),
                expression="SELECT * FROM df",
                overwrite=False,
            )
        )
