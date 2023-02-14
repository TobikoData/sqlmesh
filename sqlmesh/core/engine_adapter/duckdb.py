from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class DuckDBEngineAdapter(EngineAdapter):
    DIALECT = "duckdb"

    def to_json(self, model: PydanticModel) -> str:
        """
        DuckDB executes the output directly against the engine so we don't need to add the additional backslashes.
        """
        return model.json()

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
