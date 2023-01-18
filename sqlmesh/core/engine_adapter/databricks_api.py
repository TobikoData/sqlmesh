from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF


class DatabricksAPIEngineAdapter(BaseSparkEngineAdapter):
    DIALECT = "databricks"

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        """
        Returns a Pandas DataFrame from a query or expression.
        """
        self.execute(query)
        return self.cursor.fetchall_arrow().to_pandas()
