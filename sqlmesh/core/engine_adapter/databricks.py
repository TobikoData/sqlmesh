from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF


class DatabricksEngineAdapter(BaseSparkEngineAdapter):
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, "databricks", multithreaded=multithreaded)

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        """
        Currently returns a Pandas DataFrame. Need to figure out how to return a PySpark DataFrame
        """
        return self.cursor.fetchall_arrow().to_pandas()
