from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import hive_create_table_properties
from sqlmesh.core.engine_adapter.transaction_type import TransactionType

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF


class DatabricksEngineAdapter(EngineAdapter):
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, "spark", multithreaded=multithreaded)

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        """
        Currently returns a Pandas DataFrame. Need to figure out how to return a PySpark DataFrame
        """
        return self.cursor.fetchall_arrow().to_pandas()

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
    ) -> t.Optional[exp.Properties]:
        return hive_create_table_properties(storage_format, partitioned_by)

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False
