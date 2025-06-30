from __future__ import annotations

import typing as t
from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery
from sqlmesh.core.engine_adapter.base import EngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class FabricAdapter(MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric.
    """

    DIALECT = "fabric"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Implements the insert overwrite strategy for Fabric using DELETE and INSERT.

        This method is overridden to avoid the MERGE statement from the parent
        MSSQLEngineAdapter, which is not fully supported in Fabric.
        """
        return EngineAdapter._insert_overwrite_by_condition(
            self,
            table_name=table_name,
            source_queries=source_queries,
            columns_to_types=columns_to_types,
            where=where,
            insert_overwrite_strategy_override=InsertOverwriteStrategy.DELETE_INSERT,
            **kwargs,
        )
