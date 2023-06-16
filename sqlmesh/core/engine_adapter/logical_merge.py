from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import EngineAdapter, TransactionType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class LogicalMergeAdapter(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[str],
    ) -> None:
        """
        Merge implementation for engine adapters that do not support merge natively.

        The merge is executed as follows:
        1. Create a temporary table containing the new data to merge.
        2. Delete rows from target table where unique_key cols match a row in the temporary table.
        3. Insert the temporary table contents into the target table. Any duplicate, non-unique rows
           within the temporary table are ommitted.
        4. Drop the temporary table.
        """
        if columns_to_types is None:
            columns_to_types = self.columns(target_table)

        temp_table = self._get_temp_table(target_table)
        unique_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", *unique_key)
        column_names = list(columns_to_types or [])

        with self.transaction(TransactionType.DML):
            self.ctas(temp_table, source_table, columns_to_types=columns_to_types, exists=False)
            self.execute(
                exp.delete(target_table).where(
                    unique_exp.isin(query=exp.select(unique_exp).from_(temp_table))
                )
            )
            self.execute(
                exp.insert(
                    exp.select(*columns_to_types)
                    .distinct(*unique_key)
                    .from_(temp_table)
                    .subquery(),
                    target_table,
                    columns=column_names,
                )
            )
            self.drop_table(temp_table)
