from __future__ import annotations

import typing as t

from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.base import EngineAdapter, TransactionType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class LogicalMerge(EngineAdapter):
    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Dict[str, exp.DataType],
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
        temp_table = self._get_temp_table(target_table)
        unique_exp: exp.Expression = parse_one("|| '__SQLMESH_DELIM__' ||".join(unique_key))
        with self.transaction(TransactionType.DML):
            self.ctas(temp_table, source_table, columns_to_types=columns_to_types, exists=False)
            self.execute(
                exp.Delete(
                    this=target_table,
                    where=exp.Where(
                        this=exp.In(
                            this=unique_exp,
                            query=exp.Select(expressions=[unique_exp]).from_(temp_table),
                        )
                    ),
                )
            )
            subquery = exp.Subquery(
                this=exp.Select(
                    distinct=exp.Distinct(
                        on=exp.Tuple(
                            expressions=[
                                exp.Column(this=exp.Identifier(this=key)) for key in unique_key
                            ]
                        )
                    ),
                    expressions=[
                        exp.Column(this=exp.Identifier(this=col)) for col in columns_to_types
                    ],
                ).from_(temp_table)
            )
            self.execute(
                exp.Insert(
                    this=exp.Schema(
                        this=target_table,
                        expressions=[
                            exp.Column(this=exp.Identifier(this=col)) for col in columns_to_types
                        ],
                    ),
                    expression=subquery,
                )
            )
            self.drop_table(temp_table)
