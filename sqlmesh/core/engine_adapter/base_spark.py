from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp, parse_one

from sqlmesh.core.dialect import pandas_to_sql
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import TransactionType
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class BaseSparkEngineAdapter(EngineAdapter):
    def replace_query(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        # Note: Some storage formats (like Delta and Iceberg) support REPLACE TABLE but since we don't
        # currently check for storage formats we will just do an insert/overwrite.
        return self._insert_overwrite_by_condition(
            table_name, query_or_df, columns_to_types=columns_to_types
        )

    def _insert_overwrite_by_condition(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if isinstance(query_or_df, pd.DataFrame):
            if columns_to_types is None:
                raise SQLMeshError(
                    "columns_to_types must be provided when using Pandas DataFrames"
                )
            query_or_df = next(
                pandas_to_sql(
                    query_or_df,
                    alias=table_name.split(".")[-1],
                    columns_to_types=columns_to_types,
                )
            )
        self.execute(
            exp.Insert(
                this=self._insert_into_expression(table_name, columns_to_types),
                expression=query_or_df,
                overwrite=True,
            )
        )

    def alter_table(
        self,
        table_name: str,
        added_columns: t.Dict[str, str],
        dropped_columns: t.Sequence[str],
    ) -> None:
        alter_table = exp.AlterTable(this=exp.to_table(table_name))

        if added_columns:
            add_columns = exp.Schema(
                expressions=[
                    exp.ColumnDef(
                        this=exp.to_identifier(column_name),
                        kind=parse_one(column_type, into=exp.DataType),  # type: ignore
                    )
                    for column_name, column_type in added_columns.items()
                ],
            )
            alter_table.set("actions", [add_columns])
            self.execute(alter_table)

        if dropped_columns:
            drop_columns = exp.Drop(
                this=exp.Schema(
                    expressions=[
                        exp.to_identifier(column_name)
                        for column_name in dropped_columns
                    ]
                ),
                kind="COLUMNS",
            )
            alter_table.set("actions", [drop_columns])
            self.execute(alter_table)

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
    ) -> t.Optional[exp.Properties]:
        format_property = None
        partition_columns_property = None
        if storage_format:
            format_property = exp.TableFormatProperty(this=exp.Var(this=storage_format))
        if partitioned_by:
            partition_columns_property = exp.PartitionedByProperty(
                this=exp.Schema(
                    expressions=[exp.to_identifier(column) for column in partitioned_by]
                ),
            )
        return exp.Properties(
            expressions=[
                table_property
                for table_property in [format_property, partition_columns_property]
                if table_property
            ]
        )

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False
