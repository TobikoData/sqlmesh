from __future__ import annotations

import typing as t
from enum import Enum

from sqlglot import exp


class TransactionType(str, Enum):
    DDL = "DDL"
    DML = "DML"

    @property
    def is_ddl(self) -> bool:
        return self == TransactionType.DDL

    @property
    def is_dml(self) -> bool:
        return self == TransactionType.DML


def hive_create_table_properties(
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
