from __future__ import annotations

import typing as t
from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import InsertOverwriteStrategy, SourceQuery

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

    def table_exists(self, table_name: TableName) -> bool:
        """
        Checks if a table exists.

        Querying the uppercase `INFORMATION_SCHEMA` required
        by case-sensitive Fabric environments.
        """
        table = exp.to_table(table_name)
        sql = (
            exp.select("1")
            .from_("INFORMATION_SCHEMA.TABLES")
            .where(f"TABLE_NAME = '{table.alias_or_name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"TABLE_SCHEMA = '{database_name}'")

        result = self.fetchone(sql, quote_identifiers=True)

        return result[0] == 1 if result else False

    def columns(
        self,
        table_name: TableName,
        include_pseudo_columns: bool = True,
    ) -> t.Dict[str, exp.DataType]:
        """Fabric doesn't support describe so we query INFORMATION_SCHEMA."""

        table = exp.to_table(table_name)

        sql = (
            exp.select(
                "COLUMN_NAME",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
                "NUMERIC_PRECISION",
                "NUMERIC_SCALE",
            )
            .from_("INFORMATION_SCHEMA.COLUMNS")
            .where(f"TABLE_NAME = '{table.name}'")
        )
        database_name = table.db
        if database_name:
            sql = sql.where(f"TABLE_SCHEMA = '{database_name}'")

        columns_raw = self.fetchall(sql, quote_identifiers=True)

        def build_var_length_col(
            column_name: str,
            data_type: str,
            character_maximum_length: t.Optional[int] = None,
            numeric_precision: t.Optional[int] = None,
            numeric_scale: t.Optional[int] = None,
        ) -> tuple:
            data_type = data_type.lower()
            if (
                data_type in self.VARIABLE_LENGTH_DATA_TYPES
                and character_maximum_length is not None
                and character_maximum_length > 0
            ):
                return (column_name, f"{data_type}({character_maximum_length})")
            if (
                data_type in ("varbinary", "varchar", "nvarchar")
                and character_maximum_length is not None
                and character_maximum_length == -1
            ):
                return (column_name, f"{data_type}(max)")
            if data_type in ("decimal", "numeric"):
                return (column_name, f"{data_type}({numeric_precision}, {numeric_scale})")
            if data_type == "float":
                return (column_name, f"{data_type}({numeric_precision})")

            return (column_name, data_type)

        columns = [build_var_length_col(*row) for row in columns_raw]

        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect)
            for column_name, data_type in columns
        }

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
        Implements the insert overwrite strategy for Fabric.

        Overridden to enforce a `DELETE`/`INSERT` strategy, as Fabric's
        `MERGE` statement has limitations.
        """

        columns_to_types = columns_to_types or self.columns(table_name)

        self.delete_from(table_name, where=where or exp.true())

        for source_query in source_queries:
            with source_query as query:
                query = self._order_projections_and_filter(query, columns_to_types, where=where)
                self._insert_append_query(
                    table_name,
                    query,
                    columns_to_types=columns_to_types,
                    order_projections=False,
                )
