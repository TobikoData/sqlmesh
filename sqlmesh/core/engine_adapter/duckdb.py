from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import CatalogSupport, SourceQuery
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType, set_catalog

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF


class DuckDBEngineAdapter(LogicalMergeMixin, GetCurrentCatalogFromFunctionMixin):
    DIALECT = "duckdb"
    SUPPORTS_TRANSACTIONS = False
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    CURRENT_CATALOG_FUNCTION = "current_catalog()"

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(f"USE {catalog}")

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(target_table)
        temp_table_sql = (
            exp.select(*self._casted_columns(columns_to_types))
            .from_("df")
            .sql(dialect=self.dialect)
        )
        self.cursor.sql(f"CREATE TABLE {temp_table} AS {temp_table_sql}")
        return [
            SourceQuery(
                query_factory=lambda: exp.select(*columns_to_types).from_(temp_table),  # type: ignore
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

    @set_catalog(override=CatalogSupport.REQUIRES_SET_CATALOG)
    def _get_data_objects(self, schema_name: SchemaName) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        current_catalog = self.get_current_catalog()
        query = f"""
            SELECT
              '{current_catalog}' as catalog,
              table_name as name,
              table_schema as schema,
              CASE table_type
                WHEN 'BASE TABLE' THEN 'table'
                WHEN 'VIEW' THEN 'view'
                WHEN 'LOCAL TEMPORARY' THEN 'table'
                END as type
            FROM information_schema.tables
            WHERE table_schema = '{ schema_name }'
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]
