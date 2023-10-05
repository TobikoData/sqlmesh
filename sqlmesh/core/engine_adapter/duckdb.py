from __future__ import annotations

import math
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base import SourceQuery
from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF


class DuckDBEngineAdapter(LogicalMergeMixin):
    DIALECT = "duckdb"
    SUPPORTS_TRANSACTIONS = False

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

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        catalog_name = f"'{catalog_name}'" if catalog_name else "NULL"
        query = f"""
            SELECT
              {catalog_name} as catalog,
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
                catalog=None if math.isnan(row.catalog) else row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]
