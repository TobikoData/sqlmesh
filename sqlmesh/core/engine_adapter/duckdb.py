from __future__ import annotations

import math
import typing as t

from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType


class DuckDBEngineAdapter(LogicalMergeMixin):
    DIALECT = "duckdb"

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
