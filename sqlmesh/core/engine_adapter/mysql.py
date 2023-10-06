from __future__ import annotations

import typing as t

from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class MySQLEngineAdapter(
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
):
    DEFAULT_BATCH_SIZE = 200
    DIALECT = "mysql"
    ESCAPE_JSON = True
    SUPPORTS_INDEXES = True

    def create_index(
        self,
        table_name: TableName,
        index_name: str,
        columns: t.Tuple[str, ...],
        exists: bool = True,
    ) -> None:
        # MySQL doesn't support IF EXISTS clause for indexes.
        super().create_index(table_name, index_name, columns, exists=False)

    def drop_schema(
        self, schema_name: str, ignore_if_not_exists: bool = True, cascade: bool = False
    ) -> None:
        # MySQL doesn't support CASCADE clause and drops schemas unconditionally.
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        if catalog_name is not None:
            raise NotImplementedError("MySQL doesn't support catalogs.")
        query = f"""
            SELECT
                null AS catalog_name,
                table_name AS name,
                table_schema AS schema_name,
                CASE
                    WHEN table_type = 'BASE TABLE' THEN 'table'
                    WHEN table_type = 'VIEW' THEN 'view'
                    ELSE table_type
                END AS type
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]
