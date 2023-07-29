from __future__ import annotations

import typing as t

from sqlmesh.core.engine_adapter.mixins import (
    CommitOnExecuteMixin,
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName


class MySQLEngineAdapter(
    CommitOnExecuteMixin,
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
