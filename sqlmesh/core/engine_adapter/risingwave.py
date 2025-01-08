from __future__ import annotations

import logging
import typing as t


from sqlglot import exp

from sqlmesh.core.engine_adapter.postgres import PostgresEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
    CatalogSupport,
    CommentCreationView,
    DataObjectType,
    CommentCreationTable,
)


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName

logger = logging.getLogger(__name__)


@set_catalog()
class RisingwaveEngineAdapter(PostgresEngineAdapter):
    DIALECT = "risingwave"
    DEFAULT_BATCH_SIZE = 400
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SUPPORTS_MATERIALIZED_VIEWS = True
    # Temporarily set this because integration test: test_transaction uses truncate table operation, which is not supported in risingwave.
    SUPPORTS_TRANSACTIONS = False

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expression],
    ) -> None:
        """
        Risingwave doesn't support CASCADE clause and drops schemas unconditionally so far.
        If cascade is supported later, this logic could be discarded.
        """
        if cascade:
            objects = self._get_data_objects(schema_name)
            for obj in objects:
                if obj.type == DataObjectType.VIEW:
                    self.drop_view(
                        ".".join([obj.schema_name, obj.name]),
                        ignore_if_not_exists=ignore_if_not_exists,
                    )
                else:
                    self.drop_table(
                        ".".join([obj.schema_name, obj.name]),
                        exists=ignore_if_not_exists,
                    )
        super().drop_schema(schema_name, ignore_if_not_exists=ignore_if_not_exists, cascade=False)
