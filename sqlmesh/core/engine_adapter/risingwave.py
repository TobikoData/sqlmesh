from __future__ import annotations

import logging
import typing as t


from sqlglot import exp

from sqlmesh.core.engine_adapter.postgres import PostgresEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
    CatalogSupport,
    CommentCreationView,
    CommentCreationTable,
)


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName

logger = logging.getLogger(__name__)


@set_catalog()
class RisingwaveEngineAdapter(PostgresEngineAdapter):
    DIALECT = "risingwave"
    DEFAULT_BATCH_SIZE = 400
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_TRANSACTIONS = False

    def _truncate_table(self, table_name: TableName) -> None:
        return self.execute(exp.Delete(this=exp.to_table(table_name)))
