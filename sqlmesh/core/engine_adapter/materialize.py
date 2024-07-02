from __future__ import annotations

import logging
import typing as t


from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog
from sqlglot import exp

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName

logger = logging.getLogger(__name__)


@set_catalog()
class MaterializeEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    CURRENT_CATALOG_EXPRESSION = exp.func("current_database")
    DIALECT = "materialize"
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_TRANSACTIONS = False

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        # Materialize does not support TRUNCATE TABLE so we have to use DELETE FROM
        self.execute(f"DELETE FROM {table.sql(dialect=self.dialect, identify=True)}")
