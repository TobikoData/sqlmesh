from __future__ import annotations

import logging
import typing as t


from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog

if t.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@set_catalog()
class MaterializeEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "materialize"
    SUPPORTS_REPLACE_TABLE = False
    SUPPORTS_MATERIALIZED_VIEWS = True
