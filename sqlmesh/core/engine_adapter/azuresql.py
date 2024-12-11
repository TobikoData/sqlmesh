"""Contains AzureSQLEngineAdapter."""

from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import CatalogSupport


class AzureSQLEngineAdapter(MSSQLEngineAdapter):
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
