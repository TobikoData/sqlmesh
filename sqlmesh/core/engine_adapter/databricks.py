from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter


class DatabricksEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
