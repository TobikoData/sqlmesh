from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter


class DatabricksSparkSessionEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
