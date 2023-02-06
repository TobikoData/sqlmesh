from __future__ import annotations

from sqlmesh.core.engine_adapter.base_databricks import BaseDatabricks
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter


class DatabricksSparkSessionEngineAdapter(BaseDatabricks, SparkEngineAdapter):
    pass
