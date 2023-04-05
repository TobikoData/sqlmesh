from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.schema_diff import DiffConfig


class DatabricksSparkSessionEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    DIFF_CONFIG = DiffConfig(
        support_positional_add=True,
        support_struct_add=True,
        array_suffix=".element",
    )
