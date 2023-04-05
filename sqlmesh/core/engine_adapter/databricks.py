from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.schema_diff import SchemaDiffConfig


class DatabricksSparkSessionEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    SCHEMA_DIFF_CONFIG = SchemaDiffConfig(
        support_positional_add=True,
        support_struct_add=True,
        array_suffix=".element",
    )
