from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter


class DatabricksSparkSessionEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    STRUCT_DIFFER_PROPERTIES = {
        "support_positional_add": True,
        "support_struct_add_drop": True,
        "array_suffix": ".element",
    }
