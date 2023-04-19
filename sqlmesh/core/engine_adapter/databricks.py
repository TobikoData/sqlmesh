from __future__ import annotations

from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.schema_diff import SchemaDiffer


class DatabricksSparkSessionEngineAdapter(SparkEngineAdapter):
    DIALECT = "databricks"
    SCHEMA_DIFFER = SchemaDiffer(
        support_positional_add=True,
        support_nested_operations=True,
        array_element_selector="element",
    )
