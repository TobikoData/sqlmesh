import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.base_spark import BaseSparkEngineAdapter


class BaseDatabricks(BaseSparkEngineAdapter):
    DIALECT = "databricks"

    def create_snapshots_table(
        self, snapshots_table_name: str, snapshots_columns: t.Dict[str, exp.DataType]
    ) -> None:
        """Create a table to store snapshots."""
        self.create_table(
            snapshots_table_name,
            snapshots_columns,
            partitioned_by=["name", "identifier"],
        )
