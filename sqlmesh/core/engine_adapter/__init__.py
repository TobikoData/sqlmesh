from __future__ import annotations

import typing as t

from sqlmesh.core.engine_adapter._typing import PySparkDataFrame
from sqlmesh.core.engine_adapter.base import (
    EngineAdapter,
    EngineAdapterWithIndexSupport,
)
from sqlmesh.core.engine_adapter.bigquery import BigQueryEngineAdapter
from sqlmesh.core.engine_adapter.databricks import DatabricksSparkSessionEngineAdapter
from sqlmesh.core.engine_adapter.databricks_api import DatabricksSQLEngineAdapter
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.engine_adapter.postgres import PostgresEngineAdapter
from sqlmesh.core.engine_adapter.redshift import RedshiftEngineAdapter
from sqlmesh.core.engine_adapter.shared import TransactionType
from sqlmesh.core.engine_adapter.snowflake import SnowflakeEngineAdapter
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core.config.connection import BigQueryExecutionConfig

DIALECT_TO_ENGINE_ADAPTER = {
    "spark": SparkEngineAdapter,
    "bigquery": BigQueryEngineAdapter,
    "duckdb": DuckDBEngineAdapter,
    "snowflake": SnowflakeEngineAdapter,
    "databricks": DatabricksSparkSessionEngineAdapter,
    "redshift": RedshiftEngineAdapter,
    "postgres": PostgresEngineAdapter,
    "mysql": EngineAdapterWithIndexSupport,
    "mssql": EngineAdapterWithIndexSupport,
}

DIALECT_ALIASES = {
    "postgresql": "postgres",
}


def create_engine_adapter(
    connection_factory: t.Callable[[], t.Any],
    dialect: str,
    multithreaded: bool = False,
    execution_config: t.Optional[BigQueryExecutionConfig] = None,
) -> EngineAdapter:
    dialect = dialect.lower()
    dialect = DIALECT_ALIASES.get(dialect, dialect)
    if dialect == "databricks":
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark:
                engine_adapter: t.Optional[
                    t.Type[EngineAdapter]
                ] = DatabricksSparkSessionEngineAdapter
            else:
                engine_adapter = DatabricksSQLEngineAdapter
        except ImportError:
            engine_adapter = DatabricksSQLEngineAdapter
    else:
        engine_adapter = DIALECT_TO_ENGINE_ADAPTER.get(dialect)
    if engine_adapter is None:
        return EngineAdapter(
            connection_factory,
            dialect,
            multithreaded=multithreaded,
            execution_config=execution_config,
        )
    if engine_adapter is EngineAdapterWithIndexSupport:
        return EngineAdapterWithIndexSupport(
            connection_factory,
            dialect,
            multithreaded=multithreaded,
            execution_config=execution_config,
        )
    return engine_adapter(
        connection_factory, multithreaded=multithreaded, execution_config=execution_config
    )
