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
from sqlmesh.core.engine_adapter.redshift import RedshiftEngineAdapter
from sqlmesh.core.engine_adapter.shared import TransactionType
from sqlmesh.core.engine_adapter.snowflake import SnowflakeEngineAdapter
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter

DIALECT_TO_ENGINE_ADAPTER = {
    "spark": SparkEngineAdapter,
    "bigquery": BigQueryEngineAdapter,
    "duckdb": DuckDBEngineAdapter,
    "snowflake": SnowflakeEngineAdapter,
    "databricks": DatabricksSparkSessionEngineAdapter,
    "redshift": RedshiftEngineAdapter,
    "postgres": EngineAdapterWithIndexSupport,
    "mysql": EngineAdapterWithIndexSupport,
    "mssql": EngineAdapterWithIndexSupport,
}


def create_engine_adapter(
    connection_factory: t.Callable[[], t.Any], dialect: str, multithreaded: bool = False
) -> EngineAdapter:
    dialect = dialect.lower()
    if dialect == "postgresql":
        dialect = "postgres"
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
        return EngineAdapter(connection_factory, dialect, multithreaded=multithreaded)
    if engine_adapter is EngineAdapterWithIndexSupport:
        return EngineAdapterWithIndexSupport(
            connection_factory, dialect, multithreaded=multithreaded
        )
    return engine_adapter(connection_factory, multithreaded=multithreaded)
