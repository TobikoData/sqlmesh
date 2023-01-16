import typing as t

from sqlmesh.core.engine_adapter._typing import PySparkDataFrame
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.bigquery import BigQueryEngineAdapter
from sqlmesh.core.engine_adapter.databricks import DatabricksEngineAdapter
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.engine_adapter.snowflake import SnowflakeEngineAdapter
from sqlmesh.core.engine_adapter.spark import SparkEngineAdapter
from sqlmesh.core.engine_adapter.transaction_type import TransactionType

DIALECT_TO_ENGINE_ADAPTER = {
    "spark": SparkEngineAdapter,
    "bigquery": BigQueryEngineAdapter,
    "duckdb": DuckDBEngineAdapter,
    "snowflake": SnowflakeEngineAdapter,
    "databricks": DatabricksEngineAdapter,
}


def create_engine_adapter(
    connection_factory: t.Callable[[], t.Any], dialect: str, multithreaded: bool = False
) -> EngineAdapter:
    engine_adapter = DIALECT_TO_ENGINE_ADAPTER.get(dialect)
    if engine_adapter is None:
        return EngineAdapter(connection_factory, dialect, multithreaded=multithreaded)
    return engine_adapter(connection_factory, multithreaded=multithreaded)
