import os

from sqlmesh.core.config import (
    AirflowSchedulerConfig,
    AutoCategorizationMode,
    CategorizerConfig,
    Config,
    DuckDBConnectionConfig,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# An in memory DuckDB config.
config = Config(
    default_connection=DuckDBConnectionConfig(),
    auto_categorize_changes=CategorizerConfig(python=AutoCategorizationMode.FULL),
)


# A configuration used for SQLMesh tests.
test_config = Config(
    default_connection=DuckDBConnectionConfig(),
    auto_categorize_changes=CategorizerConfig(sql=AutoCategorizationMode.SEMI),
)

# A stateful DuckDB config.
local_config = Config(
    default_connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb"),
    auto_categorize_changes=CategorizerConfig(python=AutoCategorizationMode.FULL),
)


airflow_config = Config(default_scheduler=AirflowSchedulerConfig())


airflow_config_docker = Config(
    default_scheduler=AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/"),
)
