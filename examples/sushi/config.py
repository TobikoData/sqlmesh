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
config = Config(connections=DuckDBConnectionConfig())


# A configuration used for SQLMesh tests.
test_config = Config(
    connections=DuckDBConnectionConfig(),
    auto_categorize_changes=CategorizerConfig(sql=AutoCategorizationMode.SEMI),
)

# A stateful DuckDB config.
local_config = Config(
    connections=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb"),
)


airflow_config = Config(**{"scheduler": AirflowSchedulerConfig()})


airflow_config_docker = Config(
    **{"scheduler": AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/")},
)
