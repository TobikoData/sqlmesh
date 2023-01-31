import os

from sqlmesh.core.config import AirflowSchedulerConfig, Config, DuckDBConnectionConfig

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# An in memory DuckDB config.
config = Config(connections=DuckDBConnectionConfig())

# A stateful DuckDB config.
local_config = Config(
    connections=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb"),
)


airflow_config = Config(scheduler=AirflowSchedulerConfig())


airflow_config_docker = Config(
    scheduler=AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/"),
)
