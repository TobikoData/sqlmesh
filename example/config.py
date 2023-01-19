import os

from sqlmesh.core.config import AirflowSchedulerConfig, Config, DuckDBConnectionConfig

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# An in memory DuckDB config.
config = Config()

# A stateful DuckDB config.
local_config = Config(
    connections=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb")
)

# The config to run model tests.
test_config = Config()


# A config that uses Airflow + Spark.
DEFAULT_AIRFLOW_KWARGS = {
    "backfill_concurrent_tasks": 4,
    "ddl_concurrent_tasks": 4,
}


airflow_config = Config(
    **{
        **DEFAULT_AIRFLOW_KWARGS,
        "scheduler": AirflowSchedulerConfig(),
    }
)


airflow_config_docker = Config(
    **{
        **DEFAULT_AIRFLOW_KWARGS,
        "scheduler": AirflowSchedulerConfig(
            airflow_url="http://airflow-webserver:8080/"
        ),
    }
)
