import os

import duckdb

from sqlmesh.core.config import AirflowSchedulerBackend, Config

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


DEFAULT_KWARGS = {
    "engine_dialect": "duckdb",
    "engine_connection_factory": duckdb.connect,
}

# An in memory DuckDB config.
config = Config(**DEFAULT_KWARGS)

# A stateful DuckDB config.
local_config = Config(
    **{
        **DEFAULT_KWARGS,
        "engine_connection_factory": lambda: duckdb.connect(
            database=f"{DATA_DIR}/local.duckdb"
        ),
    }
)

# The config to run model tests.
test_config = Config(**DEFAULT_KWARGS)


# A config that uses Airflow + Spark.
DEFAULT_AIRFLOW_KWARGS = {
    **DEFAULT_KWARGS,
    "backfill_concurrent_tasks": 4,
    "ddl_concurrent_tasks": 4,
}


airflow_config = Config(
    **{
        **DEFAULT_AIRFLOW_KWARGS,
        "scheduler_backend": AirflowSchedulerBackend(),
    }
)


airflow_config_docker = Config(
    **{
        **DEFAULT_AIRFLOW_KWARGS,
        "scheduler_backend": AirflowSchedulerBackend(
            airflow_url="http://airflow-webserver:8080/"
        ),
    }
)
