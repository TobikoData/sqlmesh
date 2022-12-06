import os

import duckdb

from sqlmesh.core.config import AirflowSchedulerBackend, Config
from sqlmesh.core.engine_adapter import EngineAdapter

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


DEFAULT_KWARGS = {
    "dialect": "duckdb",  # The default dialect of models is DuckDB SQL.
    "engine_adapter": EngineAdapter(
        duckdb.connect(), "duckdb"
    ),  # The default engine runs in DuckDB.
}

# An in memory DuckDB config.
config = Config(**DEFAULT_KWARGS)

# A stateful DuckDB config.
local_config = Config(
    **{
        **DEFAULT_KWARGS,
        "engine_adapter": EngineAdapter(
            lambda: duckdb.connect(database=f"{DATA_DIR}/local.duckdb"), "duckdb"
        ),
    }
)

# The config to run model tests.
test_config = Config(**DEFAULT_KWARGS)


# A config that uses Airflow + Spark.
airflow_config = Config(
    **{
        **DEFAULT_KWARGS,
        "scheduler_backend": AirflowSchedulerBackend(),
    }
)


airflow_config_docker = Config(
    **{
        **DEFAULT_KWARGS,
        "scheduler_backend": AirflowSchedulerBackend(
            airflow_url="http://airflow-webserver:8080/"
        ),
    }
)
