import os

from sqlmesh.core.config import (
    AirflowSchedulerConfig,
    AutoCategorizationMode,
    CategorizerConfig,
    Config,
    DuckDBConnectionConfig,
)
from sqlmesh.core.user import User, UserRole

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# An in memory DuckDB config.
config = Config(default_connection=DuckDBConnectionConfig())


# A configuration used for SQLMesh tests.
test_config = Config(
    default_connection=DuckDBConnectionConfig(),
    auto_categorize_changes=CategorizerConfig(sql=AutoCategorizationMode.SEMI),
)

# A stateful DuckDB config.
local_config = Config(
    default_connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb"),
)


# Bug in mypy 1.0.1 that makes us have to ignore
airflow_config = Config(default_scheduler=AirflowSchedulerConfig())  # type: ignore


# Bug in mypy 1.0.1 that makes us have to ignore
airflow_config_docker = Config(
    default_scheduler=AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/"),  # type: ignore
)


required_approvers_config = Config(
    default_connection=DuckDBConnectionConfig(),
    users=[User(username="test", roles=[UserRole.REQUIRED_APPROVER])],
)
