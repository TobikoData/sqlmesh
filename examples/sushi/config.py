import os

from sqlmesh.core.config import (
    AirflowSchedulerConfig,
    AutoCategorizationMode,
    CategorizerConfig,
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    SparkConnectionConfig,
)
from sqlmesh.core.user import User, UserRole

CURRENT_FILE_PATH = os.path.abspath(__file__)
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

# Due to a 3.7 mypy bug we ignore. Can remove once 3.7 support is dropped.
airflow_config = Config(  # type: ignore
    default_scheduler=AirflowSchedulerConfig(),
    gateways=GatewayConfig(
        connection=SparkConnectionConfig(
            config_dir=os.path.join(CURRENT_FILE_PATH, "..", "airflow", "spark_conf"),
            config={
                "spark.hadoop.javax.jdo.option.ConnectionURL": "jdbc:postgresql://localhost:5432/metastore_db"
            },
        )
    ),
)


# Due to a 3.7 mypy bug we ignore. Can remove once 3.7 support is dropped.
airflow_config_docker = Config(  # type: ignore
    default_scheduler=AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/"),
    gateways=GatewayConfig(connection=SparkConnectionConfig()),
)


required_approvers_config = Config(
    default_connection=DuckDBConnectionConfig(),
    users=[User(username="test", roles=[UserRole.REQUIRED_APPROVER])],
)
