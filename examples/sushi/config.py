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
from sqlmesh.core.notification_target import (
    BasicSMTPNotificationTarget,
    SlackApiNotificationTarget,
    SlackWebhookNotificationTarget,
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
    users=[
        User(
            username="admin",
            roles=[UserRole.REQUIRED_APPROVER],
            notification_targets=[
                SlackApiNotificationTarget(
                    notify_on=["apply_start", "apply_failure", "apply_end", "audit_failure"],
                    token=os.getenv("ADMIN_SLACK_API_TOKEN"),
                    channel="UXXXXXXXXX",  # User's Slack member ID
                ),
            ],
        )
    ],
    notification_targets=[
        SlackWebhookNotificationTarget(
            notify_on=["apply_start", "apply_failure", "run_start"],
            url=os.getenv("SLACK_WEBHOOK_URL"),
        ),
        BasicSMTPNotificationTarget(
            notify_on=["run_failure"],
            host=os.getenv("SMTP_HOST"),
            user=os.getenv("SMTP_USER"),
            password=os.getenv("SMTP_PASSWORD"),
            sender="sushi@example.com",
            recipients=[
                "team@example.com",
            ],
        ),
    ],
)
