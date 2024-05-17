import os

from sqlmesh.core.config import (
    AirflowSchedulerConfig,
    AutoCategorizationMode,
    BigQueryConnectionConfig,
    CategorizerConfig,
    Config,
    DuckDBConnectionConfig,
    EnvironmentSuffixTarget,
    GatewayConfig,
    ModelDefaultsConfig,
    PlanConfig,
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
config = Config(
    default_connection=DuckDBConnectionConfig(),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

bigquery_config = Config(
    gateways={
        "gcp": GatewayConfig(
            connection=BigQueryConnectionConfig(),
            state_connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/bigquery.duckdb"),
        )
    },
    default_gateway="gcp",
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

# A configuration used for SQLMesh tests.
test_config = Config(
    gateways={"in_memory": GatewayConfig(connection=DuckDBConnectionConfig())},
    default_gateway="in_memory",
    plan=PlanConfig(auto_categorize_changes=CategorizerConfig(sql=AutoCategorizationMode.SEMI)),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

# A stateful DuckDB config.
local_config = Config(
    default_connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/local.duckdb"),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

airflow_config = Config(
    default_scheduler=AirflowSchedulerConfig(),
    gateways=GatewayConfig(
        connection=SparkConnectionConfig(
            config_dir=os.path.join(CURRENT_FILE_PATH, "..", "airflow", "spark_conf"),
            config={
                "spark.hadoop.javax.jdo.option.ConnectionURL": "jdbc:postgresql://localhost:5432/metastore_db"
            },
        )
    ),
    model_defaults=ModelDefaultsConfig(dialect="duckdb", storage_format="iceberg"),
)


airflow_config_docker = Config(
    default_scheduler=AirflowSchedulerConfig(airflow_url="http://airflow-webserver:8080/"),
    gateways=GatewayConfig(connection=SparkConnectionConfig()),
    model_defaults=ModelDefaultsConfig(dialect="duckdb", storage_format="iceberg"),
)

# A DuckDB config with a physical schema map.
map_config = Config(
    default_connection=DuckDBConnectionConfig(),
    physical_schema_override={"sushi": "company_internal"},
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
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
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)


environment_suffix_config = Config(
    default_connection=DuckDBConnectionConfig(),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    environment_suffix_target=EnvironmentSuffixTarget.TABLE,
)


CATALOGS = {
    "in_memory": ":memory:",
    "other_catalog": ":memory:",
}

local_catalogs = Config(
    default_connection=DuckDBConnectionConfig(catalogs=CATALOGS),
    default_test_connection=DuckDBConnectionConfig(catalogs=CATALOGS),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

environment_catalog_mapping_config = Config(
    default_connection=DuckDBConnectionConfig(
        catalogs={
            "physical": ":memory:",
            "prod_catalog": ":memory:",
            "dev_catalog": ":memory:",
        }
    ),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    environment_catalog_mapping={
        "^prod$": "prod_catalog",
        ".*": "dev_catalog",
    },
)
