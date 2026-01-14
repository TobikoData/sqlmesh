import os

from sqlmesh.core.config.common import VirtualEnvironmentMode, TableNamingConvention
from sqlmesh.core.config import (
    AutoCategorizationMode,
    BigQueryConnectionConfig,
    CategorizerConfig,
    Config,
    DuckDBConnectionConfig,
    EnvironmentSuffixTarget,
    GatewayConfig,
    ModelDefaultsConfig,
    PlanConfig,
)
from sqlmesh.core.config.linter import LinterConfig
from sqlmesh.core.notification_target import (
    BasicSMTPNotificationTarget,
    SlackApiNotificationTarget,
    SlackWebhookNotificationTarget,
)
from sqlmesh.core.user import User, UserRole

CURRENT_FILE_PATH = os.path.abspath(__file__)
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


defaults = {"dialect": "duckdb"}
model_defaults = ModelDefaultsConfig(**defaults)
model_defaults_iceberg = ModelDefaultsConfig(**defaults, storage_format="iceberg")
before_all = [
    "CREATE SCHEMA IF NOT EXISTS raw",
    "DROP VIEW IF EXISTS raw.demographics",
    "CREATE VIEW raw.demographics AS (SELECT 1 AS customer_id, '00000' AS zip)",
]


# A DuckDB config, in-memory by default.
config = Config(
    gateways={
        "duckdb": GatewayConfig(
            connection=DuckDBConnectionConfig(),
        ),
        "duckdb_persistent": GatewayConfig(
            connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/duckdb.db"),
        ),
    },
    default_gateway="duckdb",
    model_defaults=model_defaults,
    linter=LinterConfig(
        enabled=False,
        rules=[
            "ambiguousorinvalidcolumn",
            "invalidselectstarexpansion",
            "noselectstar",
            "nomissingaudits",
            "nomissingowner",
            "nomissingexternalmodels",
        ],
    ),
    before_all=before_all,
)

bigquery_config = Config(
    gateways={
        "bq": GatewayConfig(
            connection=BigQueryConnectionConfig(),
            state_connection=DuckDBConnectionConfig(database=f"{DATA_DIR}/bigquery.duckdb"),
        )
    },
    default_gateway="bq",
    model_defaults=model_defaults,
    before_all=before_all,
)

# A configuration used for SQLMesh tests.
test_config = Config(
    gateways={"in_memory": GatewayConfig(connection=DuckDBConnectionConfig())},
    default_gateway="in_memory",
    plan=PlanConfig(
        auto_categorize_changes=CategorizerConfig(
            sql=AutoCategorizationMode.SEMI, python=AutoCategorizationMode.OFF
        )
    ),
    model_defaults=model_defaults,
    before_all=before_all,
)

# A configuration used for SQLMesh tests with virtual environment mode set to DEV_ONLY.
test_config_virtual_environment_mode_dev_only = test_config.copy(
    update={
        "virtual_environment_mode": VirtualEnvironmentMode.DEV_ONLY,
        "plan": PlanConfig(
            auto_categorize_changes=CategorizerConfig.all_full(),
        ),
    },
)

# A DuckDB config with a physical schema map.
map_config = Config(
    default_connection=DuckDBConnectionConfig(),
    physical_schema_mapping={"^sushi$": "company_internal"},
    model_defaults=model_defaults,
    before_all=before_all,
)

# A config representing isolated systems with a gateway per system
isolated_systems_config = Config(
    gateways={
        "dev": GatewayConfig(connection=DuckDBConnectionConfig()),
        "test": GatewayConfig(connection=DuckDBConnectionConfig()),
        "prod": GatewayConfig(connection=DuckDBConnectionConfig()),
    },
    default_gateway="dev",
    model_defaults=model_defaults,
    before_all=before_all,
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
    model_defaults=model_defaults,
    before_all=before_all,
)


environment_suffix_table_config = Config(
    default_connection=DuckDBConnectionConfig(),
    model_defaults=model_defaults,
    environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    before_all=before_all,
)

environment_suffix_catalog_config = environment_suffix_table_config.model_copy(
    update={
        "environment_suffix_target": EnvironmentSuffixTarget.CATALOG,
    },
)

CATALOGS = {
    "in_memory": ":memory:",
    "other_catalog": ":memory:",
}

local_catalogs = Config(
    default_connection=DuckDBConnectionConfig(catalogs=CATALOGS),
    default_test_connection=DuckDBConnectionConfig(catalogs=CATALOGS),
    model_defaults=model_defaults,
    before_all=before_all,
)

environment_catalog_mapping_config = Config(
    default_connection=DuckDBConnectionConfig(
        catalogs={
            "physical": ":memory:",
            "prod_catalog": ":memory:",
            "dev_catalog": ":memory:",
        }
    ),
    model_defaults=model_defaults,
    environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    environment_catalog_mapping={
        "^prod$": "prod_catalog",
        ".*": "dev_catalog",
    },
    before_all=before_all,
)

hash_md5_naming_config = config.copy(
    update={"physical_table_naming_convention": TableNamingConvention.HASH_MD5}
)

table_only_naming_config = config.copy(
    update={"physical_table_naming_convention": TableNamingConvention.TABLE_ONLY}
)
