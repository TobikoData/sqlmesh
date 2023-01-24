import typing as t
from pathlib import Path

import pytest

from sqlmesh.dbt.datawarehouse import (
    DatabricksConfig,
    DataWarehouseConfig,
    PostgresConfig,
    RedshiftConfig,
    SnowflakeConfig,
)
from sqlmesh.dbt.models import Materialization, ModelConfig
from sqlmesh.dbt.project import ProjectConfig
from sqlmesh.utils.yaml import load as yaml_load


@pytest.fixture
def sushi_dbt_project() -> ProjectConfig:
    return ProjectConfig.load(Path("examples/sushi_dbt"))


@pytest.mark.parametrize(
    "current, new, expected",
    [
        ({}, {"alias": "correct name"}, {"alias": "correct name"}),
        ({"alias": "correct name"}, {}, {"alias": "correct name"}),
        (
            {"alias": "wrong name"},
            {"alias": "correct name"},
            {"alias": "correct name"},
        ),
        ({}, {"tags": ["two"]}, {"tags": ["two"]}),
        ({"tags": ["one"]}, {}, {"tags": ["one"]}),
        ({"tags": ["one"]}, {"tags": ["two"]}, {"tags": ["one", "two"]}),
        ({"tags": "one"}, {"tags": "two"}, {"tags": ["one", "two"]}),
        ({}, {"meta": {"owner": "jen"}}, {"meta": {"owner": "jen"}}),
        ({"meta": {"owner": "jen"}}, {}, {"meta": {"owner": "jen"}}),
        (
            {"meta": {"owner": "bob"}},
            {"meta": {"owner": "jen"}},
            {"meta": {"owner": "jen"}},
        ),
        ({}, {"grants": {"select": ["bob"]}}, {"grants": {"select": ["bob"]}}),
        ({"grants": {"select": ["bob"]}}, {}, {"grants": {"select": ["bob"]}}),
        (
            {"grants": {"select": ["bob"]}},
            {"grants": {"select": ["jen"]}},
            {"grants": {"select": ["bob", "jen"]}},
        ),
        ({"uknown": "field"}, {"uknown": "value"}, {"uknown": "value"}),
        ({"uknown": "field"}, {}, {"uknown": "field"}),
        ({}, {"uknown": "value"}, {"uknown": "value"}),
    ],
)
def test_update(
    current: t.Dict[str, t.Any], new: t.Dict[str, t.Any], expected: t.Dict[str, t.Any]
):
    config = ModelConfig(**current).update_with(new)
    assert {k: v for k, v in config.dict().items() if k in expected} == expected


def test_model_config(sushi_dbt_project: ProjectConfig):
    model_configs = sushi_dbt_project.models
    assert set(model_configs) == {
        "items",
        "customers",
        "orders",
        "order_items",
        "waiters",
        "top_waiters",
        "customer_revenue_by_day",
        "waiter_revenue_by_day",
    }

    customer_revenue_by_day_config = model_configs["customer_revenue_by_day"]

    expected_config = {
        "materialized": Materialization.INCREMENTAL,
        "incremental_strategy": "delete+insert",
        "cluster_by": ["ds"],
        "schema_": "db",
    }
    actual_config = {
        k: getattr(customer_revenue_by_day_config, k)
        for k, v in expected_config.items()
    }
    assert actual_config == expected_config

    assert (
        customer_revenue_by_day_config.model_name == "sushi_db.customer_revenue_by_day"
    )


def test_source_config(sushi_dbt_project: ProjectConfig):
    source_configs = sushi_dbt_project.sources
    assert set(source_configs) == {
        "orders",
        "order_items",
    }

    expected_config = {
        "schema_": "sushi_raw",
        "identifier": "order_items",
    }
    actual_config = {
        k: getattr(source_configs["order_items"], k) for k, v in expected_config.items()
    }
    assert actual_config == expected_config


def test_seed_config(sushi_dbt_project: ProjectConfig):
    seed_configs = sushi_dbt_project.seeds
    assert set(seed_configs) == {"raw_items"}
    raw_items_seed = seed_configs["raw_items"]

    expected_config = {
        "path": Path(sushi_dbt_project.project_root, "seeds/raw/raw_items.csv"),
        "alias": "items",
        "schema_": "raw",
    }
    actual_config = {k: getattr(raw_items_seed, k) for k, v in expected_config.items()}
    assert actual_config == expected_config

    assert raw_items_seed.seed_name == "sushi_raw.items"


def _test_warehouse_config(
    config_yaml: str, config_model: t.Type[DataWarehouseConfig], *params_path: str
):
    config_dict = yaml_load(config_yaml)
    for path in params_path:
        config_dict = config_dict[path]

    config = config_model(**config_dict)

    for key, value in config.dict().items():
        input_value = config_dict.get(key)
        if input_value is not None:
            assert input_value == value


def test_snowflake_config():
    _test_warehouse_config(
        """
        sushi:
          target: dev
          outputs:
            dev:
              account: redacted_account
              database: sushi
              password: redacted_password
              role: accountadmin
              schema: sushi
              threads: 1
              type: snowflake
              user: redacted_user
              warehouse: redacted_warehouse

        """,
        SnowflakeConfig,
        "sushi",
        "outputs",
        "dev",
    )


def test_postgres_config():
    _test_warehouse_config(
        """
        dbt-postgres:
          target: dev
          outputs:
            dev:
              type: postgres
              host: postgres
              user: postgres
              password: postgres
              port: 5432
              dbname: postgres
              schema: demo
              threads: 3
              keepalives_idle: 0
        """,
        PostgresConfig,
        "dbt-postgres",
        "outputs",
        "dev",
    )


def test_redshift_config():
    _test_warehouse_config(
        """
        dbt-redshift:
          target: dev
          outputs:
            dev:
              type: redshift
              host: hostname.region.redshift.amazonaws.com
              user: username
              password: password1
              port: 5439
              dbname: analytics
              schema: analytics
              threads: 4
              ra3_node: false
        """,
        RedshiftConfig,
        "dbt-redshift",
        "outputs",
        "dev",
    )


def test_databricks_config():
    _test_warehouse_config(
        """
        dbt-databricks:
          target: dev
          outputs:
            dev:
              type: databricks
              catalog: test_catalog
              schema: analytics
              host: yourorg.databrickshost.com
              http_path: /sql/your/http/path
              token: dapi01234567890123456789012
        """,
        DatabricksConfig,
        "dbt-databricks",
        "outputs",
        "dev",
    )
