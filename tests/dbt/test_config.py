import typing as t
from pathlib import Path

import pytest

from sqlmesh.core.model import SqlModel
from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.common import DbtContext
from sqlmesh.dbt.model import Materialization, ModelConfig
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.dbt.target import (
    BigQueryConfig,
    DatabricksConfig,
    DuckDbConfig,
    PostgresConfig,
    RedshiftConfig,
    SnowflakeConfig,
    TargetConfig,
)
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load as yaml_load


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
def test_update(current: t.Dict[str, t.Any], new: t.Dict[str, t.Any], expected: t.Dict[str, t.Any]):
    config = ModelConfig(**current).update_with(new)
    assert {k: v for k, v in config.dict().items() if k in expected} == expected


def test_model_config(sushi_test_project: Project):
    assert set(sushi_test_project.packages["sushi"].models) == {
        "waiters",
        "top_waiters",
        "waiter_revenue_by_day",
        "waiter_as_customer_by_day",
    }

    assert set(sushi_test_project.packages["customers"].models) == {
        "customers",
        "customer_revenue_by_day",
    }

    customer_revenue_by_day_config = sushi_test_project.packages["customers"].models[
        "customer_revenue_by_day"
    ]

    context = sushi_test_project.context
    context.sources = {
        "raw.order_items": SourceConfig(target_schema="raw", name="order_items"),
        "raw.items": SourceConfig(target_schema="raw", name="items"),
        "raw.orders": SourceConfig(target_schema="raw", name="orders"),
    }
    context.variables = {}
    rendered = customer_revenue_by_day_config.render_config(context)

    expected_config = {
        "materialized": Materialization.INCREMENTAL,
        "incremental_strategy": "delete+insert",
        "cluster_by": ["ds"],
        "target_schema": "sushi",
    }
    actual_config = {k: getattr(rendered, k) for k, v in expected_config.items()}
    assert actual_config == expected_config

    assert rendered.model_name == "sushi.customer_revenue_by_day"


def test_to_sqlmesh_fields(sushi_test_project: Project):
    model_config = ModelConfig(
        alias="model",
        target_schema="schema",
        schema_="custom",
        database="database",
        materialized=Materialization.TABLE,
        description="test model",
        sql="SELECT 1 AS a FROM foo",
        start="Jan 1 2023",
        partitioned_by=["a"],
        cron="@hourly",
        meta={"stamp": "bar", "dialect": "duckdb"},
        owner="Sally",
        batch_size=2,
    )
    context = DbtContext(project_name="Foo")
    context.target = DuckDbConfig(schema="foo")
    model = model_config.to_sqlmesh(context)

    assert isinstance(model, SqlModel)
    assert model.name == "database.schema_custom.model"
    assert model.description == "test model"
    assert model.query.sql() == "SELECT 1 AS a FROM foo"
    assert model.start == "Jan 1 2023"
    assert model.partitioned_by == ["a"]
    assert model.cron == "@hourly"
    assert model.stamp == "bar"
    assert model.dialect == "duckdb"
    assert model.owner == "Sally"
    assert model.batch_size == 2


def test_model_config_sql_no_config():
    assert (
        ModelConfig(
            sql="""{{
  config(
    materialized='table',
    incremental_strategy='delete+"insert'
  )
}}
query"""
        ).sql_no_config.strip()
        == "query"
    )

    assert (
        ModelConfig(
            sql="""{{
  config(
    materialized='"table"',
    incremental_strategy='delete+insert',
    post_hook=" '{{ macro_call(this) }}' "
  )
}}
query"""
        ).sql_no_config.strip()
        == "query"
    )

    assert (
        ModelConfig(
            sql="""before {{config(materialized='table', post_hook=" {{ macro_call(this) }} ")}} after"""
        ).sql_no_config
        == "before  after"
    )


def test_variables(assert_exp_eq, sushi_test_project):
    # Case 1: using an undefined variable without a default value
    defined_variables = {}
    model_variables = {"foo"}

    model_config = ModelConfig(alias="test", sql="SELECT {{ var('foo') }}")
    model_config._dependencies = Dependencies(variables=model_variables)

    context = sushi_test_project.context
    context.variables = defined_variables

    kwargs = {"context": context}

    with pytest.raises(ConfigError, match=r".*Variable 'foo' was not found.*"):
        model_config = model_config.render_config(context)
        model_config.to_sqlmesh(**kwargs)

    # Case 2: using a defined variable without a default value
    defined_variables["foo"] = 6
    context.variables = defined_variables
    assert_exp_eq(model_config.to_sqlmesh(**kwargs).render_query(), 'SELECT 6 AS "6"')

    # Case 3: using a defined variable with a default value
    model_config.sql = "SELECT {{ var('foo', 5) }}"

    assert_exp_eq(model_config.to_sqlmesh(**kwargs).render_query(), 'SELECT 6 AS "6"')

    # Case 4: using an undefined variable with a default value
    del defined_variables["foo"]
    context.variables = defined_variables

    assert_exp_eq(model_config.to_sqlmesh(**kwargs).render_query(), 'SELECT 5 AS "5"')

    # Finally, check that variable scoping & overwriting (some_var) works as expected
    expected_sushi_variables = {
        "top_waiters:limit": 10,
        "top_waiters:revenue": "revenue",
        "customers:boo": ["a", "b"],
    }
    expected_customer_variables = {
        "some_var": ["foo", "bar"],
        "some_other_var": 5,
        "customers:bla": False,
        "customers:customer_id": "customer_id",
    }

    assert sushi_test_project.packages["sushi"].variables == expected_sushi_variables
    assert sushi_test_project.packages["customers"].variables == expected_customer_variables


def test_source_config(sushi_test_project: Project):
    source_configs = sushi_test_project.packages["sushi"].sources
    assert set(source_configs) == {
        "raw.items",
        "raw.orders",
        "raw.order_items",
    }

    expected_config = {
        "schema_": "raw",
        "identifier": "order_items",
    }
    actual_config = {
        k: getattr(source_configs["raw.order_items"], k) for k, v in expected_config.items()
    }
    assert actual_config == expected_config

    assert source_configs["raw.order_items"].source_name == "raw.order_items"


def test_seed_config(sushi_test_project: Project):
    seed_configs = sushi_test_project.packages["sushi"].seeds
    assert set(seed_configs) == {"waiter_names"}
    raw_items_seed = seed_configs["waiter_names"]

    expected_config = {
        "path": Path(sushi_test_project.context.project_root, "seeds/waiter_names.csv"),
        "target_schema": "sushi",
    }
    actual_config = {k: getattr(raw_items_seed, k) for k, v in expected_config.items()}
    assert actual_config == expected_config

    assert raw_items_seed.model_name == "sushi.waiter_names"


def _test_warehouse_config(config_yaml: str, config_model: t.Type[TargetConfig], *params_path: str):
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


def test_bigquery_config():
    _test_warehouse_config(
        """
        dbt-bigquery:
          target: dev
          outputs:
            dev:
              type: bigquery
              method: oauth
              project: your-project
              dataset: your-dataset
              threads: 1
              location: US
              keyfile: /path/to/keyfile.json
        """,
        BigQueryConfig,
        "dbt-bigquery",
        "outputs",
        "dev",
    )
    _test_warehouse_config(
        """
        dbt-bigquery:
          target: dev
          outputs:
            dev:
              type: bigquery
              method: oauth
              project: your-project
              schema: your-dataset
              threads: 1
              location: US
              keyfile: /path/to/keyfile.json
        """,
        BigQueryConfig,
        "dbt-bigquery",
        "outputs",
        "dev",
    )
    with pytest.raises(ConfigError):
        _test_warehouse_config(
            """
            dbt-bigquery:
              target: dev
              outputs:
                dev:
                  type: bigquery
                  method: oauth
                  project: your-project
                  threads: 1
                  location: US
                  keyfile: /path/to/keyfile.json
            """,
            BigQueryConfig,
            "dbt-bigquery",
            "outputs",
            "dev",
        )
