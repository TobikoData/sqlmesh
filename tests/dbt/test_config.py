import typing as t
from pathlib import Path

import pytest
from dbt.adapters.base import BaseRelation

from sqlmesh.core.model import SqlModel
from sqlmesh.dbt.common import QuotingConfig
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import IncrementalByUniqueKeyKind, Materialization, ModelConfig
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
from sqlmesh.dbt.test import TestConfig
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


def test_model_to_sqlmesh_fields():
    model_config = ModelConfig(
        name="name",
        package_name="package",
        alias="model",
        schema="custom",
        database="database",
        materialized=Materialization.INCREMENTAL,
        description="test model",
        sql="SELECT 1 AS a FROM foo",
        start="Jan 1 2023",
        partition_by=["a"],
        cluster_by=["a", '"b"'],
        cron="@hourly",
        batch_size=5,
        lookback=3,
        unique_key=["a"],
        meta={"stamp": "bar"},
        owner="Sally",
        tags=["test", "incremental"],
    )
    context = DbtContext()
    context.project_name = "Foo"
    context.target = DuckDbConfig(name="target", schema="foo")
    model = model_config.to_sqlmesh(context)

    assert isinstance(model, SqlModel)
    assert model.name == "database.custom.model"
    assert model.description == "test model"
    assert model.render_query_or_raise().sql() == 'SELECT 1 AS "a" FROM "foo" AS "foo"'
    assert model.start == "Jan 1 2023"
    assert [col.sql() for col in model.partitioned_by] == ["a"]
    assert model.clustered_by == ["a", "b"]
    assert model.cron == "@hourly"
    assert model.stamp == "bar"
    assert model.dialect == "duckdb"
    assert model.owner == "Sally"
    assert model.tags == ["test", "incremental"]
    kind = t.cast(IncrementalByUniqueKeyKind, model.kind)
    assert kind.batch_size == 5
    assert kind.lookback == 3


def test_test_to_sqlmesh_fields():
    sql = "SELECT * FROM FOO WHERE cost > 100"
    test_config = TestConfig(
        name="foo_test",
        sql=sql,
        owner="Foo",
        column_name="cost",
        severity="ERROR",
        enabled=True,
    )

    context = DbtContext()
    context._project_name = "Foo"
    context.target = DuckDbConfig(name="target", schema="foo")
    audit = test_config.to_sqlmesh(context)

    assert audit.name == "foo_test"
    assert audit.dialect == "duckdb"
    assert audit.skip == False
    assert audit.blocking == True
    assert sql in audit.query.sql()

    sql = "SELECT * FROM FOO WHERE NOT id IS NULL"
    test_config = TestConfig(
        name="foo_null_test",
        sql=sql,
        owner="Foo",
        column_name="id",
        severity="WARN",
        enabled=False,
    )

    audit = test_config.to_sqlmesh(context)

    assert audit.name == "foo_null_test"
    assert audit.dialect == "duckdb"
    assert audit.skip == True
    assert audit.blocking == False
    assert sql in audit.query.sql()


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
    materialized='table',
    incremental_strategy='delete+insert',
    post_hook=" '{{ var('new') }}' "
  )
}}
query"""
        ).sql_no_config.strip()
        == "query"
    )

    assert (
        ModelConfig(
            sql="""before {{config(materialized='table', post_hook=" {{ var('new') }} ")}} after"""
        ).sql_no_config.strip()
        == "before  after"
    )


def test_variables(assert_exp_eq, sushi_test_project):
    # Case 1: using an undefined variable without a default value
    defined_variables = {}

    context = sushi_test_project.context
    context.variables = defined_variables

    model_config = ModelConfig(alias="test", sql="SELECT {{ var('foo') }}")

    kwargs = {"context": context}

    # Case 2: using a defined variable without a default value
    defined_variables["empty_list_var"] = []
    defined_variables["jinja_list_var"] = ["{{ 1 + 1 }}"]
    defined_variables["bar"] = "{{ 2 * 3 }}"
    defined_variables["foo"] = "{{ var('bar') }}"
    context.set_and_render_variables(defined_variables, "test_package")
    assert context.variables == {
        "empty_list_var": [],
        "jinja_list_var": ["2"],
        "bar": "6",
        "foo": "6",
    }

    sqlmesh_model = model_config.to_sqlmesh(**kwargs)
    assert_exp_eq(sqlmesh_model.render_query(), 'SELECT 6 AS "6"')
    assert sqlmesh_model.jinja_macros.global_objs["vars"]["bar"] == "6"
    assert sqlmesh_model.jinja_macros.global_objs["vars"]["foo"] == "6"

    # Case 3: using a defined variable with a default value
    model_config.sql = "SELECT {{ var('foo', 5) }}"
    model_config._sql_no_config = None

    assert_exp_eq(model_config.to_sqlmesh(**kwargs).render_query(), 'SELECT 6 AS "6"')

    # Case 4: using an undefined variable with a default value
    del defined_variables["foo"]
    context.variables = defined_variables

    assert_exp_eq(model_config.to_sqlmesh(**kwargs).render_query(), 'SELECT 5 AS "5"')

    # Finally, check that variable scoping & overwriting (some_var) works as expected
    expected_sushi_variables = {
        "yet_another_var": 1,
        "top_waiters:limit": 10,
        "top_waiters:revenue": "revenue",
        "customers:boo": ["a", "b"],
    }
    expected_customer_variables = {
        "some_var": ["foo", "bar"],
        "some_other_var": 5,
        "yet_another_var": 1,
        "customers:bla": False,
        "customers:customer_id": "customer_id",
        "top_waiters:limit": 10,
        "top_waiters:revenue": "revenue",
        "customers:boo": ["a", "b"],
    }

    assert sushi_test_project.packages["sushi"].variables == expected_sushi_variables
    assert sushi_test_project.packages["customers"].variables == expected_customer_variables


def test_source_config(sushi_test_project: Project):
    source_configs = sushi_test_project.packages["sushi"].sources
    assert set(source_configs) == {
        "streaming.items",
        "streaming.orders",
        "streaming.order_items",
    }

    expected_config = {
        "schema_": "raw",
        "identifier": "order_items",
    }
    actual_config = {
        k: getattr(source_configs["streaming.order_items"], k) for k, v in expected_config.items()
    }
    assert actual_config == expected_config

    assert source_configs["streaming.order_items"].sql_name == "raw.order_items"


def test_seed_config(sushi_test_project: Project):
    seed_configs = sushi_test_project.packages["sushi"].seeds
    assert set(seed_configs) == {"waiter_names"}
    raw_items_seed = seed_configs["waiter_names"]

    expected_config = {
        "path": Path(sushi_test_project.context.project_root, "seeds/waiter_names.csv"),
        "schema_": "sushi",
    }
    actual_config = {k: getattr(raw_items_seed, k) for k, v in expected_config.items()}
    assert actual_config == expected_config

    assert raw_items_seed.sql_name == "sushi.waiter_names"


def test_quoting():
    model = ModelConfig(alias="bar", schema="foo")
    assert str(BaseRelation.create(**model.relation_info)) == '"foo"."bar"'

    model.quoting.identifier = False
    assert str(BaseRelation.create(**model.relation_info)) == '"foo".bar'

    source = SourceConfig(identifier="bar", schema="foo")
    assert str(BaseRelation.create(**source.relation_info)) == '"foo"."bar"'

    source.quoting.schema_ = False
    assert str(BaseRelation.create(**source.relation_info)) == 'foo."bar"'


def _test_warehouse_config(config_yaml: str, target_class: t.Type[TargetConfig], *params_path: str):
    config_dict = yaml_load(config_yaml)
    for path in params_path:
        config_dict = config_dict[path]

    config = target_class(**{"name": "dev", **config_dict})

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


def test_quoting_config():
    assert QuotingConfig.parse_obj(
        {"database": None, "identifier": None, "schema": None}
    ) == QuotingConfig(database=None, identifier=None, schema=None)

    assert QuotingConfig.parse_obj(
        {"database": False, "identifier": False, "schema": False}
    ) == QuotingConfig(database=False, identifier=False, schema=False)

    assert QuotingConfig.parse_obj(
        {"database": True, "identifier": True, "schema": True}
    ) == QuotingConfig(database=True, identifier=True, schema=True)
