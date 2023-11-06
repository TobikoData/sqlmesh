from __future__ import annotations

import typing as t
from unittest.mock import call

import pytest
from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from dbt.contracts.relation import Policy
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.dialect import schema_
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.target import SnowflakeConfig
from sqlmesh.utils.errors import ConfigError


def test_adapter_relation(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)

    engine_adapter.create_schema("foo")
    engine_adapter.create_schema("ignored")
    engine_adapter.create_table(
        table_name="foo.bar", columns_to_types={"baz": exp.DataType.build("int")}
    )
    engine_adapter.create_table(
        table_name="foo.another", columns_to_types={"col": exp.DataType.build("int")}
    )
    engine_adapter.create_table(
        table_name="ignored.ignore", columns_to_types={"col": exp.DataType.build("int")}
    )

    assert (
        renderer("{{ adapter.get_relation(database=None, schema='foo', identifier='bar') }}")
        == '"memory"."foo"."bar"'
    )
    assert renderer(
        "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_columns_in_relation(relation) }}"
    ) == str([Column.from_description(name="baz", raw_data_type="INT")])

    assert renderer("{{ adapter.list_relations(database=None, schema='foo')|length }}") == "2"

    assert (
        renderer(
            """
        {%- set from = adapter.get_relation(database=None, schema='foo', identifier='bar') -%}
        {%- set to = adapter.get_relation(database=None, schema='foo', identifier='another') -%}
        {{ adapter.get_missing_columns(from, to) -}}
        """
        )
        == str([Column.from_description(name="baz", raw_data_type="INT")])
    )

    assert (
        renderer(
            "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_missing_columns(relation, relation) }}"
        )
        == "[]"
    )


def test_normalization(
    sushi_test_project: Project, runtime_renderer: t.Callable, mocker: MockerFixture
):
    context = sushi_test_project.context
    assert context.target

    adapter_mock = mocker.MagicMock()
    adapter_mock.dialect = "snowflake"

    context.target = SnowflakeConfig(
        account="test",
        user="test",
        authenticator="test",
        name="test",
        database="test",
        schema="test",
    )

    renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    # bla and bob will be normalized to uppercase since we're dealing with Snowflake
    schema_bla = schema_("BLA")
    relation_bla_bob = exp.table_("BOB", db="BLA")

    renderer("{{ adapter.get_relation(database=None, schema='bla', identifier='bob') }}")
    adapter_mock._get_data_objects.assert_has_calls([call(schema_bla)])

    renderer(
        "{%- set relation = api.Relation.create(schema='bla') -%}"
        "{{ adapter.create_schema(relation) }}"
    )
    adapter_mock.create_schema.assert_has_calls([call(schema_bla)])

    renderer(
        "{%- set relation = api.Relation.create(schema='bla') -%}"
        "{{ adapter.drop_schema(relation) }}"
    )
    adapter_mock.drop_schema.assert_has_calls([call(schema_bla)])

    renderer(
        "{%- set relation = api.Relation.create(schema='bla', identifier='bob') -%}"
        "{{ adapter.drop_relation(relation) }}"
    )
    adapter_mock.drop_table.assert_has_calls([call(relation_bla_bob)])

    select_star_query: exp.Select = exp.maybe_parse("SELECT * FROM t", dialect="snowflake")

    # The following call to run_query won't return dataframes and so we're expected to
    # raise in adapter.execute right before returning from the method
    with pytest.raises(AssertionError):
        renderer("{{ run_query('SELECT * FROM t') }}")
    adapter_mock.fetchdf.assert_has_calls([call(select_star_query, quote_identifiers=False)])

    renderer("{% call statement('something') %} {{ 'SELECT * FROM t' }} {% endcall %}")
    adapter_mock.execute.assert_has_calls([call(select_star_query, quote_identifiers=False)])

    # Enforce case-sensitivity for database object names
    setattr(
        context.target.__class__,
        "quote_policy",
        Policy(database=True, schema=True, identifier=True),
    )

    adapter_mock.drop_table.reset_mock()
    renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    bla_id = exp.to_identifier("bla", quoted=True)
    bob_id = exp.to_identifier("bob", quoted=True)

    # Ensures we'll pass lowercase names to the engine
    renderer(
        "{%- set relation = api.Relation.create(schema='bla', identifier='bob') -%}"
        "{{ adapter.drop_relation(relation) }}"
    )
    adapter_mock.drop_table.assert_has_calls([call(exp.table_("bob", db="bla", quoted=True))])


def test_adapter_dispatch(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    renderer = runtime_renderer(context)
    assert renderer("{{ adapter.dispatch('current_engine', 'customers')() }}") == "duckdb"

    with pytest.raises(ConfigError, match=r"Macro 'current_engine'.*was not found."):
        renderer("{{ adapter.dispatch('current_engine')() }}")


def test_adapter_map_snapshot_tables(
    sushi_test_project: Project, runtime_renderer: t.Callable, mocker: MockerFixture
):
    snapshot_mock = mocker.Mock()
    snapshot_mock.name = "test_db.test_model"
    snapshot_mock.version = "1"
    snapshot_mock.is_symbolic = False
    snapshot_mock.table_name.return_value = "sqlmesh.test_db__test_model"

    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(
        context,
        engine_adapter=engine_adapter,
        snapshots={"test_db.test_model": snapshot_mock},
        test_model=BaseRelation.create(schema="test_db", identifier="test_model"),
        foo_bar=BaseRelation.create(schema="foo", identifier="bar"),
    )

    engine_adapter.create_schema("foo")
    engine_adapter.create_schema("sqlmesh")
    engine_adapter.create_table(
        table_name="sqlmesh.test_db__test_model",
        columns_to_types={"baz": exp.DataType.build("int")},
    )
    engine_adapter.create_table(
        table_name="foo.bar", columns_to_types={"col": exp.DataType.build("int")}
    )

    assert (
        renderer(
            "{{ adapter.get_relation(database=none, schema='test_db', identifier='test_model') }}"
        )
        == '"memory"."sqlmesh"."test_db__test_model"'
    )

    assert "baz" in renderer("{{ run_query('SELECT * FROM test_db.test_model') }}")

    assert (
        renderer("{{ adapter.get_relation(database=none, schema='foo', identifier='bar') }}")
        == '"memory"."foo"."bar"'
    )

    assert renderer("{{ adapter.resolve_schema(test_model) }}") == "sqlmesh"
    assert renderer("{{ adapter.resolve_identifier(test_model) }}") == "test_db__test_model"

    assert renderer("{{ adapter.resolve_schema(foo_bar) }}") == "foo"
    assert renderer("{{ adapter.resolve_identifier(foo_bar) }}") == "bar"
