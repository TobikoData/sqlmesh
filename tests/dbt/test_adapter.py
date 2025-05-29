from __future__ import annotations

import os
import typing as t
from unittest import mock
from unittest.mock import call

import pytest
from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh import Context
from sqlmesh.core.dialect import schema_
from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.dbt.adapter import ParsetimeAdapter
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.relation import Policy
from sqlmesh.dbt.target import SnowflakeConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry

pytestmark = pytest.mark.dbt


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

    assert renderer(
        """
        {%- set from = adapter.get_relation(database=None, schema='foo', identifier='bar') -%}
        {%- set to = adapter.get_relation(database=None, schema='foo', identifier='another') -%}
        {{ adapter.get_missing_columns(from, to) -}}
        """
    ) == str([Column.from_description(name="baz", raw_data_type="INT")])

    assert (
        renderer(
            "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_missing_columns(relation, relation) }}"
        )
        == "[]"
    )


@pytest.mark.cicdonly
def test_normalization(
    sushi_test_project: Project, runtime_renderer: t.Callable, mocker: MockerFixture
):
    context = sushi_test_project.context
    assert context.target

    # bla and bob will be normalized to lowercase since the target is duckdb
    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
    adapter_mock.dialect = "duckdb"

    duckdb_renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    schema_bla = schema_("bla", "test", quoted=True)
    relation_bla_bob = exp.table_("bob", db="bla", catalog="test", quoted=True)

    duckdb_renderer("{{ adapter.get_relation(database=None, schema='bla', identifier='bob') }}")
    adapter_mock.table_exists.assert_has_calls([call(relation_bla_bob)])

    # bla and bob will be normalized to uppercase since the target is Snowflake, even though the default dialect is duckdb
    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
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

    schema_bla = schema_("bla", "test", quoted=True)
    relation_bla_bob = exp.table_("bob", db="bla", catalog="test", quoted=True)

    renderer("{{ adapter.get_relation(database=None, schema='bla', identifier='bob') }}")
    adapter_mock.table_exists.assert_has_calls([call(relation_bla_bob)])

    renderer("{{ adapter.get_relation(database='custom_db', schema='bla', identifier='bob') }}")
    adapter_mock.table_exists.assert_has_calls(
        [call(exp.table_("bob", db="bla", catalog="custom_db", quoted=True))]
    )

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

    expected_star_query: exp.Select = exp.maybe_parse(
        'SELECT * FROM "t" as "t"', dialect="snowflake"
    )

    # The following call to run_query won't return dataframes and so we're expected to
    # raise in adapter.execute right before returning from the method
    with pytest.raises(AssertionError):
        renderer("{{ run_query('SELECT * FROM t') }}")
    adapter_mock.fetchdf.assert_has_calls([call(expected_star_query, quote_identifiers=False)])

    renderer("{% call statement('something') %} {{ 'SELECT * FROM t' }} {% endcall %}")
    adapter_mock.execute.assert_has_calls([call(expected_star_query, quote_identifiers=False)])

    # Enforce case-sensitivity for database object names
    setattr(
        context.target.__class__,
        "quote_policy",
        Policy(database=True, schema=True, identifier=True),
    )

    adapter_mock.drop_table.reset_mock()
    renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    # Ensures we'll pass lowercase names to the engine
    renderer(
        "{%- set relation = api.Relation.create(schema='bla', identifier='bob') -%}"
        "{{ adapter.drop_relation(relation) }}"
    )
    adapter_mock.drop_table.assert_has_calls([call(relation_bla_bob)])


def test_adapter_dispatch(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    renderer = runtime_renderer(context)
    assert renderer("{{ adapter.dispatch('current_engine', 'customers')() }}") == "duckdb"
    assert renderer("{{ adapter.dispatch('current_timestamp')() }}") == "now()"
    assert renderer("{{ adapter.dispatch('current_timestamp', 'dbt')() }}") == "now()"

    with pytest.raises(ConfigError, match=r"Macro 'current_engine'.*was not found."):
        renderer("{{ adapter.dispatch('current_engine')() }}")


@pytest.mark.parametrize("project_dialect", ["duckdb", "bigquery"])
def test_adapter_map_snapshot_tables(
    sushi_test_project: Project,
    runtime_renderer: t.Callable,
    mocker: MockerFixture,
    project_dialect: str,
):
    snapshot_mock = mocker.Mock()
    snapshot_mock.name = '"memory"."test_db"."test_model"'
    snapshot_mock.version = "1"
    snapshot_mock.is_embedded = False
    snapshot_mock.table_name.return_value = '"memory"."sqlmesh"."test_db__test_model"'
    snapshot_mock.snapshot_id = SnapshotId(
        name='"memory"."test_db"."test_model"', identifier="12345"
    )

    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(
        context,
        engine_adapter=engine_adapter,
        snapshots={snapshot_mock.snapshot_id: snapshot_mock},
        test_model=BaseRelation.create(schema="test_db", identifier="test_model"),
        foo_bar=BaseRelation.create(schema="foo", identifier="bar"),
        default_catalog="memory",
        dialect=project_dialect,
    )

    engine_adapter.create_schema("foo")
    engine_adapter.create_schema("sqlmesh")
    engine_adapter.create_table(
        table_name='"memory"."sqlmesh"."test_db__test_model"',
        columns_to_types={"baz": exp.DataType.build("int")},
    )
    engine_adapter.create_table(
        table_name="foo.bar", columns_to_types={"col": exp.DataType.build("int")}
    )

    expected_test_model_table_name = parse_one('"memory"."sqlmesh"."test_db__test_model"').sql(
        dialect=project_dialect
    )

    assert (
        renderer(
            "{{ adapter.get_relation(database=none, schema='test_db', identifier='test_model') }}"
        )
        == expected_test_model_table_name
    )

    assert "baz" in renderer("{{ run_query('SELECT * FROM test_db.test_model') }}")

    expected_foo_bar_table_name = parse_one('"memory"."foo"."bar"').sql(dialect=project_dialect)

    assert (
        renderer("{{ adapter.get_relation(database=none, schema='foo', identifier='bar') }}")
        == expected_foo_bar_table_name
    )

    assert renderer("{{ adapter.resolve_schema(test_model) }}") == "sqlmesh"
    assert renderer("{{ adapter.resolve_identifier(test_model) }}") == "test_db__test_model"

    assert renderer("{{ adapter.resolve_schema(foo_bar) }}") == "foo"
    assert renderer("{{ adapter.resolve_identifier(foo_bar) }}") == "bar"


def test_feature_flag_scd_type_2(copy_to_temp_path, caplog):
    project_root = "tests/fixtures/dbt/sushi_test"
    sushi_context = Context(paths=copy_to_temp_path(project_root))
    assert '"memory"."snapshots"."items_snapshot"' in sushi_context.models
    assert (
        "Skipping loading Snapshot (SCD Type 2) models due to the feature flag disabling this feature"
        not in caplog.text
    )
    with mock.patch.dict(
        os.environ,
        {
            "SQLMESH__FEATURE_FLAGS__DBT__SCD_TYPE_2_SUPPORT": "false",
        },
    ):
        sushi_context = Context(paths=copy_to_temp_path(project_root))
        assert '"memory"."snapshots"."items_snapshot"' not in sushi_context.models
        assert (
            "Skipping loading Snapshot (SCD Type 2) models due to the feature flag disabling this feature"
            in caplog.text
        )


def test_quote_as_configured():
    adapter = ParsetimeAdapter(
        JinjaMacroRegistry(),
        project_dialect="duckdb",
        quote_policy=Policy(schema=False, identifier=True),
    )
    adapter.quote_as_configured("foo", "identifier") == '"foo"'
    adapter.quote_as_configured("foo", "schema") == "foo"
    adapter.quote_as_configured("foo", "database") == "foo"


def test_adapter_get_relation_normalization(
    sushi_test_project: Project, runtime_renderer: t.Callable
):
    # Simulate that the quote policy is set to quote everything to make
    # sure that we normalize correctly even if quotes are applied
    with mock.patch.object(
        SnowflakeConfig,
        "quote_policy",
        Policy(identifier=True, schema=True, database=True),
    ):
        context = sushi_test_project.context
        assert context.target
        engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
        engine_adapter._default_catalog = '"memory"'
        renderer = runtime_renderer(context, engine_adapter=engine_adapter, dialect="snowflake")

        engine_adapter.create_schema('"FOO"')
        engine_adapter.create_table(
            table_name='"FOO"."BAR"', columns_to_types={"baz": exp.DataType.build("int")}
        )

        assert (
            renderer("{{ adapter.get_relation(database=None, schema='foo', identifier='bar') }}")
            == '"memory"."FOO"."BAR"'
        )

        assert (
            renderer("{{ adapter.list_relations(database=None, schema='foo') }}")
            == '[<SnowflakeRelation "memory"."FOO"."BAR">]'
        )
