from __future__ import annotations

import typing as t
from unittest import mock
from unittest.mock import call

import pytest
from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.dialect import schema_
from sqlmesh.core.snapshot import SnapshotId
from sqlmesh.dbt.adapter import ParsetimeAdapter
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.relation import Policy
from sqlmesh.dbt.target import BigQueryConfig, SnowflakeConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.core.schema_diff import SchemaDiffer, TableAlterChangeColumnTypeOperation

pytestmark = pytest.mark.dbt


@pytest.mark.slow
def test_adapter_relation(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)

    engine_adapter.create_schema("foo")
    engine_adapter.create_schema("ignored")
    engine_adapter.create_table(
        table_name="foo.bar", target_columns_to_types={"baz": exp.DataType.build("int")}
    )
    engine_adapter.create_table(
        table_name="foo.another", target_columns_to_types={"col": exp.DataType.build("int")}
    )
    engine_adapter.create_view(
        view_name="foo.bar_view", query_or_df=t.cast(exp.Query, parse_one("select * from foo.bar"))
    )
    engine_adapter.create_table(
        table_name="ignored.ignore", target_columns_to_types={"col": exp.DataType.build("int")}
    )

    assert (
        renderer("{{ adapter.get_relation(database=None, schema='foo', identifier='bar') }}")
        == '"memory"."foo"."bar"'
    )

    assert (
        renderer("{{ adapter.get_relation(database=None, schema='foo', identifier='bar').type }}")
        == "table"
    )

    assert (
        renderer(
            "{{ adapter.get_relation(database=None, schema='foo', identifier='bar_view').type }}"
        )
        == "view"
    )

    assert renderer(
        "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_columns_in_relation(relation) }}"
    ) == str([Column.from_description(name="baz", raw_data_type="INT")])

    assert renderer("{{ adapter.list_relations(database=None, schema='foo')|length }}") == "3"

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

    renderer("""
        {%- set old_relation = adapter.get_relation(
              database=None,
              schema='foo',
              identifier='bar') -%}

        {%- set backup_relation = api.Relation.create(schema='foo', identifier='bar__backup') -%}

        {% do adapter.rename_relation(old_relation, backup_relation) %}
    """)
    assert not engine_adapter.table_exists("foo.bar")
    assert engine_adapter.table_exists("foo.bar__backup")


@pytest.mark.slow
def test_bigquery_get_columns_in_relation(
    sushi_test_project: Project,
    runtime_renderer: t.Callable,
    mocker: MockerFixture,
):
    from dbt.adapters.bigquery import BigQueryColumn
    from google.cloud.bigquery import SchemaField

    context = sushi_test_project.context
    context.target = BigQueryConfig(name="test", schema="test", database="test")

    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
    adapter_mock.dialect = "bigquery"
    table_schema = [
        SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
        SchemaField(
            name="user_data",
            field_type="RECORD",
            mode="NULLABLE",
            fields=[
                SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
                SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
                SchemaField(name="address", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
        SchemaField(name="score", field_type="NUMERIC", mode="NULLABLE"),
        SchemaField(name="created_at", field_type="TIMESTAMP", mode="NULLABLE"),
    ]
    adapter_mock.get_bq_schema.return_value = table_schema
    renderer = runtime_renderer(context, engine_adapter=adapter_mock, dialect="bigquery")
    assert renderer(
        "{%- set relation = api.Relation.create(database='test', schema='test', identifier='test_table') -%}"
        "{{ adapter.get_columns_in_relation(relation) }}"
    ) == str([BigQueryColumn.create_from_field(field) for field in table_schema])


@pytest.mark.cicdonly
@pytest.mark.slow
def test_normalization(
    sushi_test_project: Project, runtime_renderer: t.Callable, mocker: MockerFixture
):
    from sqlmesh.core.engine_adapter.base import DataObject, DataObjectType

    context = sushi_test_project.context
    assert context.target
    data_object = DataObject(catalog="test", schema="bla", name="bob", type=DataObjectType.TABLE)

    # bla and bob will be normalized to lowercase since the target is duckdb
    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
    adapter_mock.dialect = "duckdb"
    adapter_mock.get_data_object.return_value = data_object
    duckdb_renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    schema_bla = schema_("bla", "test", quoted=True)
    relation_bla_bob = exp.table_("bob", db="bla", catalog="test", quoted=True)

    duckdb_renderer("{{ adapter.get_relation(database=None, schema='bla', identifier='bob') }}")
    adapter_mock.get_data_object.assert_has_calls([call(relation_bla_bob)])

    # bla and bob will be normalized to uppercase since the target is Snowflake, even though the default dialect is duckdb
    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
    adapter_mock.dialect = "snowflake"
    adapter_mock.get_data_object.return_value = data_object
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
    adapter_mock.get_data_object.assert_has_calls([call(relation_bla_bob)])

    renderer("{{ adapter.get_relation(database='custom_db', schema='bla', identifier='bob') }}")
    adapter_mock.get_data_object.assert_has_calls(
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


@pytest.mark.slow
def test_adapter_dispatch(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    renderer = runtime_renderer(context)
    assert renderer("{{ adapter.dispatch('current_engine', 'customers')() }}") == "duckdb"
    assert renderer("{{ adapter.dispatch('current_timestamp')() }}") == "now()"
    assert renderer("{{ adapter.dispatch('current_timestamp', 'dbt')() }}") == "now()"
    assert renderer("{{ adapter.dispatch('select_distinct', 'customers')() }}") == "distinct"

    # test with keyword arguments
    assert (
        renderer(
            "{{ adapter.dispatch(macro_name='current_engine', macro_namespace='customers')() }}"
        )
        == "duckdb"
    )
    assert renderer("{{ adapter.dispatch(macro_name='current_timestamp')() }}") == "now()"
    assert (
        renderer("{{ adapter.dispatch(macro_name='current_timestamp', macro_namespace='dbt')() }}")
        == "now()"
    )

    # mixing positional and keyword arguments
    assert (
        renderer("{{ adapter.dispatch('current_engine', macro_namespace='customers')() }}")
        == "duckdb"
    )
    assert (
        renderer("{{ adapter.dispatch('current_timestamp', macro_namespace=None)() }}") == "now()"
    )
    assert (
        renderer("{{ adapter.dispatch('current_timestamp', macro_namespace='dbt')() }}") == "now()"
    )

    with pytest.raises(ConfigError, match=r"Macro 'current_engine'.*was not found."):
        renderer("{{ adapter.dispatch(macro_name='current_engine')() }}")

    with pytest.raises(ConfigError, match=r"Macro 'current_engine'.*was not found."):
        renderer("{{ adapter.dispatch('current_engine')() }}")


@pytest.mark.parametrize("project_dialect", ["duckdb", "bigquery"])
@pytest.mark.slow
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
        target_columns_to_types={"baz": exp.DataType.build("int")},
    )
    engine_adapter.create_table(
        table_name="foo.bar", target_columns_to_types={"col": exp.DataType.build("int")}
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


def test_quote_as_configured():
    adapter = ParsetimeAdapter(
        JinjaMacroRegistry(),
        project_dialect="duckdb",
        quote_policy=Policy(schema=False, identifier=True),
    )
    adapter.quote_as_configured("foo", "identifier") == '"foo"'
    adapter.quote_as_configured("foo", "schema") == "foo"
    adapter.quote_as_configured("foo", "database") == "foo"


@pytest.mark.slow
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
            table_name='"FOO"."BAR"', target_columns_to_types={"baz": exp.DataType.build("int")}
        )

        assert (
            renderer("{{ adapter.get_relation(database=None, schema='foo', identifier='bar') }}")
            == '"memory"."FOO"."BAR"'
        )

        assert (
            renderer("{{ adapter.list_relations(database=None, schema='foo') }}")
            == '[<SnowflakeRelation "memory"."FOO"."BAR">]'
        )


@pytest.mark.slow
def test_adapter_expand_target_column_types(
    sushi_test_project: Project, runtime_renderer: t.Callable, mocker: MockerFixture
):
    from sqlmesh.core.engine_adapter.base import DataObject, DataObjectType

    data_object_from = DataObject(
        catalog="test", schema="foo", name="from_table", type=DataObjectType.TABLE
    )
    data_object_to = DataObject(
        catalog="test", schema="foo", name="to_table", type=DataObjectType.TABLE
    )
    from_columns = {
        "int_col": exp.DataType.build("int"),
        "same_text_col": exp.DataType.build("varchar(1)"),  # varchar(1) -> varchar(1)
        "unexpandable_text_col": exp.DataType.build("varchar(2)"),  # varchar(4) -> varchar(2)
        "expandable_text_col1": exp.DataType.build("varchar(16)"),  # varchar(8) -> varchar(16)
        "expandable_text_col2": exp.DataType.build("varchar(64)"),  # varchar(32) -> varchar(64)
    }
    to_columns = {
        "int_col": exp.DataType.build("int"),
        "same_text_col": exp.DataType.build("varchar(1)"),
        "unexpandable_text_col": exp.DataType.build("varchar(4)"),
        "expandable_text_col1": exp.DataType.build("varchar(8)"),
        "expandable_text_col2": exp.DataType.build("varchar(32)"),
    }
    adapter_mock = mocker.MagicMock()
    adapter_mock.default_catalog = "test"
    adapter_mock.get_data_object.side_effect = [data_object_from, data_object_to]
    # columns() is called 4 times, twice by adapter.get_columns_in_relation() and twice by the engine_adapter
    adapter_mock.columns.side_effect = [
        from_columns,
        to_columns,
        from_columns,
        to_columns,
    ]
    adapter_mock.schema_differ = SchemaDiffer()

    context = sushi_test_project.context
    renderer = runtime_renderer(context, engine_adapter=adapter_mock)

    renderer("""
        {%- set from_relation = adapter.get_relation(
              database=None,
              schema='foo',
              identifier='from_table') -%}

        {% set to_relation = adapter.get_relation(
              database=None,
              schema='foo',
              identifier='to_table') -%}

        {% do adapter.expand_target_column_types(from_relation, to_relation) %}
    """)
    adapter_mock.get_data_object.assert_has_calls(
        [
            call(exp.to_table('"test"."foo"."from_table"')),
            call(exp.to_table('"test"."foo"."to_table"')),
        ]
    )
    assert len(adapter_mock.alter_table.call_args.args) == 1
    alter_expressions = adapter_mock.alter_table.call_args.args[0]
    assert len(alter_expressions) == 2
    alter_operation1 = alter_expressions[0]
    assert isinstance(alter_operation1, TableAlterChangeColumnTypeOperation)
    assert alter_operation1.expression == parse_one(
        """ALTER TABLE "test"."foo"."to_table"
           ALTER COLUMN expandable_text_col1
           SET DATA TYPE VARCHAR(16)"""
    )
    alter_operation2 = alter_expressions[1]
    assert isinstance(alter_operation2, TableAlterChangeColumnTypeOperation)
    assert alter_operation2.expression == parse_one(
        """ALTER TABLE "test"."foo"."to_table"
           ALTER COLUMN expandable_text_col2
           SET DATA TYPE VARCHAR(64)"""
    )
