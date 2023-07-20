from __future__ import annotations

import typing as t

import pytest
from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.dbt.project import Project
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
        == '"foo"."bar"'
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
    snapshot_mock.table_name_for_mapping.return_value = "sqlmesh.test_db__test_model"

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
        == '"sqlmesh"."test_db__test_model"'
    )

    assert "baz" in renderer("{{ run_query('SELECT * FROM test_db.test_model') }}")

    assert (
        renderer("{{ adapter.get_relation(database=none, schema='foo', identifier='bar') }}")
        == '"foo"."bar"'
    )

    assert renderer("{{ adapter.resolve_schema(test_model) }}") == "sqlmesh"
    assert renderer("{{ adapter.resolve_identifier(test_model) }}") == "test_db__test_model"

    assert renderer("{{ adapter.resolve_schema(foo_bar) }}") == "foo"
    assert renderer("{{ adapter.resolve_identifier(foo_bar) }}") == "bar"
