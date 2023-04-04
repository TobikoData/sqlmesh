from __future__ import annotations

import pytest
from dbt.adapters.base.column import Column
from sqlglot import exp

from sqlmesh.dbt.project import Project
from sqlmesh.utils.errors import ConfigError


def test_adapter_relation(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.engine_adapter

    engine_adapter = context.engine_adapter
    engine_adapter.create_schema("foo")
    engine_adapter.create_schema("ignored")
    engine_adapter.create_table(
        table_name="foo.bar", query_or_columns_to_types={"baz": exp.DataType.build("int")}
    )
    engine_adapter.create_table(
        table_name="foo.another", query_or_columns_to_types={"col": exp.DataType.build("int")}
    )
    engine_adapter.create_table(
        table_name="ignored.ignore", query_or_columns_to_types={"col": exp.DataType.build("int")}
    )

    assert (
        context.render("{{ adapter.get_relation(database=None, schema='foo', identifier='bar') }}")
        == '"foo"."bar"'
    )
    assert context.render(
        "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_columns_in_relation(relation) }}"
    ) == str([Column.from_description(name="baz", raw_data_type="INTEGER")])

    assert context.render("{{ adapter.list_relations(database=None, schema='foo')|length }}") == "2"

    assert (
        context.render(
            """
        {%- set from = adapter.get_relation(database=None, schema='foo', identifier='bar') -%}
        {%- set to = adapter.get_relation(database=None, schema='foo', identifier='another') -%}
        {{ adapter.get_missing_columns(from, to) -}}
        """
        )
        == str([Column.from_description(name="baz", raw_data_type="INTEGER")])
    )

    assert (
        context.render(
            "{%- set relation = adapter.get_relation(database=None, schema='foo', identifier='bar') -%} {{ adapter.get_missing_columns(relation, relation) }}"
        )
        == "[]"
    )


def test_adapter_dispatch(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.render("{{ adapter.dispatch('current_engine', 'customers')() }}") == "duckdb"

    assert context.engine_adapter
    context.engine_adapter.dialect = "unknown"
    context._jinja_environment = None
    assert context.render("{{ adapter.dispatch('current_engine', 'customers')() }}") == "default"

    with pytest.raises(ConfigError, match=r"Macro 'current_engine'.*was not found."):
        context.render("{{ adapter.dispatch('current_engine')() }}")
