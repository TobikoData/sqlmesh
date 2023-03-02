from pathlib import Path

import agate
import pytest
from dbt.exceptions import CompilationError
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    ModelKind,
    ModelKindName,
)
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
)
from sqlmesh.dbt.common import DbtContext
from sqlmesh.dbt.model import Materialization, ModelConfig
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.utils.errors import ConfigError


@pytest.fixture()
def sushi_dbt_project(mocker: MockerFixture) -> Project:
    return Project.load(DbtContext(project_root=Path("examples/sushi_dbt")))


def test_model_name():
    assert ModelConfig(target_schema="foo", table_name="bar").model_name == "foo.bar"
    assert ModelConfig(target_schema="foo", table_name="bar", alias="baz").model_name == "foo.baz"
    assert (
        ModelConfig(target_schema="foo", table_name="bar", schema_="baz").model_name
        == "foo_baz.bar"
    )


def test_model_kind():
    assert ModelConfig(materialized=Materialization.TABLE).model_kind == ModelKind(
        name=ModelKindName.FULL
    )
    assert ModelConfig(materialized=Materialization.VIEW).model_kind == ModelKind(
        name=ModelKindName.VIEW
    )
    assert ModelConfig(materialized=Materialization.EPHEMERAL).model_kind == ModelKind(
        name=ModelKindName.EMBEDDED
    )
    with pytest.raises(ConfigError) as exception:
        ModelConfig(materialized=Materialization.INCREMENTAL).model_kind
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo"
    ).model_kind == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", unique_key=["bar"]
    ).model_kind == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"]
    ).model_kind == IncrementalByUniqueKeyKind(unique_key=["bar"])


def test_model_columns():
    model = ModelConfig(
        target_schema="foo",
        table_name="bar",
        sql="SELECT * FROM baz",
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
        },
    )

    expected_column_types = {
        "address": parse_one("text", into=exp.DataType),
        "zipcode": parse_one("varchar(5)", into=exp.DataType),
    }
    expected_column_descriptions = {
        "address": "Business address",
        "zipcode": "Business zipcode",
    }

    assert column_types_to_sqlmesh(model.columns) == expected_column_types
    assert column_descriptions_to_sqlmesh(model.columns) == expected_column_descriptions

    context = DbtContext()
    sqlmesh_model = model.to_sqlmesh(context)
    assert sqlmesh_model.columns_to_types == expected_column_types
    assert sqlmesh_model.column_descriptions == expected_column_descriptions


def test_seed_columns():
    seed = SeedConfig(
        seed_name="foo",
        path=Path("examples/sushi_dbt/seeds/waiter_names.csv"),
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
        },
    )

    expected_column_types = {
        "address": parse_one("text", into=exp.DataType),
        "zipcode": parse_one("varchar(5)", into=exp.DataType),
    }
    expected_column_descriptions = {
        "address": "Business address",
        "zipcode": "Business zipcode",
    }

    sqlmesh_seed = seed.to_sqlmesh()
    assert sqlmesh_seed.columns_to_types == expected_column_types
    assert sqlmesh_seed.column_descriptions == expected_column_descriptions


def test_config_containing_jinja():
    model = ModelConfig(
        **{"pre-hook": "GRANT INSERT ON {{ source('package', 'table') }}"},
        target_schema="{{ var('schema') }}",
        table_name="bar",
        sql="SELECT * FROM {{ source('package', 'table') }}",
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode",
                data_type="varchar({{ var('size') }})",
                description="Business zipcode",
            ),
        },
    )

    context = DbtContext()
    context.variables = {"schema": "foo", "size": "5"}
    model._dependencies.sources = set(["package.table"])
    context.sources = {"package.table": "raw.baz"}

    rendered = model.render_config(context)
    assert rendered.pre_hook == model.pre_hook
    assert rendered.sql == model.sql
    assert rendered.target_schema != model.target_schema
    assert rendered.target_schema == "foo"
    assert rendered.columns["zipcode"] != model.columns["zipcode"]
    assert rendered.columns["zipcode"].data_type == "varchar(5)"

    sqlmesh_model = rendered.to_sqlmesh(context)
    assert str(sqlmesh_model.query) == model.sql
    assert str(sqlmesh_model.render_query()) == "SELECT * FROM raw.baz AS baz"
    assert sqlmesh_model.columns_to_types == column_types_to_sqlmesh(rendered.columns)


def test_target_jinja(sushi_dbt_project: Project):
    context = sushi_dbt_project.context

    assert context.render("{{ target.name }}") == "in_memory"
    assert context.render("{{ target.schema }}") == "sushi"
    assert context.render("{{ target.type }}") == "duckdb"
    assert context.render("{{ target.profile_name }}") == "sushi"


def test_statement(sushi_dbt_project: Project):
    context = sushi_dbt_project.context
    assert context.render(
        "{% set test_var = 'SELECT 1' %}{% call statement('something', fetch_result=True) %} {{ test_var }} {% endcall %}{{ load_result('something').table }}"
    ) == str(agate.Table([[1]], column_names=["1"], column_types=[agate.Number()]))


def test_run_query(sushi_dbt_project: Project):
    context = sushi_dbt_project.context
    assert context.render("{{ run_query('SELECT 1 UNION ALL SELECT 2') }}") == str(
        agate.Table([[1], [2]], column_names=["1"], column_types=[agate.Number()])
    )


def test_exceptions(capsys, sushi_dbt_project: Project):
    context = sushi_dbt_project.context

    assert context.render('{{ exceptions.warn("Warning") }}') == ""
    assert "Warning" in capsys.readouterr().out

    with pytest.raises(CompilationError, match="Error"):
        context.render('{{ exceptions.raise_compiler_error("Error") }}')


def test_modules(sushi_dbt_project: Project):
    context = sushi_dbt_project.context

    # datetime
    assert context.render("{{ modules.datetime.date(2022, 12, 25) }}") == "2022-12-25"

    # pytz
    try:
        assert "UTC" in context.render("{{ modules.pytz.all_timezones }}")
    except AttributeError as error:
        assert "object has no attribute 'pytz'" in str(error)

    # re
    assert context.render("{{ modules.re.search('(?<=abc)def', 'abcdef').group(0) }}") == "def"

    # itertools
    itertools_jinja = (
        "{% for num in modules.itertools.accumulate([5]) %}" "{{ num }}" "{% endfor %}"
    )
    assert context.render(itertools_jinja) == "5"


def test_relation(sushi_dbt_project: Project):
    context = sushi_dbt_project.context

    jinja = (
        "{% set relation = api.Relation.create(schema='sushi', identifier='waiters') %}"
        "{{ relation.schema }} {{ relation.identifier}}"
    )

    assert context.render(jinja) == "sushi waiters"


def test_column(sushi_dbt_project: Project):
    context = sushi_dbt_project.context

    jinja = (
        "{% set col = api.Column('foo', 'integer') %}" "{{ col.is_integer() }} {{ col.is_string()}}"
    )

    assert context.render(jinja) == "True False"
