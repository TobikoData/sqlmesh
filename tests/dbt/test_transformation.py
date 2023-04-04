from pathlib import Path

import agate
import pytest
from dbt.exceptions import CompilationError
from sqlglot import exp, parse_one

from sqlmesh.core.context import Context, ExecutionContext
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
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.dbt.target import DuckDbConfig
from sqlmesh.utils.errors import ConfigError, MacroEvalError


def test_model_name():
    assert ModelConfig(target_schema="foo", path="models/bar.sql").model_name == "foo.bar"
    assert (
        ModelConfig(target_schema="foo", path="models/bar.sql", alias="baz").model_name == "foo.baz"
    )
    assert (
        ModelConfig(target_schema="foo", path="models/bar.sql", schema_="baz").model_name
        == "foo_baz.bar"
    )


def test_model_kind():
    target = DuckDbConfig(schema="foo")

    assert ModelConfig(materialized=Materialization.TABLE).model_kind(target) == ModelKind(
        name=ModelKindName.FULL
    )
    assert ModelConfig(materialized=Materialization.VIEW).model_kind(target) == ModelKind(
        name=ModelKindName.VIEW
    )
    assert ModelConfig(materialized=Materialization.EPHEMERAL).model_kind(target) == ModelKind(
        name=ModelKindName.EMBEDDED
    )

    assert ModelConfig(materialized=Materialization.INCREMENTAL, time_column="foo").model_kind(
        target
    ) == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="delete+insert",
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="insert_overwrite",
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", unique_key=["bar"]
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo")

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], incremental_strategy="merge"
    ).model_kind(target) == IncrementalByUniqueKeyKind(unique_key=["bar"])

    with pytest.raises(ConfigError) as exception:
        ModelConfig(materialized=Materialization.INCREMENTAL).model_kind(target)
    with pytest.raises(ConfigError) as exception:
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            time_column="foo",
            incremental_strategy="merge",
        ).model_kind(target)
    with pytest.raises(ConfigError) as exception:
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            time_column="foo",
            incremental_strategy="append",
        ).model_kind(target)

    with pytest.raises(ConfigError) as exception:
        ModelConfig(materialized=Materialization.INCREMENTAL, unique_key=["bar"]).model_kind(target)
    with pytest.raises(ConfigError) as exception:
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="delete+insert",
        ).model_kind(target)
    with pytest.raises(ConfigError) as exception:
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="insert_overwrite",
        ).model_kind(target)
    with pytest.raises(ConfigError) as exception:
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="append",
        ).model_kind(target)


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

    context = DbtContext(project_name="Foo")
    context.target = DuckDbConfig(schema="foo")
    sqlmesh_model = model.to_sqlmesh(context)
    assert sqlmesh_model.columns_to_types == expected_column_types
    assert sqlmesh_model.column_descriptions == expected_column_descriptions


def test_seed_columns():
    seed = SeedConfig(
        model_name="foo",
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

    context = DbtContext()
    sqlmesh_seed = seed.to_sqlmesh(context)
    assert sqlmesh_seed.columns_to_types == expected_column_types
    assert sqlmesh_seed.column_descriptions == expected_column_descriptions


def test_config_containing_jinja():
    model = ModelConfig(
        **{
            "pre-hook": "GRANT INSERT ON {{ source('package', 'table') }} TO admin",
            "post-hook": "GRANT DELETE ON {{ source('package', 'table') }} TO admin",
        },
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

    context = DbtContext(project_name="Foo")
    context.target = DuckDbConfig(schema="foo")
    context.variables = {"schema": "foo", "size": "5"}
    model._dependencies.sources = set(["package.table"])
    context.sources = {"package.table": SourceConfig(schema_="raw", name="baz")}

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


@pytest.mark.parametrize("model", ["sushi.waiters", "sushi.waiter_names"])
def test_hooks(capsys, sushi_test_dbt_context: Context, model: str):
    waiters = sushi_test_dbt_context.models[model]
    execution_context = ExecutionContext(sushi_test_dbt_context.engine_adapter, {}, False)
    capsys.readouterr()

    waiters.run_pre_hooks(execution_context)
    assert "pre-hook" in capsys.readouterr().out

    waiters.run_post_hooks(execution_context)
    assert "post-hook" in capsys.readouterr().out


def test_target_jinja(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ target.name }}") == "in_memory"
    assert context.render("{{ target.schema }}") == "sushi"
    assert context.render("{{ target.type }}") == "duckdb"
    assert context.render("{{ target.profile_name }}") == "sushi"


def test_project_name_jinja(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.render("{{ project_name }}") == "sushi"


def test_schema_jinja(sushi_test_project: Project):
    model_config = ModelConfig(target_schema="sushi", sql="SELECT 1 AS one FROM {{ schema }}")
    context = sushi_test_project.context
    model_config.to_sqlmesh(context).render_query().sql() == "SELECT 1 AS one FROM sushi AS sushi"


def test_this(assert_exp_eq, sushi_test_project: Project):
    model_config = ModelConfig(alias="test", sql="SELECT 1 AS one FROM {{ this.identifier }}")
    context = sushi_test_project.context
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query().sql(), "SELECT 1 AS one FROM test AS test"
    )


def test_statement(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.render(
        "{% set test_var = 'SELECT 1' %}{% call statement('something', fetch_result=True) %} {{ test_var }} {% endcall %}{{ load_result('something').table }}"
    ) == str(agate.Table([[1]], column_names=["1"], column_types=[agate.Number()]))


def test_run_query(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.render("{{ run_query('SELECT 1 UNION ALL SELECT 2') }}") == str(
        agate.Table([[1], [2]], column_names=["1"], column_types=[agate.Number()])
    )


def test_logging(capsys, sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render('{{ log("foo") }}') == ""
    assert "foo" in capsys.readouterr().out

    assert context.render('{{ print("bar") }}') == ""
    assert "bar" in capsys.readouterr().out


def test_exceptions(capsys, sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render('{{ exceptions.warn("Warning") }}') == ""
    assert "Warning" in capsys.readouterr().out

    with pytest.raises(CompilationError, match="Error"):
        context.render('{{ exceptions.raise_compiler_error("Error") }}')


def test_modules(sushi_test_project: Project):
    context = sushi_test_project.context

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


def test_flags(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ flags.FULL_REFRESH }}") == "None"
    assert context.render("{{ flags.STORE_FAILURES }}") == "None"
    assert context.render("{{ flags.WHICH }}") == "None"


def test_relation(sushi_test_project: Project):
    context = sushi_test_project.context

    jinja = (
        "{% set relation = api.Relation.create(schema='sushi', identifier='waiters') %}"
        "{{ relation.schema }} {{ relation.identifier}}"
    )

    assert context.render(jinja) == "sushi waiters"


def test_column(sushi_test_project: Project):
    context = sushi_test_project.context

    jinja = (
        "{% set col = api.Column('foo', 'integer') %}" "{{ col.is_integer() }} {{ col.is_string()}}"
    )

    assert context.render(jinja) == "True False"


def test_quote(sushi_test_project: Project):
    context = sushi_test_project.context

    jinja = "{{ adapter.quote('foo') }} {{ adapter.quote('bar') }}"
    assert context.render(jinja) == '"foo" "bar"'


def test_as_filters(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ True | as_bool }}") == "True"
    with pytest.raises(MacroEvalError, match="Failed to convert 'invalid' into boolean."):
        context.render("{{ 'invalid' | as_bool }}")

    assert context.render("{{ 123 | as_number }}") == "123"
    with pytest.raises(MacroEvalError, match="Failed to convert 'invalid' into number."):
        context.render("{{ 'invalid' | as_number }}")

    assert context.render("{{ None | as_text }}") == ""

    assert context.render("{{ None | as_native }}") == "None"


def test_set(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ set([1, 1, 2]) }}") == "{1, 2}"
    assert context.render("{{ set(1) }}") == "None"

    assert context.render("{{ set_strict([1, 1, 2]) }}") == "{1, 2}"
    with pytest.raises(TypeError):
        assert context.render("{{ set_strict(1) }}")


def test_json(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ tojson({'key': 'value'}) }}") == """{"key": "value"}"""
    assert context.render("{{ tojson(set([1])) }}") == "None"

    assert context.render("""{{ fromjson('{"key": "value"}') }}""") == "{'key': 'value'}"
    assert context.render("""{{ fromjson('invalid') }}""") == "None"


def test_yaml(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ toyaml({'key': 'value'}) }}").strip() == "key: value"
    assert context.render("{{ toyaml(invalid) }}", invalid=lambda: "") == "None"

    assert context.render("""{{ fromyaml('key: value') }}""") == "{'key': 'value'}"


def test_zip(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ zip([1, 2], ['a', 'b']) }}") == "[(1, 'a'), (2, 'b')]"
    assert context.render("{{ zip(12, ['a', 'b']) }}") == "None"

    assert context.render("{{ zip_strict([1, 2], ['a', 'b']) }}") == "[(1, 'a'), (2, 'b')]"
    with pytest.raises(TypeError):
        context.render("{{ zip_strict(12, ['a', 'b']) }}")
