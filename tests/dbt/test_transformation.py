import typing as t
from pathlib import Path

import agate
import pytest
from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import Policy
from dbt.exceptions import CompilationError
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.model import (
    EmbeddedKind,
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    IncrementalUnmanagedKind,
    SqlModel,
    ViewKind,
)
from sqlmesh.dbt.builtin import _relation_info_to_relation
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
)
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import Materialization, ModelConfig
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.target import BigQueryConfig, DuckDbConfig, SnowflakeConfig
from sqlmesh.utils.errors import ConfigError, MacroEvalError, SQLMeshError


def test_model_name():
    assert ModelConfig(schema="foo", path="models/bar.sql").sql_name == "foo.bar"
    assert ModelConfig(schema="foo", path="models/bar.sql", alias="baz").sql_name == "foo.baz"


def test_model_kind():
    target = DuckDbConfig(name="target", schema="foo")

    assert ModelConfig(materialized=Materialization.TABLE).model_kind(target) == FullKind()
    assert ModelConfig(materialized=Materialization.VIEW).model_kind(target) == ViewKind()
    assert ModelConfig(materialized=Materialization.EPHEMERAL).model_kind(target) == EmbeddedKind()

    assert ModelConfig(materialized=Materialization.INCREMENTAL, time_column="foo").model_kind(
        target
    ) == IncrementalByTimeRangeKind(time_column="foo", forward_only=True)
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="delete+insert",
        forward_only=False,
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="insert_overwrite",
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo", forward_only=True)
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", unique_key=["bar"]
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo", forward_only=True)

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], incremental_strategy="merge"
    ).model_kind(target) == IncrementalByUniqueKeyKind(unique_key=["bar"], forward_only=True)
    assert ModelConfig(materialized=Materialization.INCREMENTAL, unique_key=["bar"]).model_kind(
        target
    ) == IncrementalByUniqueKeyKind(unique_key=["bar"], forward_only=True)

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", incremental_strategy="merge"
    ).model_kind(target) == IncrementalByTimeRangeKind(
        time_column="foo", forward_only=True, disable_restatement=False
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="append",
        disable_restatement=True,
    ).model_kind(target) == IncrementalByTimeRangeKind(
        time_column="foo", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar"},
        forward_only=False,
    ).model_kind(target) == IncrementalByTimeRangeKind(time_column="foo")

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar"},
    ).model_kind(target) == IncrementalUnmanagedKind(insert_overwrite=True)

    assert ModelConfig(materialized=Materialization.INCREMENTAL).model_kind(
        target
    ) == IncrementalUnmanagedKind(insert_overwrite=True)

    assert (
        ModelConfig(
            materialized=Materialization.INCREMENTAL, incremental_strategy="append"
        ).model_kind(target)
        == IncrementalUnmanagedKind()
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar", "data_type": "int64"},
    ).model_kind(target) == IncrementalUnmanagedKind(insert_overwrite=True)

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
        alias="test",
        target_schema="foo",
        table_name="bar",
        sql="SELECT * FROM baz",
        columns={
            "ADDRESS": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "ZIPCODE": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
            "DATE": ColumnConfig(
                name="date", data_type="timestamp_ntz", description="Contract date"
            ),
        },
    )

    expected_column_types = {
        "ADDRESS": parse_one("text", into=exp.DataType),
        "ZIPCODE": parse_one("varchar(5)", into=exp.DataType),
        "DATE": parse_one("timestamp_ntz", into=exp.DataType, dialect="snowflake"),
    }
    expected_column_descriptions = {
        "ADDRESS": "Business address",
        "ZIPCODE": "Business zipcode",
        "DATE": "Contract date",
    }

    assert column_types_to_sqlmesh(model.columns, "snowflake") == expected_column_types
    assert column_descriptions_to_sqlmesh(model.columns) == expected_column_descriptions

    context = DbtContext()
    context.project_name = "Foo"
    context.target = SnowflakeConfig(
        name="target", schema="test", database="test", account="foo", user="bar", password="baz"
    )
    sqlmesh_model = model.to_sqlmesh(context)
    assert sqlmesh_model.columns_to_types == expected_column_types
    assert sqlmesh_model.column_descriptions == expected_column_descriptions


def test_seed_columns():
    seed = SeedConfig(
        name="foo",
        package="package",
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
    context.project_name = "Foo"
    context.target = DuckDbConfig(name="target", schema="test")
    sqlmesh_seed = seed.to_sqlmesh(context)
    assert sqlmesh_seed.columns_to_types == expected_column_types
    assert sqlmesh_seed.column_descriptions == expected_column_descriptions


@pytest.mark.parametrize("model", ["sushi.waiters", "sushi.waiter_names"])
def test_hooks(capsys, sushi_test_dbt_context: Context, model: str):
    engine_adapter = sushi_test_dbt_context.engine_adapter
    waiters = sushi_test_dbt_context.models[model]
    capsys.readouterr()

    engine_adapter.execute(
        waiters.render_pre_statements(engine_adapter=engine_adapter, execution_time="2023-01-01")
    )
    assert "pre-hook" in capsys.readouterr().out

    engine_adapter.execute(
        waiters.render_post_statements(
            engine_adapter=sushi_test_dbt_context.engine_adapter, execution_time="2023-01-01"
        )
    )
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


def test_schema_jinja(sushi_test_project: Project, assert_exp_eq):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        sql="SELECT 1 AS one FROM {{ schema }}",
    )
    context = sushi_test_project.context
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "sushi" AS "sushi"',
    )


def test_config_jinja(sushi_test_project: Project):
    hook = "{{ config(alias='bar') }} {{ config.alias }}"
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        sql="""SELECT 1 AS one FROM foo""",
        **{"pre-hook": hook},
    )
    context = sushi_test_project.context
    model = t.cast(SqlModel, model_config.to_sqlmesh(context))
    assert hook in model.pre_statements[0].sql()
    assert model.render_pre_statements()[0].sql() == '"bar"'


def test_this(assert_exp_eq, sushi_test_project: Project):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        alias="test",
        sql="SELECT 1 AS one FROM {{ this.identifier }}",
    )
    context = sushi_test_project.context
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


def test_statement(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)
    assert renderer(
        "{% set test_var = 'SELECT 1' %}{% call statement('something', fetch_result=True) %} {{ test_var }} {% endcall %}{{ load_result('something').table }}",
    ) == str(agate.Table([[1]], column_names=["1"], column_types=[agate.Number()]))


def test_run_query(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)
    assert renderer("{{ run_query('SELECT 1 UNION ALL SELECT 2') }}") == str(
        agate.Table([[1], [2]], column_names=["1"], column_types=[agate.Number()])
    )


def test_logging(capsys, sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)

    assert renderer('{{ log("foo") }}') == ""
    assert "foo" in capsys.readouterr().out

    assert renderer('{{ print("bar") }}') == ""
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

    assert (
        context.render("{{ api.Relation }}")
        == "<class 'dbt.adapters.duckdb.relation.DuckDBRelation'>"
    )

    jinja = (
        "{% set relation = api.Relation.create(schema='sushi', identifier='waiters') %}"
        "{{ relation.schema }} {{ relation.identifier}}"
    )

    assert context.render(jinja) == "sushi waiters"


def test_column(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ api.Column }}") == "<class 'dbt.adapters.base.column.Column'>"

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


def test_dbt_version(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ dbt_version }}").startswith("1.")


def test_parsetime_adapter_call(
    assert_exp_eq, sushi_test_project: Project, sushi_test_dbt_context: Context
):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        alias="test",
        sql="""
            {% set results = run_query('select 1 as one') %}
            SELECT {{ results.columns[0].values()[0] }} AS one FROM {{ this.identifier }}
        """,
    )
    context = sushi_test_project.context

    sqlmesh_model = model_config.to_sqlmesh(context)
    assert sqlmesh_model.render_query() is None
    assert sqlmesh_model.columns_to_types is None
    assert not sqlmesh_model.annotated
    with pytest.raises(SQLMeshError):
        sqlmesh_model.ctas_query()

    engine_adapter = sushi_test_dbt_context.engine_adapter
    assert_exp_eq(
        sqlmesh_model.render_query_or_raise(engine_adapter=engine_adapter).sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


def test_partition_by(sushi_test_project: Project):
    context = sushi_test_project.context
    context.target = BigQueryConfig(name="production", database="main", schema="sushi")
    model_config = ModelConfig(
        name="model",
        schema="test",
        package_name="package",
        materialized="table",
        unique_key="ds",
        partition_by={"field": "ds", "granularity": "month"},
        sql="""SELECT 1 AS one, ds FROM foo""",
    )
    date_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert date_trunc_expr.sql(dialect="bigquery") == "DATE_TRUNC(ds, MONTH)"
    assert date_trunc_expr.sql() == "DATE_TRUNC('MONTH', ds)"

    model_config.partition_by = {"field": "`ds`", "data_type": "datetime", "granularity": "day"}
    datetime_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert datetime_trunc_expr.sql(dialect="bigquery") == "datetime_trunc(`ds`, DAY)"
    assert datetime_trunc_expr.sql() == 'DATETIME_TRUNC("ds", DAY)'

    model_config.partition_by = {"field": "ds", "data_type": "timestamp", "granularity": "day"}
    timestamp_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert timestamp_trunc_expr.sql(dialect="bigquery") == "timestamp_trunc(ds, DAY)"
    assert timestamp_trunc_expr.sql() == "TIMESTAMP_TRUNC(ds, DAY)"

    model_config.partition_by = {
        "field": "one",
        "data_type": "int64",
        "range": {"start": 0, "end": 10, "interval": 2},
    }
    assert (
        model_config.to_sqlmesh(context).partitioned_by[0].sql()
        == "RANGE_BUCKET(one, GENERATE_SERIES(0, 10, 2))"
    )

    model_config.partition_by = {"field": "ds", "data_type": "date", "granularity": "day"}
    assert model_config.to_sqlmesh(context).partitioned_by == [exp.to_column("ds")]


def test_relation_info_to_relation():
    assert _relation_info_to_relation(
        {"quote_policy": {}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=True, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": None, "schema": None, "identifier": None}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=True, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False, "schema": None, "identifier": None}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False, "schema": False, "identifier": False}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=False, identifier=False)


def test_is_incremental(sushi_test_project: Project, assert_exp_eq):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        sql="""
        SELECT 1 AS one FROM tbl_a
        {% if is_incremental() %}
        WHERE ds > (SELECT MAX(ds) FROM model)
        {% endif %}
        """,
    )
    context = sushi_test_project.context

    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "tbl_a" AS "tbl_a"',
    )

    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise(has_intervals=True).sql(),
        'SELECT 1 AS "one" FROM "tbl_a" AS "tbl_a" WHERE "ds" > (SELECT MAX("ds") FROM "model" AS "model")',
    )


def test_dbt_max_partition(sushi_test_project: Project, assert_exp_eq, mocker: MockerFixture):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        partition_by={"field": "`ds`", "data_type": "datetime", "granularity": "month"},
        materialized=Materialization.INCREMENTAL,
        sql="""
        SELECT 1 AS one FROM tbl_a
        {% if is_incremental() %}
        WHERE ds > _dbt_max_partition
        {% endif %}
        """,
    )
    context = sushi_test_project.context
    context.target = BigQueryConfig(
        name="test_target", schema="test_schema", database="test-project"
    )

    assert (
        model_config.to_sqlmesh(context).pre_statements[-1].sql().strip()  # type: ignore
        == """
JINJA_STATEMENT_BEGIN;
{% if is_incremental() %}
  DECLARE _dbt_max_partition DATETIME DEFAULT (
    SELECT MAX(PARSE_DATETIME('%Y%m', partition_id)) FROM `{{ target.database }}`.`{{ adapter.resolve_schema(this) }}`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = '{{ adapter.resolve_identifier(this) }}' AND NOT partition_id IS NULL AND partition_id <> '__NULL__'
  );
{% endif %}
JINJA_END;""".strip()
    )


def test_bigquery_table_properties(sushi_test_project: Project, mocker: MockerFixture):
    context = sushi_test_project.context
    context.target = BigQueryConfig(
        name="test_target", schema="test_schema", database="test-project"
    )

    base_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        partition_by={"field": "`ds`", "data_type": "datetime", "granularity": "month"},
        materialized=Materialization.INCREMENTAL,
        sql="SELECT 1 AS one FROM tbl_a",
    )

    assert base_config.to_sqlmesh(context).table_properties == {}

    assert base_config.copy(
        update={"require_partition_filter": True, "partition_expiration_days": 7}
    ).to_sqlmesh(context).table_properties == {
        "require_partition_filter": exp.convert(True),
        "partition_expiration_days": exp.convert(7),
    }

    assert base_config.copy(update={"require_partition_filter": True}).to_sqlmesh(
        context
    ).table_properties == {
        "require_partition_filter": exp.convert(True),
    }

    assert base_config.copy(update={"partition_expiration_days": 7}).to_sqlmesh(
        context
    ).table_properties == {
        "partition_expiration_days": exp.convert(7),
    }
