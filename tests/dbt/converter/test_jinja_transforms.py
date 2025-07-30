import pytest
import typing as t
from sqlglot import parse_one
from sqlmesh.core.model import create_sql_model, create_external_model
from sqlmesh.dbt.converter.jinja import transform, JinjaGenerator
import sqlmesh.dbt.converter.jinja_transforms as jt
from sqlmesh.dbt.converter.common import JinjaTransform
from sqlmesh.utils.jinja import environment, Environment, ENVIRONMENT
from sqlmesh.core.context import Context
from sqlmesh.core.config import Config, ModelDefaultsConfig


def transform_str(
    input: str, handler: JinjaTransform, environment: t.Optional[Environment] = None
) -> str:
    environment = environment or ENVIRONMENT
    ast = environment.parse(input)
    return JinjaGenerator().generate(transform(ast, handler))


@pytest.mark.parametrize(
    "input,expected",
    [
        ("select * from {{ ref('bar') }} as t", "select * from foo.bar as t"),
        ("select * from {{ ref('bar', version=1) }} as t", "select * from foo.bar_v1 as t"),
        ("select * from {{ ref('bar', v=1) }} as t", "select * from foo.bar_v1 as t"),
        (
            "select * from {{ ref('unknown') }} as t",
            "select * from __unresolved_ref__.unknown as t",
        ),
        (
            "{% macro foo() %}select * from {{ ref('bar') }}{% endmacro %}",
            "{% macro foo() %}select * from foo.bar{% endmacro %}",
        ),
        # these shouldnt be transformed as the macro call might rely on some property of the Relation object returned by ref()
        ("{{ dbt_utils.union_relations([ref('foo')]) }},", None),
        ("select * from {% if some_macro(ref('bar')) %}foo{% endif %}", None),
        (
            "select * from {% if some_macro(ref('bar')) %}{{ ref('bar') }}{% endif %}",
            "select * from {% if some_macro(ref('bar')) %}foo.bar{% endif %}",
        ),
        ("{{ some_macro(ref('bar')) }}", None),
        ("{{ some_macro(table=ref('bar')) }}", None),
    ],
)
def test_resolve_dbt_ref_to_model_name(input: str, expected: t.Optional[str]) -> None:
    expected = expected or input

    from dbt.adapters.base import BaseRelation

    # note: bigquery dialect chosen because its identifiers have backticks
    # but internally SQLMesh stores model fqn with double quotes
    config = Config(model_defaults=ModelDefaultsConfig(dialect="bigquery"))
    ctx = Context(config=config)
    ctx.default_catalog = "sqlmesh"

    assert ctx.default_catalog == "sqlmesh"
    assert ctx.default_dialect == "bigquery"

    model = create_sql_model(
        name="foo.bar", query=parse_one("select 1"), default_catalog=ctx.default_catalog
    )
    model2 = create_sql_model(
        name="foo.bar_v1", query=parse_one("select 1"), default_catalog=ctx.default_catalog
    )
    ctx.upsert_model(model)
    ctx.upsert_model(model2)

    assert '"sqlmesh"."foo"."bar"' in ctx.models

    def _resolve_ref(ref_name: str, version: t.Optional[int] = None) -> t.Optional[BaseRelation]:
        if ref_name == "bar":
            identifier = "bar"
            if version:
                identifier = f"bar_v{version}"

            relation = BaseRelation.create(
                database="sqlmesh", schema="foo", identifier=identifier, quote_character="`"
            )
            assert (
                relation.render() == "`sqlmesh`.`foo`.`bar`"
                if not version
                else f"`sqlmesh`.`foo`.`bar_v{version}`"
            )
            return relation
        return None

    jinja_env = environment()
    jinja_env.globals["ref"] = _resolve_ref

    assert (
        transform_str(
            input,
            jt.resolve_dbt_ref_to_model_name(ctx.models, jinja_env, dialect=ctx.default_dialect),
        )
        == expected
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            "select * from {{ ref('bar') }} as t",
            "select * from {{ __migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar') }} as t",
        ),
        (
            "{% macro foo() %}select * from {{ ref('bar') }}{% endmacro %}",
            "{% macro foo() %}select * from {{ __migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar') }}{% endmacro %}",
        ),
        (
            "{{ dbt_utils.union_relations([ref('bar')]) }}",
            "{{ dbt_utils.union_relations([__migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar')]) }}",
        ),
        (
            "select * from {% if some_macro(ref('bar')) %}foo{% endif %}",
            "select * from {% if some_macro(__migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar')) %}foo{% endif %}",
        ),
        (
            "select * from {% if some_macro(ref('bar')) %}{{ ref('bar') }}{% endif %}",
            "select * from {% if some_macro(__migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar')) %}{{ __migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar') }}{% endif %}",
        ),
        (
            "{{ some_macro(ref('bar')) }}",
            "{{ some_macro(__migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar')) }}",
        ),
        (
            "{{ some_macro(table=ref('bar')) }}",
            "{{ some_macro(table=__migrated_ref(database='sqlmesh', schema='foo', identifier='bar', sqlmesh_model_name='foo.bar')) }}",
        ),
    ],
)
def test_rewrite_dbt_ref_to_migrated_ref(input: str, expected: t.Optional[str]) -> None:
    expected = expected or input

    from dbt.adapters.base import BaseRelation

    # note: bigquery dialect chosen because its identifiers have backticks
    # but internally SQLMesh stores model fqn with double quotes
    config = Config(model_defaults=ModelDefaultsConfig(dialect="bigquery"))
    ctx = Context(config=config)
    ctx.default_catalog = "sqlmesh"

    assert ctx.default_catalog == "sqlmesh"
    assert ctx.default_dialect == "bigquery"

    model = create_sql_model(
        name="foo.bar", query=parse_one("select 1"), default_catalog=ctx.default_catalog
    )
    ctx.upsert_model(model)

    assert '"sqlmesh"."foo"."bar"' in ctx.models

    def _resolve_ref(ref_name: str) -> t.Optional[BaseRelation]:
        if ref_name == "bar":
            relation = BaseRelation.create(
                database="sqlmesh", schema="foo", identifier="bar", quote_character="`"
            )
            assert relation.render() == "`sqlmesh`.`foo`.`bar`"
            return relation
        return None

    jinja_env = environment()
    jinja_env.globals["ref"] = _resolve_ref

    assert (
        transform_str(
            input,
            jt.rewrite_dbt_ref_to_migrated_ref(ctx.models, jinja_env, dialect=ctx.default_dialect),
        )
        == expected
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        ("select * from {{ source('upstream', 'foo') }} as t", "select * from upstream.foo as t"),
        ("select * from {{ source('unknown', 'foo') }} as t", "select * from unknown.foo as t"),
        (
            "{% macro foo() %}select * from {{ source('upstream', 'foo') }}{% endmacro %}",
            "{% macro foo() %}select * from upstream.foo{% endmacro %}",
        ),
        # these shouldnt be transformed as the macro call might rely on some property of the Relation object returned by source()
        ("select * from {% if some_macro(source('upstream', 'foo')) %}foo{% endif %}", None),
        ("{{ dbt_utils.union_relations([source('upstream', 'foo')]) }},", None),
        (
            "select * from {% if some_macro(source('upstream', 'foo')) %}{{ source('upstream', 'foo') }}{% endif %}",
            "select * from {% if some_macro(source('upstream', 'foo')) %}upstream.foo{% endif %}",
        ),
        ("{{ some_macro(source('upstream', 'foo')) }}", None),
        ("{% set results = run_query('select foo from ' ~ source('schema', 'table')) %}", None),
    ],
)
def test_resolve_dbt_source_to_model_name(input: str, expected: t.Optional[str]) -> None:
    expected = expected or input

    from dbt.adapters.base import BaseRelation

    # note: bigquery dialect chosen because its identifiers have backticks
    # but internally SQLMesh stores model fqn with double quotes
    config = Config(model_defaults=ModelDefaultsConfig(dialect="bigquery"))
    ctx = Context(config=config)
    ctx.default_catalog = "sqlmesh"

    assert ctx.default_catalog == "sqlmesh"
    assert ctx.default_dialect == "bigquery"

    model = create_external_model(name="upstream.foo", default_catalog=ctx.default_catalog)
    ctx.upsert_model(model)

    assert '"sqlmesh"."upstream"."foo"' in ctx.models

    def _resolve_source(schema_name: str, table_name: str) -> t.Optional[BaseRelation]:
        if schema_name == "upstream" and table_name == "foo":
            relation = BaseRelation.create(
                database="sqlmesh", schema="upstream", identifier="foo", quote_character="`"
            )
            assert relation.render() == "`sqlmesh`.`upstream`.`foo`"
            return relation
        return None

    jinja_env = environment()
    jinja_env.globals["source"] = _resolve_source

    assert (
        transform_str(
            input,
            jt.resolve_dbt_source_to_model_name(ctx.models, jinja_env, dialect=ctx.default_dialect),
        )
        == expected
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            "select * from {{ source('upstream', 'foo') }} as t",
            "select * from {{ __migrated_source(database='sqlmesh', schema='upstream', identifier='foo') }} as t",
        ),
        (
            "select * from {{ source('unknown', 'foo') }} as t",
            "select * from {{ source('unknown', 'foo') }} as t",
        ),
        (
            "{% macro foo() %}select * from {{ source('upstream', 'foo') }}{% endmacro %}",
            "{% macro foo() %}select * from {{ __migrated_source(database='sqlmesh', schema='upstream', identifier='foo') }}{% endmacro %}",
        ),
        (
            "select * from {% if some_macro(source('upstream', 'foo')) %}foo{% endif %}",
            "select * from {% if some_macro(__migrated_source(database='sqlmesh', schema='upstream', identifier='foo')) %}foo{% endif %}",
        ),
        (
            "{{ dbt_utils.union_relations([source('upstream', 'foo')]) }},",
            "{{ dbt_utils.union_relations([__migrated_source(database='sqlmesh', schema='upstream', identifier='foo')]) }},",
        ),
        (
            "select * from {% if some_macro(source('upstream', 'foo')) %}{{ source('upstream', 'foo') }}{% endif %}",
            "select * from {% if some_macro(__migrated_source(database='sqlmesh', schema='upstream', identifier='foo')) %}{{ __migrated_source(database='sqlmesh', schema='upstream', identifier='foo') }}{% endif %}",
        ),
        (
            "{{ some_macro(source('upstream', 'foo')) }}",
            "{{ some_macro(__migrated_source(database='sqlmesh', schema='upstream', identifier='foo')) }}",
        ),
        (
            "{% set results = run_query('select foo from ' ~ source('upstream', 'foo')) %}",
            "{% set results = run_query('select foo from ' ~ __migrated_source(database='sqlmesh', schema='upstream', identifier='foo')) %}",
        ),
    ],
)
def test_rewrite_dbt_source_to_migrated_source(input: str, expected: t.Optional[str]) -> None:
    expected = expected or input

    from dbt.adapters.base import BaseRelation

    # note: bigquery dialect chosen because its identifiers have backticks
    # but internally SQLMesh stores model fqn with double quotes
    config = Config(model_defaults=ModelDefaultsConfig(dialect="bigquery"))
    ctx = Context(config=config)
    ctx.default_catalog = "sqlmesh"

    assert ctx.default_catalog == "sqlmesh"
    assert ctx.default_dialect == "bigquery"

    model = create_external_model(name="upstream.foo", default_catalog=ctx.default_catalog)
    ctx.upsert_model(model)

    assert '"sqlmesh"."upstream"."foo"' in ctx.models

    def _resolve_source(schema_name: str, table_name: str) -> t.Optional[BaseRelation]:
        if schema_name == "upstream" and table_name == "foo":
            relation = BaseRelation.create(
                database="sqlmesh", schema="upstream", identifier="foo", quote_character="`"
            )
            assert relation.render() == "`sqlmesh`.`upstream`.`foo`"
            return relation
        return None

    jinja_env = environment()
    jinja_env.globals["source"] = _resolve_source

    assert (
        transform_str(
            input,
            jt.rewrite_dbt_source_to_migrated_source(
                ctx.models, jinja_env, dialect=ctx.default_dialect
            ),
        )
        == expected
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        ("select * from {{ this }}", "select * from foo.bar"),
        ("{% if foo(this) %}bar{% endif %}", None),
        ("select * from {{ this.identifier }}", None),
    ],
)
def test_resolve_dbt_this_to_model_name(input: str, expected: t.Optional[str]):
    expected = expected or input
    assert transform_str(input, jt.resolve_dbt_this_to_model_name("foo.bar")) == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        # sqlmesh_incremental present, is_incremental() block removed
        (
            """
     select * from foo where
        {% if is_incremental() %}ds > (select max(ds)) from {{ this }}){% endif %}
        {% if sqlmesh_incremental is defined %}ds BETWEEN {{ start_ds }} and {{ end_ds }}{% endif %}
    """,
            """
    select * from foo
    where
        {% if sqlmesh_incremental is defined %}ds BETWEEN {{ start_ds }} and {{ end_ds }}{% endif %}
    """,
        ),
        # sqlmesh_incremental is NOT present; is_incremental() blocks untouched
        (
            """
    select * from foo
    where
        {% if is_incremental() %}ds > (select max(ds)) from {{ this }}){% endif %}
    """,
            """
    select * from foo
    where
        {% if is_incremental() %}ds > (select max(ds)) from {{ this }}){% endif %}
    """,
        ),
    ],
)
def test_deduplicate_incremental_checks(input: str, expected: str) -> None:
    assert " ".join(transform_str(input, jt.deduplicate_incremental_checks()).split()) == " ".join(
        expected.split()
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        # is_incremental() removed
        (
            "select * from foo where {% if is_incremental() %}ds >= (select max(ds) from {{ this }} ){% endif %}",
            "select * from foo where ds >= (select max(ds) from {{ this }} )",
        ),
        # sqlmesh_incremental removed
        (
            "select * from foo where {% if sqlmesh_incremental is defined %}ds BETWEEN {{ start_ds }} and {{ end_ds }}{% endif %}",
            "select * from foo where ds BETWEEN {{ start_ds }} and {{ end_ds }}",
        ),
        # else untouched
        (
            "select * from foo where {% if is_incremental() %}ds >= (select max(ds) from {{ this }} ){% else %}ds is not null{% endif %}",
            "select * from foo where {% if is_incremental() %}ds >= (select max(ds) from {{ this }} ){% else %}ds is not null{% endif %}",
        ),
    ],
)
def test_unpack_incremental_checks(input: str, expected: str) -> None:
    assert " ".join(transform_str(input, jt.unpack_incremental_checks()).split()) == " ".join(
        expected.split()
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        ("{{ start_ds }}", "@start_ds"),
        (
            "select id, ds from foo where ds between {{ start_ts }} and {{ end_ts }}",
            "select id, ds from foo where ds between @start_ts and @end_ts",
        ),
        ("select {{ some_macro(start_ts) }}", None),
        ("{{ start_date }}", "@start_date"),
        ("'{{ start_date }}'", "'@start_ds'"),  # date inside string literal should remain a string
    ],
)
def test_rewrite_sqlmesh_predefined_variables_to_sqlmesh_macro_syntax(
    input: str, expected: t.Optional[str]
) -> None:
    expected = expected or input
    assert (
        transform_str(input, jt.rewrite_sqlmesh_predefined_variables_to_sqlmesh_macro_syntax())
        == expected
    )


@pytest.mark.parametrize(
    "input,expected,package",
    [
        ("{{ var('foo') }}", "{{ var('foo') }}", None),
        ("{{ var('foo') }}", "{{ var('foo', __dbt_package='test') }}", "test"),
        (
            "{{ var('foo', 'default') }}",
            "{{ var('foo', 'default', __dbt_package='test') }}",
            "test",
        ),
        (
            "{% if 'col_name' in var('history_columns') %}bar{% endif %}",
            "{% if 'col_name' in var('history_columns', __dbt_package='test') %}bar{% endif %}",
            "test",
        ),
    ],
)
def test_append_dbt_package_kwarg_to_var_calls(
    input: str, expected: str, package: t.Optional[str]
) -> None:
    assert (
        transform_str(input, jt.append_dbt_package_kwarg_to_var_calls(package_name=package))
        == expected
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            "select * from foo where ds between '@start_dt' and '@end_dt'",
            "SELECT * FROM foo WHERE ds BETWEEN @start_dt AND @end_dt",
        ),
        (
            "select * from foo where bar <> '@unrelated'",
            "SELECT * FROM foo WHERE bar <> '@unrelated'",
        ),
    ],
)
def test_unwrap_macros_in_string_literals(input: str, expected: str) -> None:
    assert parse_one(input).transform(jt.unwrap_macros_in_string_literals()).sql() == expected
