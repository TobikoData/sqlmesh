import pytest
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroExtractor
from sqlmesh.dbt.converter.jinja import JinjaGenerator, convert_jinja_query, convert_jinja_macro
import sqlmesh.dbt.converter.jinja_transforms as jt
from pathlib import Path
from sqlmesh.core.context import Context
import sqlmesh.core.dialect as d
from sqlglot import exp
from _pytest.mark.structures import ParameterSet
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.utils import columns_to_types_all_known


def _load_fixture(name: str) -> ParameterSet:
    return pytest.param(
        (Path(__file__).parent / "fixtures" / name).read_text(encoding="utf8"), id=name
    )


@pytest.mark.parametrize(
    "original_jinja",
    [
        "select 1",
        "select bar from {{ ref('foo') }} as f",
        "select max(ds) from {{ this }}",
        "{% if is_incremental() %}where ds > (select max(ds) from {{ this }}){% endif %}",
        "foo {% if sqlmesh_incremental is defined %} bar {% endif %} bar",
        "foo between '{{ start_ds }}' and '{{ end_ds }}'",
        "{{ 42 }}",
        "{{ foo.bar }}",
        "{{ 'baz' }}",
        "{{ col }} BETWEEN '{{ dates[0] }}' AND '{{ dates[1] }}'",
        "{% set foo = bar(baz, bing='bong') %}",
        "{% if a == 'ds' %}foo{% elif a == 'ts' %}bar{% elif a < 'ys' or (b != 'ds' and c >= 'ts') %}baz{% else %}bing{% endif %}",
        "{% set my_string = my_string ~ stuff ~ ', ' ~ 1 %}",
        "{{ context.do_some_action('param') }}",
        "{% set big_ole_block %}foo{% endset %}",
        "{% if not loop.last %}foo{% endif %}",
        "{% for a, b in some_func(a=foo['bar'][0], b=c.d[5]).items() %}foo_{{ a }}_{{ b }}{% endfor %}",
        "{{ column | replace(prefix, '') }}",
        "{{ column | filter('a', foo='bar') }}",
        "{% filter upper %}foo{% endfilter %}",
        "{% filter foo(0, bar='baz') %}foo{% endfilter %}",
        "{% if foo in ('bar', 'baz') %}bar{% endif %}",
        "{% if foo not in ('bar', 'baz') %}bing{% endif %}",
        "{% if (field.a if field.a else field.b) | lower not in ('c', 'd') %}foo{% endif %}",
        "{% do foo.bar('baz') %}",
        "{% set a = (col | lower + '_') + b %}",
        "{{ foo[1:10] | lower }}",
        "{{ foo[1:] }}",
        "{{ foo[:1] }}",
        "{% for col in all_columns if col.name in columns_to_compare and col.name in special_names %}{{ col }}{% endfor %}",
        "{{ ' or ' if not loop.first else '' }}",
        "{% set foo = ['a', 'b', c, d.e, f[0], g.h.i[0][1]] %}",
        """{% set foo = "('%Y%m%d', partition_id)" %}""",
        "{% set foo = (graph.nodes.values() | selectattr('name', 'equalto', model_name) | list)[0] %}",
        "{% set foo.bar = baz.bing(database='foo') %}",
        "{{ return(('some', 'tuple')) }}",
        "{% call foo('bar', baz=True) %}bar{% endcall %}",
        "{% call(user) dump_users(list_of_user) %}bar{% endcall %}",
        "{% macro foo(a, b='default', c=None) %}{% endmacro %}",
        # "{# some comment #}", #todo: comments get stripped entirely
        # "foo\n{%- if bar -%}    baz {% endif -%}", #todo: whitespace trim handling is a nice-to-have
        _load_fixture("model_query_incremental.sql"),
        _load_fixture("macro_dbt_incremental.sql"),
        _load_fixture("jinja_nested_if.sql"),
    ],
)
def test_generator_roundtrip(original_jinja: str) -> None:
    registry = JinjaMacroRegistry()
    env = registry.build_environment()

    ast = env.parse(original_jinja)
    generated = JinjaGenerator().generate(ast)

    assert generated == original_jinja

    me = MacroExtractor()
    # basically just test this doesnt throw an exception.
    # The MacroExtractor uses SQLGLot's tokenizer and not Jinja's so these need to work when the converted project is loaded by the native loader
    me.extract(generated)


def test_generator_sql_comment_macro():
    jinja_str = "-- before sql comment{% macro foo() %}-- inner sql comment{% endmacro %}"

    registry = JinjaMacroRegistry()
    env = registry.build_environment()

    ast = env.parse(jinja_str)
    generated = JinjaGenerator().generate(ast)

    assert (
        generated == "-- before sql comment\n{% macro foo() %}-- inner sql comment\n{% endmacro %}"
    )

    # check roundtripping an existing newline doesnt keep adding newlines
    assert JinjaGenerator().generate(env.parse(generated)) == generated


@pytest.mark.parametrize("original_jinja", [_load_fixture("macro_func_with_params.sql")])
def test_generator_roundtrip_ignore_whitespace(original_jinja: str) -> None:
    """
    This makes the following assumptions:
      - SQL isnt too sensitive about indentation / whitespace
      - The Jinja AST doesnt capture enough information to perfectly replicate the input template with regards to whitespace handling

    So if, disregarding whitespace, the original input string is the same as the AST being run through the generator: the test passes
    """
    registry = JinjaMacroRegistry()
    env = registry.build_environment()

    ast = env.parse(original_jinja)

    generated = JinjaGenerator().generate(ast)

    assert " ".join(original_jinja.split()) == " ".join(generated.split())


def test_convert_jinja_query(sushi_dbt_context: Context) -> None:
    model = sushi_dbt_context.models['"memory"."sushi"."customer_revenue_by_day"']
    assert isinstance(model, SqlModel)

    query = model.query
    assert isinstance(query, d.JinjaQuery)

    result = convert_jinja_query(sushi_dbt_context, model, query)

    assert isinstance(result, exp.Query)

    assert (
        result.sql(dialect=model.dialect, pretty=True)
        == """WITH order_total AS (
  SELECT
    oi.order_id AS order_id,
    SUM(oi.quantity * i.price) AS total,
    oi.ds AS ds
  FROM sushi_raw.order_items AS oi
  LEFT JOIN sushi_raw.items AS i
    ON oi.item_id = i.id AND oi.ds = i.ds
  WHERE
    oi.ds BETWEEN @start_ds AND @end_ds
  GROUP BY
    oi.order_id,
    oi.ds
)
SELECT
  CAST(o.customer_id AS INT) AS customer_id, /* Customer id */
  CAST(SUM(ot.total) AS DOUBLE) AS revenue, /* Revenue from orders made by this customer */
  CAST(o.ds AS TEXT) AS ds /* Date */
FROM sushi_raw.orders AS o
LEFT JOIN order_total AS ot
  ON o.id = ot.order_id AND o.ds = ot.ds
WHERE
  o.ds BETWEEN @start_ds AND @end_ds
GROUP BY
  o.customer_id,
  o.ds"""
    )


def test_convert_jinja_query_exclude_transform(empty_dbt_context: Context) -> None:
    ctx = empty_dbt_context

    (ctx.path / "models" / "model1.sql").write_text("""
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds'
        )
    }}

    select * from {{ ref('items') }}
    {% if is_incremental() %}
        where ds > (select max(ds) from {{ this }})
    {% endif %}
    """)

    ctx.load()

    model = ctx.models['"memory"."project"."model1"']
    assert isinstance(model, SqlModel)

    query = model.query
    assert isinstance(query, d.JinjaQuery)

    converted_query = convert_jinja_query(
        ctx,
        model,
        query,
        exclude=[jt.resolve_dbt_ref_to_model_name, jt.rewrite_dbt_ref_to_migrated_ref],
    )
    sql = converted_query.sql()

    assert "{{ ref('items') }}" in sql
    assert "{{ this }}" not in sql
    assert "{% if is_incremental() %}" not in sql
    assert "{% endif %}" not in sql


def test_convert_jinja_query_self_referencing(empty_dbt_context: Context) -> None:
    ctx = empty_dbt_context

    (ctx.path / "models" / "model1.sql").write_text("""
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds'
        )
    }}

    select * from {{ ref('items') }}
    {% if is_incremental() %}
        where ds > (select max(ds) from {{ this }})
    {% endif %}
    """)

    ctx.load()

    model = ctx.models['"memory"."project"."model1"']
    assert model.columns_to_types_or_raise
    assert (
        not model.depends_on_self
    )  # the DBT loader doesnt detect self-references within is_incremental blocks
    assert isinstance(model, SqlModel)

    query = model.query
    assert isinstance(query, d.JinjaQuery)

    converted_query = convert_jinja_query(ctx, model, query)
    converted_model_definition = model.copy().render_definition()[0].sql()

    # load from scratch to use the native loader and clear @cached_property's
    ctx.upsert_model(
        load_sql_based_model(
            expressions=[d.parse_one(converted_model_definition), converted_query],
            default_catalog=ctx.default_catalog,
        )
    )
    converted_model = ctx.models['"memory"."project"."model1"']
    assert isinstance(converted_model, SqlModel)

    assert not "{% is_incremental" in converted_model.query.sql()
    assert (
        converted_model.depends_on_self
    )  # Once the is_incremental blocks are removed, the model can be detected as self referencing
    assert columns_to_types_all_known(
        converted_model.columns_to_types_or_raise
    )  # columns to types must all be known for self-referencing models


def test_convert_jinja_query_self_referencing_columns_to_types_not_all_known(
    empty_dbt_context: Context,
) -> None:
    ctx = empty_dbt_context

    (ctx.path / "models" / "model1.sql").write_text("""
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds'
        )
    }}

    select id, name, ds from external.table
    {% if is_incremental() %}
        where ds > (select max(ds) from {{ this }})
    {% endif %}
    """)

    ctx.load()

    model = ctx.models['"memory"."project"."model1"']
    assert model.columns_to_types_or_raise
    assert (
        not model.depends_on_self
    )  # the DBT loader doesnt detect self-references within is_incremental blocks
    assert isinstance(model, SqlModel)

    query = model.query
    assert isinstance(query, d.JinjaQuery)

    converted_query = convert_jinja_query(ctx, model, query)
    converted_model_definition = model.render_definition()[0].sql()

    # load from scratch to use the native loader and clear @cached_property's
    ctx.upsert_model(
        load_sql_based_model(
            expressions=[d.parse_one(converted_model_definition), converted_query],
            jinja_macros=model.jinja_macros,
            default_catalog=ctx.default_catalog,
        )
    )
    converted_model = ctx.models['"memory"."project"."model1"']
    assert isinstance(converted_model, SqlModel)

    # {% is_incremental() %} block should be retained because removing it would make the model self-referencing but the columns_to_types
    # arent all known so this would create a load error like: Error: Self-referencing models require inferrable column types.
    assert "{% if is_incremental" in converted_model.query.sql()
    assert "{{ this }}" not in converted_model.query.sql()
    assert not converted_model.depends_on_self

    assert not columns_to_types_all_known(
        converted_model.columns_to_types_or_raise
    )  # this is ok because the model is not self-referencing


def test_convert_jinja_query_migrated_ref(empty_dbt_context: Context) -> None:
    ctx = empty_dbt_context

    (ctx.path / "models" / "model1.sql").write_text("""
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds'
        )
    }}
                                                    
    {% macro ref_handler(relation) %}
        {{ relation.name }}
    {% endmacro %}

    select * from {{ ref_handler(ref("items")) }}
    """)

    ctx.load()

    model = ctx.models['"memory"."project"."model1"']
    assert isinstance(model, SqlModel)
    query = model.query
    assert isinstance(query, d.JinjaQuery)

    converted_query = convert_jinja_query(ctx, model, query)

    assert (
        """select * from {{ ref_handler(__migrated_ref(database='memory', schema='project_raw', identifier='items', sqlmesh_model_name='project_raw.items')) }}"""
        in converted_query.sql()
    )


def test_convert_jinja_query_post_statement(empty_dbt_context: Context) -> None:
    ctx = empty_dbt_context

    (ctx.path / "models" / "model1.sql").write_text("""
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds',
            post_hook="create index foo_idx on {{ this }} (id)"
        )
    }}
                                                    
    select * from {{ ref("items") }}    
    """)

    ctx.load()

    model = ctx.models['"memory"."project"."model1"']
    assert isinstance(model, SqlModel)

    assert model.post_statements
    post_statement = model.post_statements[0]
    assert isinstance(post_statement, d.JinjaStatement)

    converted_post_statement = convert_jinja_query(ctx, model, post_statement)

    assert "CREATE INDEX foo_idx ON project.model1(id)" in converted_post_statement.sql(
        dialect="duckdb"
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            """
    {% macro incremental_by_time(col, time_type) %}
        {% if is_incremental() %}
        WHERE
            {{ col }} > (select max({{ col }}) from {{ this }})
        {% endif %}
        {% if sqlmesh_incremental is defined %}
        {% set dates = incremental_dates_by_time_type(time_type) %}
        WHERE
            {{ col }} BETWEEN '{{ dates[0] }}' AND '{{ dates[1] }}'
        {% endif %}
    {% endmacro %}
    """,
            """
    {% macro incremental_by_time(col, time_type) %}
        {% set dates = incremental_dates_by_time_type(time_type) %}
        WHERE
            {{ col }} BETWEEN '{{ dates[0] }}' AND '{{ dates[1] }}'
    {% endmacro %}
    """,
        ),
        (
            """
    {% macro foo(iterations) %}
        with base as (
            select * from {{ ref('customer_revenue_by_day') }}
        ),
        iter as (
            {% for i in range(0, iterations) %}
            'iter_{{ i }}' as iter_num_{{ i }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        select 1
    {% endmacro %}""",
            """
    {% macro foo(iterations) %}
        with base as (
            select * from sushi.customer_revenue_by_day
        ),
        iter as (
            {% for i in range(0, iterations) %}
            'iter_{{ i }}' as iter_num_{{ i }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        select 1
    {% endmacro %}""",
        ),
        (
            """{% macro expand_ref(model_name) %}{{ ref(model_name) }}{% endmacro %}""",
            """{% macro expand_ref(model_name) %}{{ ref(model_name) }}{% endmacro %}""",
        ),
    ],
)
def test_convert_jinja_macro(input: str, expected: str, sushi_dbt_context: Context) -> None:
    result = convert_jinja_macro(sushi_dbt_context, input.strip())

    assert " ".join(result.split()) == " ".join(expected.strip().split())
