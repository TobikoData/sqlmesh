from pathlib import Path
from sqlmesh.core.context import Context
from sqlmesh.dbt.converter.convert import convert_project_files, resolve_fqns_to_model_names
import uuid
import sqlmesh.core.constants as c


def test_convert_project_files(sushi_dbt_context: Context, tmp_path: Path) -> None:
    src_context = sushi_dbt_context
    src_path = sushi_dbt_context.path
    output_path = tmp_path / f"output_{uuid.uuid4().hex}"

    convert_project_files(src_path, output_path)

    target_context = Context(paths=output_path)

    assert src_context.models.keys() == target_context.models.keys()

    target_context.plan(auto_apply=True)


def test_convert_project_files_includes_library_macros(
    sushi_dbt_context: Context, tmp_path: Path
) -> None:
    src_path = sushi_dbt_context.path
    output_path = tmp_path / f"output_{uuid.uuid4().hex}"

    (src_path / "macros" / "call_library.sql").write_text("""
{% macro call_library() %}
    {{ dbt.current_timestamp() }}
{% endmacro %}
""")

    convert_project_files(src_path, output_path)

    migrated_output_macros_path = output_path / "macros" / c.MIGRATED_DBT_PACKAGES
    assert (migrated_output_macros_path / "dbt" / "current_timestamp.sql").exists()
    # note: the DBT manifest is smart enough to prune "dbt / default__current_timestamp.sql" from the list so it is not migrated
    assert (migrated_output_macros_path / "dbt_duckdb" / "duckdb__current_timestamp.sql").exists()


def test_resolve_fqns_to_model_names(empty_dbt_context: Context) -> None:
    ctx = empty_dbt_context

    # macro that uses a property of {{ ref() }} and also creates another ref()
    (ctx.path / "macros" / "foo.sql").write_text(
        """
{% macro foo(relation) %}
    {{ relation.name }} r
    left join {{ source('external', 'orders') }} et
        on r.id = et.id
{% endmacro %}
"""
    )

    # model 1 - can be fully unwrapped
    (ctx.path / "models" / "model1.sql").write_text(
        """
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
"""
    )

    # model 2 - has ref passed to macro as parameter and also another ref nested in macro
    (ctx.path / "models" / "model2.sql").write_text(
        """
select * from {{ foo(ref('model1')) }} union select * from {{ ref('items') }}
"""
    )

    ctx.load()

    assert len(ctx.models) == 3

    model1 = ctx.models['"memory"."project"."model1"']
    model2 = ctx.models['"memory"."project"."model2"']

    assert model1.depends_on == {'"memory"."project_raw"."items"'}
    assert model2.depends_on == {
        '"memory"."project"."model1"',
        '"memory"."external"."orders"',
        '"memory"."project_raw"."items"',
    }

    # All dependencies in model 1 can be tracked by the native loader but its very difficult to cover all the edge cases at conversion time
    # so we still populate depends_on()
    assert resolve_fqns_to_model_names(ctx, model1.depends_on) == {"project_raw.items"}

    # For model 2, the external model "external.orders" should be removed from depends_on
    # If it was output verbatim as depends_on ("memory"."external"."orders"), the native loader would throw an error like:
    # - Error: Failed to load model definition, 'Dot' object is not iterable
    assert resolve_fqns_to_model_names(ctx, model2.depends_on) == {
        "project.model1",
        "project_raw.items",
    }
