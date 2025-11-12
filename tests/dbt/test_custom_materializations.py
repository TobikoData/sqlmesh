from __future__ import annotations

import typing as t
from pathlib import Path

import pytest

from sqlmesh import Context
from sqlmesh.core.config import ModelDefaultsConfig
from sqlmesh.core.engine_adapter import DuckDBEngineAdapter
from sqlmesh.core.model.kind import DbtCustomKind
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.basemodel import Materialization

pytestmark = pytest.mark.dbt


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_manifest_loading():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )
    materializations = helper.materializations()

    # custom materialization should have loaded from the manifest
    assert "custom_incremental_default" in materializations
    custom_incremental = materializations["custom_incremental_default"]
    assert custom_incremental.name == "custom_incremental"
    assert custom_incremental.adapter == "default"
    assert "make_temp_relation(new_relation)" in custom_incremental.definition
    assert "run_hooks(pre_hooks)" in custom_incremental.definition
    assert " {{ return({'relations': [new_relation]}) }}" in custom_incremental.definition


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_model_config():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )

    models = helper.models()

    custom_model = models["custom_incremental_model"]
    assert isinstance(custom_model, ModelConfig)
    assert custom_model.materialized == "custom_incremental"
    assert custom_model.model_materialization == Materialization.CUSTOM

    # pre and post hooks should also be handled in custom materializations
    assert len(custom_model.pre_hook) == 2
    assert (
        custom_model.pre_hook[1].sql
        == "CREATE TABLE IF NOT EXISTS hook_table (id INTEGER, length_col TEXT, updated_at TIMESTAMP)"
    )
    assert len(custom_model.post_hook) == 2
    assert "COALESCE(MAX(id), 0)" in custom_model.post_hook[1].sql

    custom_filter_model = models["custom_incremental_with_filter"]
    assert isinstance(custom_filter_model, ModelConfig)
    assert custom_filter_model.materialized == "custom_incremental"
    assert custom_filter_model.model_materialization == Materialization.CUSTOM
    assert custom_filter_model.interval == "2 day"
    assert custom_filter_model.time_column == "created_at"

    # verify also that the global hooks are inherited in the model without
    assert len(custom_filter_model.pre_hook) == 1
    assert len(custom_filter_model.post_hook) == 1


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_model_kind():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    context = DbtContext(project_path)
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )

    context._target = profile.target
    context._manifest = helper
    models = helper.models()

    # custom materialization models get DbtCustomKind populated
    custom_model = models["custom_incremental_model"]
    kind = custom_model.model_kind(context)
    assert isinstance(kind, DbtCustomKind)
    assert kind.materialization == "custom_incremental"
    assert kind.adapter == "default"
    assert "create_table_as" in kind.definition

    custom_filter_model = models["custom_incremental_with_filter"]
    kind = custom_filter_model.model_kind(context)
    assert isinstance(kind, DbtCustomKind)
    assert kind.materialization == "custom_incremental"
    assert kind.adapter == "default"
    assert "run_hooks" in kind.definition

    # the DbtCustomKind shouldnt be set for normal strategies
    regular_model = models["simple_model_a"]
    regular_kind = regular_model.model_kind(context)
    assert not isinstance(regular_kind, DbtCustomKind)

    # verify in sqlmesh as well
    sqlmesh_context = Context(
        paths=["tests/fixtures/dbt/sushi_test"],
        config=None,
    )

    custom_incremental = sqlmesh_context.get_model("sushi.custom_incremental_model")
    assert isinstance(custom_incremental.kind, DbtCustomKind)
    assert custom_incremental.kind.materialization == "custom_incremental"

    custom_with_filter = sqlmesh_context.get_model("sushi.custom_incremental_with_filter")
    assert isinstance(custom_with_filter.kind, DbtCustomKind)
    assert custom_with_filter.kind.materialization == "custom_incremental"


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_dependencies():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    context = DbtContext(project_path)
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )

    context._target = profile.target
    context._manifest = helper
    models = helper.models()

    # custom materialization uses macros that should appear in dependencies
    for model_name in ["custom_incremental_model", "custom_incremental_with_filter"]:
        materialization_deps = models[model_name]._get_custom_materialization(context)
        assert materialization_deps is not None
        assert len(materialization_deps.dependencies.macros) > 0
        macro_names = [macro.name for macro in materialization_deps.dependencies.macros]
        expected_macros = [
            "build_incremental_filter_sql",
            "Relation",
            "create_table_as",
            "make_temp_relation",
            "run_hooks",
            "statement",
        ]
        assert any(macro in macro_names for macro in expected_macros)


@pytest.mark.xdist_group("dbt_manifest")
def test_adapter_specific_materialization_override(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    macros_dir = temp_project / "macros" / "materializations"
    macros_dir.mkdir(parents=True, exist_ok=True)

    adapter_mat_content = """
{%- materialization custom_adapter_test, default -%}
  {%- set new_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- call statement('main') -%}
    CREATE TABLE {{ new_relation }} AS (
      SELECT 'default_adapter' as adapter_type, * FROM ({{ sql }}) AS subquery
    )
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [new_relation]}) }}
{%- endmaterialization -%}

{%- materialization custom_adapter_test, adapter='postgres' -%}
  {%- set new_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- call statement('main') -%}
    CREATE TABLE {{ new_relation }} AS (
      SELECT 'postgres_adapter'::text as adapter_type, * FROM ({{ sql }}) AS subquery
    )
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [new_relation]}) }}
{%- endmaterialization -%}

{%- materialization custom_adapter_test, adapter='duckdb' -%}
  {%- set new_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- call statement('main') -%}
    CREATE TABLE {{ new_relation }} AS (
      SELECT 'duckdb_adapter' as adapter_type, * FROM ({{ sql }}) AS subquery
    )
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [new_relation]}) }}
{%- endmaterialization -%}
""".strip()

    (macros_dir / "custom_adapter_test.sql").write_text(adapter_mat_content)

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    test_model_content = """
{{ config(
    materialized='custom_adapter_test',
) }}

SELECT
    1 as id,
    'test' as name
""".strip()

    (models_dir / "test_adapter_specific.sql").write_text(test_model_content)

    context = DbtContext(temp_project)
    profile = Profile.load(context)

    helper = ManifestHelper(
        temp_project,
        temp_project,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )

    materializations = helper.materializations()
    assert "custom_adapter_test_default" in materializations
    assert "custom_adapter_test_duckdb" in materializations
    assert "custom_adapter_test_postgres" in materializations

    default_mat = materializations["custom_adapter_test_default"]
    assert "default_adapter" in default_mat.definition
    assert default_mat.adapter == "default"

    duckdb_mat = materializations["custom_adapter_test_duckdb"]
    assert "duckdb_adapter" in duckdb_mat.definition
    assert duckdb_mat.adapter == "duckdb"

    postgres_mat = materializations["custom_adapter_test_postgres"]
    assert "postgres_adapter" in postgres_mat.definition
    assert postgres_mat.adapter == "postgres"

    # verify that the correct adapter is selected based on target
    context._target = profile.target
    context._manifest = helper
    models = helper.models()

    test_model = models["test_adapter_specific"]

    kind = test_model.model_kind(context)
    assert isinstance(kind, DbtCustomKind)
    assert kind.materialization == "custom_adapter_test"
    # Should use duckdb adapter since that's the default target
    assert "duckdb_adapter" in kind.definition or "default_adapter" in kind.definition

    # test also that adapter-specific materializations execute with correct adapter
    sushi_context = Context(paths=path)

    plan = sushi_context.plan(select_models=["sushi.test_adapter_specific"])
    sushi_context.apply(plan)

    # check that the table was created with the correct adapter type
    result = sushi_context.engine_adapter.fetchdf("SELECT * FROM sushi.test_adapter_specific")
    assert len(result) == 1
    assert "adapter_type" in result.columns
    assert result["adapter_type"][0] == "duckdb_adapter"
    assert result["id"][0] == 1
    assert result["name"][0] == "test"


@pytest.mark.xdist_group("dbt_manifest")
def test_missing_custom_materialization_error():
    from sqlmesh.utils.errors import ConfigError

    project_path = Path("tests/fixtures/dbt/sushi_test")
    context = DbtContext(project_path)
    profile = Profile.load(context)

    # the materialization is non-existent
    fake_model_config = ModelConfig(
        name="test_model",
        path=project_path / "models" / "fake_model.sql",
        raw_code="SELECT 1 as id",
        materialized="non_existent_custom",
        schema="test_schema",
    )

    context._target = profile.target
    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )
    context._manifest = helper

    # Should raise ConfigError when trying to get the model kind
    with pytest.raises(ConfigError) as e:
        fake_model_config.model_kind(context)

    assert "Unknown materialization 'non_existent_custom'" in str(e.value)
    assert "Custom materializations must be defined" in str(e.value)


@pytest.mark.xdist_group("dbt_manifest")
def test_broken_jinja_materialization_error(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    macros_dir = temp_project / "macros" / "materializations"
    macros_dir.mkdir(parents=True, exist_ok=True)

    # Create broken Jinja materialization
    broken_mat_content = """
{%- materialization broken_jinja, default -%}
  {%- set new_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {# An intentional undefined variable that will cause runtime error #}
  {%- set broken_var = undefined_variable_that_does_not_exist + 10 -%}

  {%- call statement('main') -%}
    CREATE TABLE {{ new_relation }} AS (
      SELECT * FROM ({{ sql }}) AS subquery
      WHERE 1 = {{ broken_var }}
    )
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [new_relation]}) }}
{%- endmaterialization -%}
""".strip()

    (macros_dir / "broken_jinja.sql").write_text(broken_mat_content)

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    test_model_content = """
{{ config(
    materialized='broken_jinja',
) }}

SELECT
    1 as id,
    'This should fail with Jinja error' as error_msg
""".strip()

    (models_dir / "test_broken_jinja.sql").write_text(test_model_content)

    sushi_context = Context(paths=path)

    # The model will load fine jinja won't fail at parse time
    model = sushi_context.get_model("sushi.test_broken_jinja")
    assert isinstance(model.kind, DbtCustomKind)
    assert model.kind.materialization == "broken_jinja"

    # but execution should fail
    with pytest.raises(Exception) as e:
        plan = sushi_context.plan(select_models=["sushi.test_broken_jinja"])
        sushi_context.apply(plan)

    assert "plan application failed" in str(e.value).lower()


@pytest.mark.xdist_group("dbt_manifest")
def test_failing_hooks_in_materialization(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    test_model_content = """
{{ config(
    materialized='custom_incremental',
    pre_hook="CREATE TABLE will_fail_due_to_intentional_syntax_error (",
    post_hook="DROP TABLE non_existent_table_that_will_fail",
) }}

SELECT
    1 as id,
    'Testing hook failures' as test_msg
""".strip()

    (models_dir / "test_failing_hooks.sql").write_text(test_model_content)

    sushi_context = Context(paths=[str(temp_project)])

    # in this case the pre_hook has invalid syntax
    with pytest.raises(Exception) as e:
        plan = sushi_context.plan(select_models=["sushi.test_failing_hooks"])
        sushi_context.apply(plan)

    assert "plan application failed" in str(e.value).lower()


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_virtual_environments(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    test_model_content = """
{{ config(
    materialized='custom_incremental',
    time_column='created_at',
) }}

SELECT
    CURRENT_TIMESTAMP as created_at,
    1 as id,
    'venv_test' as test_type
""".strip()

    (models_dir / "test_venv_model.sql").write_text(test_model_content)

    sushi_context = Context(paths=path)
    prod_plan = sushi_context.plan(select_models=["sushi.test_venv_model"])
    sushi_context.apply(prod_plan)
    prod_result = sushi_context.engine_adapter.fetchdf(
        "SELECT * FROM sushi.test_venv_model ORDER BY id"
    )
    assert len(prod_result) == 1
    assert prod_result["id"][0] == 1
    assert prod_result["test_type"][0] == "venv_test"

    # Create dev environment and check the dev table was created with proper naming
    dev_plan = sushi_context.plan("dev", select_models=["sushi.test_venv_model"])
    sushi_context.apply(dev_plan)
    dev_result = sushi_context.engine_adapter.fetchdf(
        "SELECT * FROM sushi__dev.test_venv_model ORDER BY id"
    )
    assert len(dev_result) == 1
    assert dev_result["id"][0] == 1
    assert dev_result["test_type"][0] == "venv_test"

    dev_tables = sushi_context.engine_adapter.fetchdf("""
        SELECT table_name, table_schema
        FROM system.information_schema.tables
        WHERE table_schema LIKE 'sushi%dev%'
        AND table_name LIKE '%test_venv_model%'
    """)

    prod_tables = sushi_context.engine_adapter.fetchdf("""
        SELECT table_name, table_schema
        FROM system.information_schema.tables
        WHERE table_schema = 'sushi'
        AND table_name LIKE '%test_venv_model%'
    """)

    # Verify both environments have their own tables
    assert len(dev_tables) >= 1
    assert len(prod_tables) >= 1


@pytest.mark.xdist_group("dbt_manifest")
def test_virtual_environment_schema_names(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    test_model_content = """
{{ config(
    materialized='custom_incremental',
    time_column='created_at',
) }}

SELECT
    CURRENT_TIMESTAMP as created_at,
    1 as id,
    'schema_naming_test' as test_type
""".strip()

    (models_dir / "test_schema_naming.sql").write_text(test_model_content)

    context = Context(paths=path)
    prod_plan = context.plan(select_models=["sushi.test_schema_naming"])
    context.apply(prod_plan)

    dev_plan = context.plan("dev", select_models=["sushi.test_schema_naming"])
    context.apply(dev_plan)

    prod_result = context.engine_adapter.fetchdf(
        "SELECT * FROM sushi.test_schema_naming ORDER BY id"
    )
    assert len(prod_result) == 1
    assert prod_result["test_type"][0] == "schema_naming_test"

    dev_result = context.engine_adapter.fetchdf(
        "SELECT * FROM sushi__dev.test_schema_naming ORDER BY id"
    )
    assert len(dev_result) == 1
    assert dev_result["test_type"][0] == "schema_naming_test"

    # to examine the schema structure
    all_schemas_query = """
        SELECT DISTINCT table_schema, COUNT(*) as table_count
        FROM system.information_schema.tables
        WHERE table_schema LIKE '%sushi%'
        AND table_name LIKE '%test_schema_naming%'
        GROUP BY table_schema
        ORDER BY table_schema
    """

    schema_info = context.engine_adapter.fetchdf(all_schemas_query)

    schema_names = schema_info["table_schema"].tolist()

    # - virtual schemas: sushi, sushi__dev (for views)
    view_schemas = [s for s in schema_names if not s.startswith("sqlmesh__")]

    # - physical schema: sqlmesh__sushi (for actual data tables)
    physical_schemas = [s for s in schema_names if s.startswith("sqlmesh__")]

    # verify we got both of them
    assert len(view_schemas) >= 2
    assert len(physical_schemas) >= 1
    assert "sushi" in view_schemas
    assert "sushi__dev" in view_schemas
    assert any("sqlmesh__sushi" in s for s in physical_schemas)


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_lineage_tracking(copy_to_temp_path: t.Callable):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # create a custom materialization model that depends on simple_model_a and waiter_names seed
    lineage_model_content = """
{{ config(
    materialized='custom_incremental',
    time_column='created_at',
) }}

SELECT
    CURRENT_TIMESTAMP as created_at,
    w.id as waiter_id,
    w.name as waiter_name,
    s.a as simple_value,
    w.id * s.a as computed_value,
    'lineage_test' as model_type
FROM {{ ref('waiter_names') }} w
CROSS JOIN {{ ref('simple_model_a') }} s
""".strip()

    (models_dir / "enhanced_waiter_data.sql").write_text(lineage_model_content)

    # Create another custom materialization model that depends on the first one and simple_model_b
    downstream_model_content = """
{{ config(
    materialized='custom_incremental',
    time_column='analysis_date',
) }}

SELECT
    CURRENT_TIMESTAMP as analysis_date,
    e.waiter_name,
    e.simple_value,
    e.computed_value,
    b.a as model_b_value,
    e.computed_value + b.a as final_computation,
    CASE
        WHEN e.computed_value >= 5 THEN 'High'
        WHEN e.computed_value >= 2 THEN 'Medium'
        ELSE 'Low'
    END as category,
    'downstream_lineage_test' as model_type
FROM {{ ref('enhanced_waiter_data') }} e
CROSS JOIN {{ ref('simple_model_b') }} b
WHERE e.computed_value >= 0
""".strip()

    (models_dir / "waiter_analytics_summary.sql").write_text(downstream_model_content)

    context = Context(paths=path)
    enhanced_data_model = context.get_model("sushi.enhanced_waiter_data")
    analytics_summary_model = context.get_model("sushi.waiter_analytics_summary")

    # Verify that custom materialization models have proper model kinds
    assert isinstance(enhanced_data_model.kind, DbtCustomKind)
    assert enhanced_data_model.kind.materialization == "custom_incremental"

    assert isinstance(analytics_summary_model.kind, DbtCustomKind)
    assert analytics_summary_model.kind.materialization == "custom_incremental"

    # - enhanced_waiter_data should depend on waiter_names and simple_model_a
    enhanced_data_deps = enhanced_data_model.depends_on
    assert '"memory"."sushi"."simple_model_a"' in enhanced_data_deps
    assert '"memory"."sushi"."waiter_names"' in enhanced_data_deps

    # - waiter_analytics_summary should depend on enhanced_waiter_data and simple_model_b
    analytics_deps = analytics_summary_model.depends_on
    assert '"memory"."sushi"."enhanced_waiter_data"' in analytics_deps
    assert '"memory"."sushi"."simple_model_b"' in analytics_deps

    # build only the models that have dependences
    plan = context.plan(
        select_models=[
            "sushi.waiter_names",
            "sushi.simple_model_a",
            "sushi.simple_model_b",
            "sushi.enhanced_waiter_data",
            "sushi.waiter_analytics_summary",
        ]
    )
    context.apply(plan)

    # Verify that all δοwnstream models were built and contain expected data
    waiter_names_result = context.engine_adapter.fetchdf(
        "SELECT COUNT(*) as count FROM sushi.waiter_names"
    )
    assert waiter_names_result["count"][0] > 0

    simple_a_result = context.engine_adapter.fetchdf("SELECT a FROM sushi.simple_model_a")
    assert len(simple_a_result) > 0
    assert simple_a_result["a"][0] == 1

    simple_b_result = context.engine_adapter.fetchdf("SELECT a FROM sushi.simple_model_b")
    assert len(simple_b_result) > 0
    assert simple_b_result["a"][0] == 1

    # Check intermediate custom materialization model
    enhanced_data_result = context.engine_adapter.fetchdf("""
        SELECT
            waiter_name,
            simple_value,
            computed_value,
            model_type
        FROM sushi.enhanced_waiter_data
        ORDER BY waiter_id
        LIMIT 5
    """)

    assert len(enhanced_data_result) > 0
    assert enhanced_data_result["model_type"][0] == "lineage_test"
    assert all(val == 1 for val in enhanced_data_result["simple_value"])
    assert all(val >= 0 for val in enhanced_data_result["computed_value"])
    assert any(val == "Ryan" for val in enhanced_data_result["waiter_name"])

    # Check final downstream custom materialization model
    analytics_summary_result = context.engine_adapter.fetchdf("""
        SELECT
            waiter_name,
            category,
            model_type,
            final_computation
        FROM sushi.waiter_analytics_summary
        ORDER BY waiter_name
        LIMIT 5
    """)

    assert len(analytics_summary_result) > 0
    assert analytics_summary_result["model_type"][0] == "downstream_lineage_test"
    assert all(cat in ["High", "Medium", "Low"] for cat in analytics_summary_result["category"])
    assert all(val >= 0 for val in analytics_summary_result["final_computation"])

    # Test that lineage information is preserved in dev environments
    dev_plan = context.plan("dev", select_models=["sushi.waiter_analytics_summary"])
    context.apply(dev_plan)

    dev_analytics_result = context.engine_adapter.fetchdf("""
        SELECT
            COUNT(*) as count,
            COUNT(DISTINCT waiter_name) as unique_waiters
        FROM sushi__dev.waiter_analytics_summary
    """)

    prod_analytics_result = context.engine_adapter.fetchdf("""
        SELECT
            COUNT(*) as count,
            COUNT(DISTINCT waiter_name) as unique_waiters
        FROM sushi.waiter_analytics_summary
    """)

    # Dev and prod should have the same data as they share physical data
    assert dev_analytics_result["count"][0] == prod_analytics_result["count"][0]
    assert dev_analytics_result["unique_waiters"][0] == prod_analytics_result["unique_waiters"][0]


@pytest.mark.xdist_group("dbt_manifest")
def test_custom_materialization_grants(copy_to_temp_path: t.Callable, mocker):
    path = copy_to_temp_path("tests/fixtures/dbt/sushi_test")
    temp_project = path[0]

    models_dir = temp_project / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    grants_model_content = """
{{ config(
    materialized='custom_incremental',
    grants={
        'select': ['user1', 'user2'],
        'insert': ['writer']
    }
) }}

SELECT
    CURRENT_TIMESTAMP as created_at,
    1 as id,
    'grants_test' as test_type
""".strip()

    (models_dir / "test_grants_model.sql").write_text(grants_model_content)

    mocker.patch.object(DuckDBEngineAdapter, "SUPPORTS_GRANTS", True)
    mocker.patch.object(DuckDBEngineAdapter, "_get_current_grants_config", return_value={})

    sync_grants_calls = []

    def mock_sync_grants(*args, **kwargs):
        sync_grants_calls.append((args, kwargs))

    mocker.patch.object(DuckDBEngineAdapter, "sync_grants_config", side_effect=mock_sync_grants)

    context = Context(paths=path)

    model = context.get_model("sushi.test_grants_model")
    assert isinstance(model.kind, DbtCustomKind)
    plan = context.plan(select_models=["sushi.test_grants_model"])
    context.apply(plan)

    assert len(sync_grants_calls) == 1
    args = sync_grants_calls[0][0]
    assert args

    table = args[0]
    grants_config = args[1]
    assert table.sql(dialect="duckdb") == "memory.sushi.test_grants_model"
    assert grants_config == {
        "select": ["user1", "user2"],
        "insert": ["writer"],
    }
