import datetime
import typing as t
import pytest

from pathlib import Path

from sqlglot import exp
from sqlglot.errors import SchemaError
from sqlmesh import Context
from sqlmesh.core.model import TimeColumn, IncrementalByTimeRangeKind
from sqlmesh.core.model.kind import OnDestructiveChange, OnAdditiveChange
from sqlmesh.core.state_sync.db.snapshot import _snapshot_to_json
from sqlmesh.dbt.common import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.target import PostgresConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils.yaml import YAML

pytestmark = pytest.mark.dbt


@pytest.fixture
def create_empty_project(tmp_path: Path) -> t.Callable[[], t.Tuple[Path, Path]]:
    def _create_empty_project() -> t.Tuple[Path, Path]:
        yaml = YAML()
        dbt_project_dir = tmp_path / "dbt"
        dbt_project_dir.mkdir()
        dbt_model_dir = dbt_project_dir / "models"
        dbt_model_dir.mkdir()
        dbt_project_config = {
            "name": "empty_project",
            "version": "1.0.0",
            "config-version": 2,
            "profile": "test",
            "model-paths": ["models"],
        }
        dbt_project_file = dbt_project_dir / "dbt_project.yml"
        with open(dbt_project_file, "w", encoding="utf-8") as f:
            YAML().dump(dbt_project_config, f)
        sqlmesh_config = {
            "model_defaults": {
                "start": "2025-01-01",
            }
        }
        sqlmesh_config_file = dbt_project_dir / "sqlmesh.yaml"
        with open(sqlmesh_config_file, "w", encoding="utf-8") as f:
            YAML().dump(sqlmesh_config, f)
        dbt_data_dir = tmp_path / "dbt_data"
        dbt_data_dir.mkdir()
        dbt_data_file = dbt_data_dir / "local.db"
        dbt_profile_config = {
            "test": {
                "outputs": {"duckdb": {"type": "duckdb", "path": str(dbt_data_file)}},
                "target": "duckdb",
            }
        }
        db_profile_file = dbt_project_dir / "profiles.yml"
        with open(db_profile_file, "w", encoding="utf-8") as f:
            yaml.dump(dbt_profile_config, f)
        return dbt_project_dir, dbt_model_dir

    return _create_empty_project


def test_model_test_circular_references() -> None:
    upstream_model = ModelConfig(name="upstream")
    downstream_model = ModelConfig(name="downstream", dependencies=Dependencies(refs={"upstream"}))
    context = DbtContext(_refs={"upstream": upstream_model, "downstream": downstream_model})

    # Test and downstream model references
    downstream_test = TestConfig(
        name="downstream_with_upstream",
        sql="",
        dependencies=Dependencies(refs={"upstream", "downstream"}),
    )
    upstream_test = TestConfig(
        name="upstream_with_downstream",
        sql="",
        dependencies=Dependencies(refs={"upstream", "downstream"}),
    )

    # No circular reference
    downstream_model.tests = [downstream_test]
    downstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [downstream_test]

    # Upstream model reference in downstream model
    downstream_model.tests = []
    upstream_model.tests = [upstream_test]
    upstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [upstream_test]

    upstream_model.tests = [upstream_test]
    downstream_model.tests = [downstream_test]
    upstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [downstream_test, upstream_test]

    downstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [downstream_test, upstream_test]

    # Test only references
    upstream_model.tests = [upstream_test]
    downstream_model.tests = [downstream_test]
    downstream_model.dependencies = Dependencies()
    upstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [downstream_test, upstream_test]

    downstream_model.fix_circular_test_refs(context)
    assert upstream_model.tests == []
    assert downstream_model.tests == [downstream_test, upstream_test]


@pytest.mark.slow
def test_load_invalid_ref_audit_constraints(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    yaml = YAML()
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    full_model_contents = """{{ config(tags=["blah"], tests=[{"blah": {"to": "ref('completely_ignored')", "field": "blah2"} }]) }} SELECT 1 as cola"""
    full_model_file = model_dir / "full_model.sql"
    with open(full_model_file, "w", encoding="utf-8") as f:
        f.write(full_model_contents)
    model_schema = {
        "version": 2,
        "models": [
            {
                "name": "full_model",
                "description": "A full model bad ref for audit and constraints",
                "columns": [
                    {
                        "name": "cola",
                        "description": "A column that is used in a ref audit and constraints",
                        "constraints": [
                            {
                                "type": "primary_key",
                                "columns": ["cola"],
                                "expression": "ref('not_real_model') (cola)",
                            }
                        ],
                        "tests": [
                            {
                                # References a model that doesn't exist
                                "relationships": {
                                    "to": "ref('not_real_model')",
                                    "field": "cola",
                                },
                            },
                            {
                                # Reference a source that doesn't exist
                                "relationships": {
                                    "to": "source('not_real_source', 'not_real_table')",
                                    "field": "cola",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }
    model_schema_file = model_dir / "schema.yml"
    with open(model_schema_file, "w", encoding="utf-8") as f:
        yaml.dump(model_schema, f)

    context = Context(paths=project_dir)
    assert (
        "Skipping audit 'relationships_full_model_cola__cola__ref_not_real_model_' because model 'not_real_model' is not a valid ref"
        in caplog.text
    )
    assert (
        "Skipping audit 'relationships_full_model_cola__cola__source_not_real_source_not_real_table_' because source 'not_real_source.not_real_table' is not a valid ref"
        in caplog.text
    )
    fqn = '"local"."main"."full_model"'
    assert fqn in context.snapshots
    # The audit isn't loaded due to the invalid ref
    assert context.snapshots[fqn].model.audits == []


@pytest.mark.slow
def test_load_microbatch_all_defined(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    microbatch_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ds',
            begin='2020-01-01',
            batch_size='day',
            lookback=2,
            concurrent_batches=true
        )
    }}
    
    SELECT 1 as cola, '2025-01-01' as ds
    """
    microbatch_model_file = model_dir / "microbatch.sql"
    with open(microbatch_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_contents)

    snapshot_fqn = '"local"."main"."microbatch"'
    context = Context(paths=project_dir)
    model = context.snapshots[snapshot_fqn].model
    # Validate model-level attributes
    assert model.start == datetime.datetime(2020, 1, 1, 0, 0)
    assert model.interval_unit.is_day
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    assert model.kind.lookback == 2
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("ds", quoted=True), format="%Y-%m-%d"
    )
    assert model.kind.batch_size == 1
    assert model.depends_on_self is False


@pytest.mark.slow
def test_load_microbatch_all_defined_diff_values(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    microbatch_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            cron='@yearly',
            event_time='blah',
            begin='2022-01-01',
            batch_size='year',
            lookback=20,
            concurrent_batches=false
        )
    }}

    SELECT 1 as cola, '2022-01-01' as blah
    """
    microbatch_model_file = model_dir / "microbatch.sql"
    with open(microbatch_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_contents)

    snapshot_fqn = '"local"."main"."microbatch"'
    context = Context(paths=project_dir)
    model = context.snapshots[snapshot_fqn].model
    # Validate model-level attributes
    assert model.start == datetime.datetime(2022, 1, 1, 0, 0)
    assert model.interval_unit.is_year
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    assert model.kind.lookback == 20
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("blah", quoted=True), format="%Y-%m-%d"
    )
    assert model.kind.batch_size == 1
    assert model.depends_on_self is True


@pytest.mark.slow
def test_load_microbatch_required_only(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    microbatch_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            begin='2021-01-01',
            event_time='ds',
            batch_size='hour',
        )
    }}

    SELECT 1 as cola, '2021-01-01' as ds
    """
    microbatch_model_file = model_dir / "microbatch.sql"
    with open(microbatch_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_contents)

    snapshot_fqn = '"local"."main"."microbatch"'
    context = Context(paths=project_dir)
    model = context.snapshots[snapshot_fqn].model
    # Validate model-level attributes
    assert model.start == datetime.datetime(2021, 1, 1, 0, 0)
    assert model.interval_unit.is_hour
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    assert model.kind.lookback == 1
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("ds", quoted=True), format="%Y-%m-%d"
    )
    assert model.kind.batch_size == 1
    assert model.depends_on_self is False


@pytest.mark.slow
def test_load_incremental_time_range_strategy_required_only(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    incremental_time_range_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='incremental_by_time_range',
            time_column='ds',
        )
    }}

    SELECT 1 as cola, '2021-01-01' as ds
    """
    incremental_time_range_model_file = model_dir / "incremental_time_range.sql"
    with open(incremental_time_range_model_file, "w", encoding="utf-8") as f:
        f.write(incremental_time_range_contents)

    snapshot_fqn = '"local"."main"."incremental_time_range"'
    context = Context(paths=project_dir)
    snapshot = context.snapshots[snapshot_fqn]
    model = snapshot.model
    # Validate model-level attributes
    assert model.start == "2025-01-01"
    assert model.interval_unit.is_day
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    assert model.kind.lookback == 1
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("ds", quoted=True), format="%Y-%m-%d"
    )
    assert model.kind.batch_size is None
    assert model.depends_on_self is False
    assert model.kind.auto_restatement_intervals is None
    assert model.kind.partition_by_time_column is True
    # make sure the snapshot can be serialized to json
    assert isinstance(_snapshot_to_json(snapshot), str)


@pytest.mark.slow
def test_load_incremental_time_range_strategy_all_defined(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    incremental_time_range_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='incremental_by_time_range',
            time_column={
                'column': 'ds',
                'format': '%Y%m%d'
            },
            auto_restatement_intervals=3,
            partition_by_time_column=false,
            lookback=5,
            batch_size=3,
            batch_concurrency=2,
            forward_only=true,
            disable_restatement=true,
            on_destructive_change='allow',
            on_additive_change='error',
            auto_restatement_cron='@hourly',
            on_schema_change='ignore'
        )
    }}

    SELECT 1 as cola, '2021-01-01' as ds
    """
    incremental_time_range_model_file = model_dir / "incremental_time_range.sql"
    with open(incremental_time_range_model_file, "w", encoding="utf-8") as f:
        f.write(incremental_time_range_contents)

    snapshot_fqn = '"local"."main"."incremental_time_range"'
    context = Context(paths=project_dir)
    snapshot = context.snapshots[snapshot_fqn]
    model = snapshot.model
    # Validate model-level attributes
    assert model.start == "2025-01-01"
    assert model.interval_unit.is_day
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    # `on_schema_change` is ignored since the user explicitly overrode the values
    assert model.kind.on_destructive_change == OnDestructiveChange.ALLOW
    assert model.kind.on_additive_change == OnAdditiveChange.ERROR
    assert model.kind.forward_only is True
    assert model.kind.disable_restatement is True
    assert model.kind.auto_restatement_cron == "@hourly"
    assert model.kind.auto_restatement_intervals == 3
    assert model.kind.partition_by_time_column is False
    assert model.kind.lookback == 5
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("ds", quoted=True), format="%Y%m%d"
    )
    assert model.kind.batch_size == 3
    assert model.kind.batch_concurrency == 2
    assert model.depends_on_self is False
    # make sure the snapshot can be serialized to json
    assert isinstance(_snapshot_to_json(snapshot), str)


@pytest.mark.slow
def test_load_deprecated_incremental_time_column(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    project_dir, model_dir = create_empty_project()
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    incremental_time_range_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            time_column='ds'
        )
    }}

    SELECT 1 as cola, '2021-01-01' as ds
    """
    incremental_time_range_model_file = model_dir / "incremental_time_range.sql"
    with open(incremental_time_range_model_file, "w", encoding="utf-8") as f:
        f.write(incremental_time_range_contents)

    snapshot_fqn = '"local"."main"."incremental_time_range"'
    context = Context(paths=project_dir)
    model = context.snapshots[snapshot_fqn].model
    # Validate model-level attributes
    assert model.start == "2025-01-01"
    assert model.interval_unit.is_day
    # Validate model-level attributes
    assert model.start == "2025-01-01"
    assert model.interval_unit.is_day
    # Validate model kind attributes
    assert isinstance(model.kind, IncrementalByTimeRangeKind)
    assert model.kind.lookback == 1
    assert model.kind.time_column == TimeColumn(
        column=exp.to_column("ds", quoted=True), format="%Y-%m-%d"
    )
    assert model.kind.batch_size is None
    assert model.depends_on_self is False
    assert model.kind.auto_restatement_intervals is None
    assert model.kind.partition_by_time_column is True
    assert (
        "Using `time_column` on a model with incremental_strategy 'delete+insert' has been deprecated. Please use `incremental_by_time_range` instead in model 'main.incremental_time_range'."
        in caplog.text
    )


@pytest.mark.slow
def test_load_microbatch_with_ref(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    yaml = YAML()
    project_dir, model_dir = create_empty_project()
    source_schema = {
        "version": 2,
        "sources": [
            {
                "name": "my_source",
                "tables": [{"name": "my_table", "config": {"event_time": "ds_source"}}],
            }
        ],
    }
    source_schema_file = model_dir / "source_schema.yml"
    with open(source_schema_file, "w", encoding="utf-8") as f:
        yaml.dump(source_schema, f)
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    microbatch_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ds',
            begin='2020-01-01',
            batch_size='day'
        )
    }}

    SELECT cola, ds_source as ds FROM {{ source('my_source', 'my_table') }}
    """
    microbatch_model_file = model_dir / "microbatch.sql"
    with open(microbatch_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_contents)

    microbatch_two_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ds',
            begin='2020-01-05',
            batch_size='day'
        )
    }}

    SELECT cola, ds FROM {{ ref('microbatch') }}
    """
    microbatch_two_model_file = model_dir / "microbatch_two.sql"
    with open(microbatch_two_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_two_contents)

    microbatch_snapshot_fqn = '"local"."main"."microbatch"'
    microbatch_two_snapshot_fqn = '"local"."main"."microbatch_two"'
    context = Context(paths=project_dir)
    assert (
        context.render(microbatch_snapshot_fqn, start="2025-01-01", end="2025-01-10").sql()
        == 'SELECT "cola" AS "cola", "ds_source" AS "ds" FROM (SELECT * FROM "local"."my_source"."my_table" AS "my_table" WHERE "ds_source" >= \'2025-01-01 00:00:00+00:00\' AND "ds_source" < \'2025-01-11 00:00:00+00:00\') AS "_q_0"'
    )
    assert (
        context.render(microbatch_two_snapshot_fqn, start="2025-01-01", end="2025-01-10").sql()
        == 'SELECT "_q_0"."cola" AS "cola", "_q_0"."ds" AS "ds" FROM (SELECT "microbatch"."cola" AS "cola", "microbatch"."ds" AS "ds" FROM "local"."main"."microbatch" AS "microbatch" WHERE "microbatch"."ds" < \'2025-01-11 00:00:00+00:00\' AND "microbatch"."ds" >= \'2025-01-01 00:00:00+00:00\') AS "_q_0"'
    )


@pytest.mark.slow
def test_load_microbatch_with_ref_no_filter(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig, create_empty_project
) -> None:
    yaml = YAML()
    project_dir, model_dir = create_empty_project()
    source_schema = {
        "version": 2,
        "sources": [
            {
                "name": "my_source",
                "tables": [{"name": "my_table", "config": {"event_time": "ds"}}],
            }
        ],
    }
    source_schema_file = model_dir / "source_schema.yml"
    with open(source_schema_file, "w", encoding="utf-8") as f:
        yaml.dump(source_schema, f)
    # add `tests` to model config since this is loaded by dbt and ignored and we shouldn't error when loading it
    microbatch_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ds',
            begin='2020-01-01',
            batch_size='day'
        )
    }}

    SELECT cola, ds FROM {{ source('my_source', 'my_table').render() }}
    """
    microbatch_model_file = model_dir / "microbatch.sql"
    with open(microbatch_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_contents)

    microbatch_two_contents = """
    {{
        config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ds',
            begin='2020-01-01',
            batch_size='day'
        )
    }}

    SELECT cola, ds FROM {{ ref('microbatch').render() }}
    """
    microbatch_two_model_file = model_dir / "microbatch_two.sql"
    with open(microbatch_two_model_file, "w", encoding="utf-8") as f:
        f.write(microbatch_two_contents)

    microbatch_snapshot_fqn = '"local"."main"."microbatch"'
    microbatch_two_snapshot_fqn = '"local"."main"."microbatch_two"'
    context = Context(paths=project_dir)
    assert (
        context.render(microbatch_snapshot_fqn, start="2025-01-01", end="2025-01-10").sql()
        == 'SELECT "cola" AS "cola", "ds" AS "ds" FROM "local"."my_source"."my_table" AS "my_table"'
    )
    assert (
        context.render(microbatch_two_snapshot_fqn, start="2025-01-01", end="2025-01-10").sql()
        == 'SELECT "microbatch"."cola" AS "cola", "microbatch"."ds" AS "ds" FROM "local"."main"."microbatch" AS "microbatch"'
    )


@pytest.mark.slow
def test_dbt_jinja_macro_undefined_variable_error(create_empty_project):
    project_dir, model_dir = create_empty_project()

    dbt_profile_config = {
        "test": {
            "outputs": {
                "duckdb": {
                    "type": "duckdb",
                    "path": str(project_dir.parent / "dbt_data" / "main.db"),
                }
            },
            "target": "duckdb",
        }
    }
    db_profile_file = project_dir / "profiles.yml"
    with open(db_profile_file, "w", encoding="utf-8") as f:
        YAML().dump(dbt_profile_config, f)

    macros_dir = project_dir / "macros"
    macros_dir.mkdir()

    # the execute guard in the macro is so that dbt won't fail on the manifest loading earlier
    macro_file = macros_dir / "my_macro.sql"
    macro_file.write_text("""
{%- macro select_columns(table_name) -%}
  {% if execute %}
    {%- if target.name == 'production' -%}
        {%- set columns = run_query('SELECT column_name FROM information_schema.columns WHERE table_name = \'' ~ table_name ~ '\'') -%}
    {%- endif -%}
    SELECT {{ columns.rows[0][0] }} FROM {{ table_name }}
  {%- endif -%}
{%- endmacro -%}
""")

    model_file = model_dir / "my_model.sql"
    model_file.write_text("""
{{ config(
    materialized='table'
) }}

{{ select_columns('users') }}
""")

    with pytest.raises(SchemaError) as exc_info:
        Context(paths=project_dir)

    error_message = str(exc_info.value)
    assert "Failed to update model schemas" in error_message
    assert "Could not render jinja for" in error_message
    assert "Undefined macro/variable: 'columns' in macro: 'select_columns'" in error_message
