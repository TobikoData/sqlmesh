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


def test_test_config_is_standalone_behavior() -> None:
    """Test that TestConfig.is_standalone correctly identifies tests with cross-model references"""

    # Test with no model_name (should be standalone)
    standalone_test = TestConfig(
        name="standalone_test",
        sql="SELECT 1",
        model_name=None,
        dependencies=Dependencies(refs={"some_model"}),
    )
    assert standalone_test.is_standalone is True

    # Test with only self-reference (should not be standalone)
    self_ref_test = TestConfig(
        name="self_ref_test",
        sql="SELECT * FROM {{ this }}",
        model_name="my_model",
        dependencies=Dependencies(refs={"my_model"}),
    )
    assert self_ref_test.is_standalone is False

    # Test with no references (should not be standalone)
    no_ref_test = TestConfig(
        name="no_ref_test",
        sql="SELECT 1",
        model_name="my_model",
        dependencies=Dependencies(),
    )
    assert no_ref_test.is_standalone is False

    # Test with references to other models (should be standalone)
    cross_ref_test = TestConfig(
        name="cross_ref_test",
        sql="SELECT * FROM {{ ref('other_model') }}",
        model_name="my_model",
        dependencies=Dependencies(refs={"my_model", "other_model"}),
    )
    assert cross_ref_test.is_standalone is True

    # Test with only references to other models, no self-reference (should be standalone)
    other_only_test = TestConfig(
        name="other_only_test",
        sql="SELECT * FROM {{ ref('other_model') }}",
        model_name="my_model",
        dependencies=Dependencies(refs={"other_model"}),
    )
    assert other_only_test.is_standalone is True


def test_test_to_sqlmesh_creates_correct_audit_type(
    dbt_dummy_postgres_config: PostgresConfig,
) -> None:
    """Test that TestConfig.to_sqlmesh creates the correct audit type based on is_standalone"""
    from sqlmesh.core.audit.definition import StandaloneAudit, ModelAudit

    # Set up models in context
    my_model = ModelConfig(
        name="my_model", sql="SELECT 1", schema="test_schema", database="test_db", alias="my_model"
    )
    other_model = ModelConfig(
        name="other_model",
        sql="SELECT 2",
        schema="test_schema",
        database="test_db",
        alias="other_model",
    )
    context = DbtContext(
        _refs={"my_model": my_model, "other_model": other_model},
        _target=dbt_dummy_postgres_config,
    )

    # Test with only self-reference (should create ModelAudit)
    self_ref_test = TestConfig(
        name="self_ref_test",
        sql="SELECT * FROM {{ this }}",
        model_name="my_model",
        dependencies=Dependencies(refs={"my_model"}),
    )
    audit = self_ref_test.to_sqlmesh(context)
    assert isinstance(audit, ModelAudit)
    assert audit.name == "self_ref_test"

    # Test with references to other models (should create StandaloneAudit)
    cross_ref_test = TestConfig(
        name="cross_ref_test",
        sql="SELECT * FROM {{ ref('other_model') }}",
        model_name="my_model",
        dependencies=Dependencies(refs={"my_model", "other_model"}),
    )
    audit = cross_ref_test.to_sqlmesh(context)
    assert isinstance(audit, StandaloneAudit)
    assert audit.name == "cross_ref_test"

    # Test with no model_name (should create StandaloneAudit)
    standalone_test = TestConfig(
        name="standalone_test",
        sql="SELECT 1",
        model_name=None,
        dependencies=Dependencies(),
    )
    audit = standalone_test.to_sqlmesh(context)
    assert isinstance(audit, StandaloneAudit)
    assert audit.name == "standalone_test"


@pytest.mark.slow
def test_manifest_filters_standalone_tests_from_models(
    tmp_path: Path, create_empty_project
) -> None:
    """Integration test that verifies models only contain non-standalone tests after manifest loading."""
    yaml = YAML()
    project_dir, model_dir = create_empty_project()

    # Create two models
    model1_contents = "SELECT 1 as id"
    model1_file = model_dir / "model1.sql"
    with open(model1_file, "w", encoding="utf-8") as f:
        f.write(model1_contents)

    model2_contents = "SELECT 2 as id"
    model2_file = model_dir / "model2.sql"
    with open(model2_file, "w", encoding="utf-8") as f:
        f.write(model2_contents)

    # Create schema with both standalone and non-standalone tests
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": "model1",
                "columns": [
                    {
                        "name": "id",
                        "tests": [
                            "not_null",  # Non-standalone test - only references model1
                            {
                                "relationships": {  # Standalone test - references model2
                                    "to": "ref('model2')",
                                    "field": "id",
                                }
                            },
                        ],
                    }
                ],
            },
            {
                "name": "model2",
                "columns": [
                    {"name": "id", "tests": ["not_null"]}  # Non-standalone test
                ],
            },
        ],
    }

    schema_file = model_dir / "schema.yml"
    with open(schema_file, "w", encoding="utf-8") as f:
        yaml.dump(schema_yaml, f)

    # Load the project through SQLMesh Context
    from sqlmesh.core.context import Context

    context = Context(paths=project_dir)

    model1_snapshot = context.snapshots['"local"."main"."model1"']
    model2_snapshot = context.snapshots['"local"."main"."model2"']

    # Verify model1 only has non-standalone test in its audits
    # Should only have "not_null" test, not the "relationships" test
    model1_audit_names = [audit[0] for audit in model1_snapshot.model.audits]
    assert len(model1_audit_names) == 1
    assert model1_audit_names[0] == "not_null_model1_id"

    # Verify model2 has its non-standalone test
    model2_audit_names = [audit[0] for audit in model2_snapshot.model.audits]
    assert len(model2_audit_names) == 1
    assert model2_audit_names[0] == "not_null_model2_id"

    # Verify the standalone test (relationships) exists as a StandaloneAudit
    all_non_standalone_audits = [name for name in context._audits]
    assert sorted(all_non_standalone_audits) == [
        "not_null_model1_id",
        "not_null_model2_id",
    ]

    standalone_audits = [name for name in context._standalone_audits]
    assert len(standalone_audits) == 1
    assert standalone_audits[0] == "relationships_model1_id__id__ref_model2_"

    plan_builder = context.plan_builder()
    dag = plan_builder._build_dag()
    assert [x.name for x in dag.sorted] == [
        '"local"."main"."model1"',
        '"local"."main"."model2"',
        "relationships_model1_id__id__ref_model2_",
    ]


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
def test_load_multiple_snapshots_defined_in_same_file(sushi_test_dbt_context: Context) -> None:
    context = sushi_test_dbt_context
    assert context.get_model("snapshots.items_snapshot")
    assert context.get_model("snapshots.items_check_snapshot")

    # Make sure cache works too
    context.load()
    assert context.get_model("snapshots.items_snapshot")
    assert context.get_model("snapshots.items_check_snapshot")


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


@pytest.mark.slow
def test_node_name_populated_for_dbt_models(dbt_dummy_postgres_config: PostgresConfig) -> None:
    model_config = ModelConfig(
        name="test_model",
        package_name="test_package",
        sql="SELECT 1 as id",
        database="test_db",
        schema_="test_schema",
        alias="test_model",
    )

    context = DbtContext()
    context.project_name = "test_project"
    context.target = dbt_dummy_postgres_config

    # check after convert to SQLMesh model that node_name is populated correctly
    sqlmesh_model = model_config.to_sqlmesh(context)
    assert sqlmesh_model.dbt_name == "model.test_package.test_model"


@pytest.mark.slow
def test_load_model_dbt_node_name(tmp_path: Path) -> None:
    yaml = YAML()
    dbt_project_dir = tmp_path / "dbt"
    dbt_project_dir.mkdir()
    dbt_model_dir = dbt_project_dir / "models"
    dbt_model_dir.mkdir()

    model_contents = "SELECT 1 as id, 'test' as name"
    model_file = dbt_model_dir / "simple_model.sql"
    with open(model_file, "w", encoding="utf-8") as f:
        f.write(model_contents)

    dbt_project_config = {
        "name": "test_project",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test",
        "model-paths": ["models"],
    }
    dbt_project_file = dbt_project_dir / "dbt_project.yml"
    with open(dbt_project_file, "w", encoding="utf-8") as f:
        yaml.dump(dbt_project_config, f)

    sqlmesh_config = {
        "model_defaults": {
            "start": "2025-01-01",
        }
    }
    sqlmesh_config_file = dbt_project_dir / "sqlmesh.yaml"
    with open(sqlmesh_config_file, "w", encoding="utf-8") as f:
        yaml.dump(sqlmesh_config, f)

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

    context = Context(paths=dbt_project_dir)

    # find the model by its sqlmesh fully qualified name
    model_fqn = '"local"."main"."simple_model"'
    assert model_fqn in context.snapshots

    # Verify that node_name is the equivalent dbt one
    model = context.snapshots[model_fqn].model
    assert model.dbt_name == "model.test_project.simple_model"
