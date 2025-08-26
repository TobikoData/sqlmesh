import pytest

from pathlib import Path

from sqlmesh import Context
from sqlmesh.dbt.common import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.target import PostgresConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import YAML

pytestmark = pytest.mark.dbt


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
    downstream_model.tests = [downstream_test]
    downstream_model.check_for_circular_test_refs(context)

    downstream_model.tests = []
    upstream_model.tests = [upstream_test]
    with pytest.raises(ConfigError, match="downstream model"):
        upstream_model.check_for_circular_test_refs(context)

    downstream_model.tests = [downstream_test]
    with pytest.raises(ConfigError, match="downstream model"):
        upstream_model.check_for_circular_test_refs(context)
    downstream_model.check_for_circular_test_refs(context)

    # Test only references
    downstream_model.dependencies = Dependencies()
    with pytest.raises(ConfigError, match="between tests"):
        upstream_model.check_for_circular_test_refs(context)
    with pytest.raises(ConfigError, match="between tests"):
        downstream_model.check_for_circular_test_refs(context)


@pytest.mark.slow
def test_load_invalid_ref_audit_constraints(
    tmp_path: Path, caplog, dbt_dummy_postgres_config: PostgresConfig
) -> None:
    yaml = YAML()
    dbt_project_dir = tmp_path / "dbt"
    dbt_project_dir.mkdir()
    dbt_model_dir = dbt_project_dir / "models"
    dbt_model_dir.mkdir()
    full_model_contents = "SELECT 1 as cola"
    full_model_file = dbt_model_dir / "full_model.sql"
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
                                }
                            }
                        ],
                    }
                ],
            }
        ],
    }
    model_schema_file = dbt_model_dir / "schema.yml"
    with open(model_schema_file, "w", encoding="utf-8") as f:
        yaml.dump(model_schema, f)
    dbt_project_config = {
        "name": "invalid_ref_audit_constraints",
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
    assert (
        "Skipping audit 'relationships_full_model_cola__cola__ref_not_real_model_' because model 'not_real_model' is not a valid ref"
        in caplog.text
    )
    fqn = '"local"."main"."full_model"'
    assert fqn in context.snapshots
    # The audit isn't loaded due to the invalid ref
    assert context.snapshots[fqn].model.audits == []
