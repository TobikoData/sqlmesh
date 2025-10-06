from pathlib import Path

import pytest

from sqlmesh.dbt.test import TestConfig


def test_multiline_test_kwarg() -> None:
    test = TestConfig(
        name="test",
        sql="{{ test(**_dbt_generic_test_kwargs) }}",
        test_kwargs={"test_field": "foo\nbar\n"},
    )
    assert test._kwargs() == 'test_field="foo\nbar"'


@pytest.mark.xdist_group("dbt_manifest")
def test_tests_get_unique_names(tmp_path: Path, create_empty_project) -> None:
    from sqlmesh.utils.yaml import YAML
    from sqlmesh.core.context import Context

    yaml = YAML()
    project_dir, model_dir = create_empty_project(project_name="local")

    model_file = model_dir / "my_model.sql"
    with open(model_file, "w", encoding="utf-8") as f:
        f.write("SELECT 1 as id, 'value1' as status")

    # Create schema.yml with:
    # 1. Same test on model and source, both with/without custom test name
    # 2. Same test on same model with different args, both with/without custom test name
    # 3. Versioned model with tests (both built-in and custom named)
    schema_yaml = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "tables": [
                    {
                        "name": "my_source",
                        "columns": [
                            {
                                "name": "id",
                                "data_tests": [
                                    {"not_null": {"name": "custom_notnull_name"}},
                                    {"not_null": {}},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        "models": [
            {
                "name": "my_model",
                "columns": [
                    {
                        "name": "id",
                        "data_tests": [
                            {"not_null": {"name": "custom_notnull_name"}},
                            {"not_null": {}},
                        ],
                    },
                    {
                        "name": "status",
                        "data_tests": [
                            {"accepted_values": {"values": ["value1", "value2"]}},
                            {"accepted_values": {"values": ["value1", "value2", "value3"]}},
                            {
                                "accepted_values": {
                                    "name": "custom_accepted_values_name",
                                    "values": ["value1", "value2"],
                                }
                            },
                            {
                                "accepted_values": {
                                    "name": "custom_accepted_values_name",
                                    "values": ["value1", "value2", "value3"],
                                }
                            },
                        ],
                    },
                ],
            },
            {
                "name": "versioned_model",
                "columns": [
                    {
                        "name": "id",
                        "data_tests": [
                            {"not_null": {}},
                            {"not_null": {"name": "custom_versioned_notnull"}},
                        ],
                    },
                    {
                        "name": "amount",
                        "data_tests": [
                            {"accepted_values": {"values": ["low", "high"]}},
                        ],
                    },
                ],
                "versions": [
                    {"v": 1},
                    {"v": 2},
                ],
            },
        ],
    }

    schema_file = model_dir / "schema.yml"
    with open(schema_file, "w", encoding="utf-8") as f:
        yaml.dump(schema_yaml, f)

    # Create versioned model files
    versioned_model_v1_file = model_dir / "versioned_model_v1.sql"
    with open(versioned_model_v1_file, "w", encoding="utf-8") as f:
        f.write("SELECT 1 as id, 'low' as amount")

    versioned_model_v2_file = model_dir / "versioned_model_v2.sql"
    with open(versioned_model_v2_file, "w", encoding="utf-8") as f:
        f.write("SELECT 1 as id, 'low' as amount")

    context = Context(paths=project_dir)

    all_audit_names = list(context._audits.keys()) + list(context._standalone_audits.keys())
    assert sorted(all_audit_names) == [
        "local.accepted_values_my_model_status__value1__value2",
        "local.accepted_values_my_model_status__value1__value2__value3",
        "local.accepted_values_versioned_model_v1_amount__low__high",
        "local.accepted_values_versioned_model_v2_amount__low__high",
        "local.custom_accepted_values_name_my_model_status__value1__value2",
        "local.custom_accepted_values_name_my_model_status__value1__value2__value3",
        "local.custom_notnull_name_my_model_id",
        "local.custom_versioned_notnull_versioned_model_v1_id",
        "local.custom_versioned_notnull_versioned_model_v2_id",
        "local.not_null_my_model_id",
        "local.not_null_versioned_model_v1_id",
        "local.not_null_versioned_model_v2_id",
        "local.source_custom_notnull_name_raw_my_source_id",
        "local.source_not_null_raw_my_source_id",
    ]
