import typing as t
from pathlib import Path
import pytest
import logging
from pytest_mock import MockerFixture
from click.testing import Result
from sqlmesh.utils.errors import SQLMeshError
from sqlglot.errors import SqlglotError
from tests.dbt.conftest import EmptyProjectCreator

pytestmark = pytest.mark.slow


def test_profile_and_target(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    # profile doesnt exist - error
    result = invoke_cli(["--profile", "nonexist"])
    assert result.exit_code == 1
    assert "Profile 'nonexist' not found in profiles" in result.output

    # profile exists - successful load with default target
    result = invoke_cli(["--profile", "jaffle_shop"])
    assert result.exit_code == 0
    assert "No command specified" in result.output

    # profile exists but target doesnt - error
    result = invoke_cli(["--profile", "jaffle_shop", "--target", "nonexist"])
    assert result.exit_code == 1
    assert "Target 'nonexist' not specified in profiles" in result.output
    assert "valid target names for this profile are" in result.output
    assert "- dev" in result.output

    # profile exists and so does target - successful load with specified target
    result = invoke_cli(["--profile", "jaffle_shop", "--target", "dev"])
    assert result.exit_code == 0
    assert "No command specified" in result.output


def test_run_error_handler(
    jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result], mocker: MockerFixture
) -> None:
    mock_run = mocker.patch("sqlmesh_dbt.operations.DbtOperations.run")
    mock_run.side_effect = SQLMeshError("Test error message")

    result = invoke_cli(["run"])
    assert result.exit_code == 1
    assert "Error: Test error message" in result.output
    assert "Traceback" not in result.output

    # test SqlglotError in run command
    mock_run = mocker.patch("sqlmesh_dbt.operations.DbtOperations.run")
    mock_run.side_effect = SqlglotError("Invalid SQL syntax")

    result = invoke_cli(["run"])

    assert result.exit_code == 1
    assert "Error: Invalid SQL syntax" in result.output
    assert "Traceback" not in result.output

    # test ValueError in run command
    mock_run = mocker.patch("sqlmesh_dbt.operations.DbtOperations.run")
    mock_run.side_effect = ValueError("Invalid configuration value")

    result = invoke_cli(["run"])

    assert result.exit_code == 1
    assert "Error: Invalid configuration value" in result.output
    assert "Traceback" not in result.output

    # test SQLMeshError in list command
    mock_list = mocker.patch("sqlmesh_dbt.operations.DbtOperations.list_")
    mock_list.side_effect = SQLMeshError("List command error")

    result = invoke_cli(["list"])

    assert result.exit_code == 1
    assert "Error: List command error" in result.output
    assert "Traceback" not in result.output

    # test SQLMeshError in main command without subcommand
    mock_create = mocker.patch("sqlmesh_dbt.cli.create")
    mock_create.side_effect = SQLMeshError("Failed to load project")
    result = invoke_cli(["--profile", "jaffle_shop"])

    assert result.exit_code == 1
    assert "Error: Failed to load project" in result.output
    assert "Traceback" not in result.output
    mocker.stopall()

    # test error with select option
    mock_run_select = mocker.patch("sqlmesh_dbt.operations.DbtOperations.run")
    mock_run_select.side_effect = SQLMeshError("Error with selector")

    result = invoke_cli(["run", "--select", "model1"])

    assert result.exit_code == 1
    assert "Error: Error with selector" in result.output
    assert "Traceback" not in result.output


def test_log_level(invoke_cli: t.Callable[..., Result], create_empty_project: EmptyProjectCreator):
    create_empty_project()

    result = invoke_cli(["--log-level", "info", "list"])
    assert result.exit_code == 0
    assert logging.getLogger("sqlmesh").getEffectiveLevel() == logging.INFO

    result = invoke_cli(["--log-level", "debug", "list"])
    assert result.exit_code == 0
    assert logging.getLogger("sqlmesh").getEffectiveLevel() == logging.DEBUG


def test_profiles_dir(
    invoke_cli: t.Callable[..., Result], create_empty_project: EmptyProjectCreator, tmp_path: Path
):
    project_dir, _ = create_empty_project(project_name="test_profiles_dir")

    orig_profiles_yml = project_dir / "profiles.yml"
    assert orig_profiles_yml.exists()

    new_profiles_yml = tmp_path / "some_other_place" / "profiles.yml"
    new_profiles_yml.parent.mkdir(parents=True)

    orig_profiles_yml.rename(new_profiles_yml)
    assert not orig_profiles_yml.exists()
    assert new_profiles_yml.exists()

    # should fail if we don't specify --profiles-dir
    result = invoke_cli(["list"])
    assert result.exit_code > 0, result.output

    # alternative ~/.dbt/profiles.yml might exist but doesn't contain the profile
    assert "profiles.yml not found" in result.output or "not found in profiles" in result.output

    # should pass if we specify --profiles-dir
    result = invoke_cli(["--profiles-dir", str(new_profiles_yml.parent), "list"])
    assert result.exit_code == 0, result.output
    assert "Models in project" in result.output


def test_project_dir(
    invoke_cli: t.Callable[..., Result], create_empty_project: EmptyProjectCreator
):
    orig_project_dir, _ = create_empty_project(project_name="test_project_dir")

    orig_project_yml = orig_project_dir / "dbt_project.yml"
    assert orig_project_yml.exists()

    new_project_yml = orig_project_dir / "nested" / "dbt_project.yml"
    new_project_yml.parent.mkdir(parents=True)

    orig_project_yml.rename(new_project_yml)
    assert not orig_project_yml.exists()
    assert new_project_yml.exists()

    # should fail if we don't specify --project-dir
    result = invoke_cli(["list"])
    assert result.exit_code != 0, result.output
    assert "Error:" in result.output

    # should fail if the profiles.yml also doesnt exist at that --project-dir
    result = invoke_cli(["--project-dir", str(new_project_yml.parent), "list"])
    assert result.exit_code != 0, result.output

    # profiles.yml might exist but doesn't contain the profile
    assert "profiles.yml not found" in result.output or "not found in profiles" in result.output

    # should pass if it can find both files, either because we specified --profiles-dir explicitly or the profiles.yml was found in --project-dir
    result = invoke_cli(
        [
            "--project-dir",
            str(new_project_yml.parent),
            "--profiles-dir",
            str(orig_project_dir),
            "list",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "Models in project" in result.output

    orig_profiles_yml = orig_project_dir / "profiles.yml"
    new_profiles_yml = new_project_yml.parent / "profiles.yml"
    assert orig_profiles_yml.exists()
    orig_profiles_yml.rename(new_profiles_yml)

    result = invoke_cli(["--project-dir", str(new_project_yml.parent), "list"])
    assert result.exit_code == 0, result.output
    assert "Models in project" in result.output
