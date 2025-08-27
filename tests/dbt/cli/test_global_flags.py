import typing as t
from pathlib import Path
import pytest
from click.testing import Result
from unittest.mock import patch
from sqlmesh.utils.errors import SQLMeshError
from sqlglot.errors import SqlglotError

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


def test_run_error_handler(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    with patch("sqlmesh_dbt.operations.DbtOperations.run") as mock_run:
        mock_run.side_effect = SQLMeshError("Test error message")

        result = invoke_cli(["run"])

        # tesg that SQLMeshError are handled gracefully in run command
        assert result.exit_code == 1
        assert "Traceback" not in result.output

    with patch("sqlmesh_dbt.operations.DbtOperations.run") as mock_run:
        mock_run.side_effect = SqlglotError("Invalid SQL syntax")

        result = invoke_cli(["run"])

        assert result.exit_code == 1
        assert "Error: Invalid SQL syntax" in result.output
        assert "Traceback" not in result.output

    with patch("sqlmesh_dbt.operations.DbtOperations.run") as mock_run:
        mock_run.side_effect = ValueError("Invalid configuration value")

        result = invoke_cli(["run"])

        assert result.exit_code == 1
        assert "Error: Invalid configuration value" in result.output
        assert "Traceback" not in result.output

    with patch("sqlmesh_dbt.operations.DbtOperations.list_") as mock_list:
        mock_list.side_effect = SQLMeshError("List command error")

        result = invoke_cli(["list"])

        assert result.exit_code == 1
        assert "Error: List command error" in result.output
        assert "Traceback" not in result.output

    with patch("sqlmesh_dbt.cli.create") as mock_create:
        mock_create.side_effect = SQLMeshError("Failed to load project")

        # use without subcommand
        result = invoke_cli(["--profile", "jaffle_shop"])

        assert result.exit_code == 1
        assert "Error: Failed to load project" in result.output
        assert "Traceback" not in result.output

    with patch("sqlmesh_dbt.operations.DbtOperations.run") as mock_run:
        mock_run.side_effect = SQLMeshError("Error with selector")

        # with select option
        result = invoke_cli(["run", "--select", "model1"])

        assert result.exit_code == 1
        assert "Error: Error with selector" in result.output
        assert "Traceback" not in result.output
