import os
from shutil import rmtree

import pytest
from click.testing import CliRunner

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.cli.main import cli


@pytest.fixture(scope="session")
def runner() -> CliRunner:
    return CliRunner()


def create_example_project(temp_dir) -> None:
    """
    Sets up CLI tests requiring a real SQLMesh project by:
        - Creating the SQLMesh example project in the temp_dir directory
        - Overwriting the config.yaml file so the duckdb database file will be created in the temp_dir directory
    """
    init_example_project(temp_dir, "duckdb")
    with open(temp_dir / "config.yaml", "w") as f:
        f.write(
            f"""gateways:
    local:
        connection:
            type: duckdb
            database: {temp_dir}/db.db

default_gateway: local

model_defaults:
    dialect: duckdb
"""
        )


def reset_example_project(temp_dir) -> None:
    rmtree(temp_dir / ".cache")
    os.unlink(temp_dir / "db.db")


def assert_duckdb_test(result) -> None:
    assert "Successfully Ran 1 tests against duckdb" in result.output


def assert_new_env(result, new_env="prod", from_env="prod") -> None:
    assert f"New environment `{new_env}` will be created from `{from_env}`" in result.output


def assert_backfill_success(result) -> None:
    assert "All model versions have been created successfully" in result.output
    assert "All model batches have been executed successfully" in result.output
    assert "The target environment has been updated successfully" in result.output


def assert_plan_success(result, new_env="prod", from_env="prod") -> None:
    assert result.exit_code == 0
    assert_duckdb_test(result)
    assert_new_env(result, new_env, from_env)
    assert_backfill_success(result)


def test_plan(runner, tmp_path):
    # Error if no SQLMesh project config is found
    result = runner.invoke(cli, ["--paths", tmp_path, "plan"])
    assert result.exit_code == 1
    assert "Error: SQLMesh project config could not be found" in result.output

    create_example_project(tmp_path)

    # Example project models have start dates, so there are no date prompts
    # for the `prod` environment. User input `y` is required to apply the plan.
    result = runner.invoke(cli, ["--paths", tmp_path, "plan"], input="y\n")
    assert_plan_success(result)
    reset_example_project(tmp_path)

    # Example project plan for `prod` runs end-to-end with no user input if `--auto-apply` is passed
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "--auto-apply"])
    assert_plan_success(result)
    reset_example_project(tmp_path)

    # Successful test run message should not appear if `--skip-tests` is passed and plan is applied
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "--skip-tests"], input="y\n")
    assert result.exit_code == 0
    assert "Successfully Ran 1 tests against duckdb" not in result.output
    assert_new_env(result)
    assert_backfill_success(result)

    rmtree(tmp_path)


def test_plan_dev(runner, tmp_path):
    create_example_project(tmp_path)

    # Example project plan for non-prod environment has both start/end and apply prompts
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "dev"], input="\n\ny\n")
    assert_plan_success(result, "dev")
    reset_example_project(tmp_path)

    # Example project plan for non-prod environment doesn't backfill if only `--no-prompts` is passed
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "dev", "--no-prompts"])
    assert result.exit_code == 0
    assert_duckdb_test(result)
    assert_new_env(result, "dev")
    assert "All model versions have been created successfully" not in result.output
    assert "All model batches have been executed successfully" not in result.output
    assert "The target environment has been updated successfully" not in result.output
    reset_example_project(tmp_path)

    # Example project plan for non-prod environment has start/end prompts if only `--auto-apply` is passed
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "dev", "--auto-apply"], input="\n\n")
    assert_plan_success(result, "dev")

    rmtree(tmp_path)


def test_plan_dev_no_changes(runner, tmp_path):
    create_example_project(tmp_path)

    # Create and backfill `prod` environment
    runner.invoke(cli, ["--paths", tmp_path, "plan", "--auto-apply"])

    # Error if no changes made and `--include-unmodified` is not passed
    result = runner.invoke(cli, ["--paths", tmp_path, "plan", "dev"])
    assert result.exit_code == 1
    assert (
        "Error: No changes were detected. Make a change or run with --include-unmodified"
        in result.output
    )

    # No error if no changes made and `--include-unmodified` is passed
    result = runner.invoke(
        cli, ["--paths", tmp_path, "plan", "dev", "--include-unmodified"], input="y\n"
    )
    assert result.exit_code == 0
    assert_new_env(result, "dev")
    assert "The target environment has been updated successfully" in result.output
    assert "Virtual Update executed successfully" in result.output

    rmtree(tmp_path)


def test_run(runner, tmp_path):
    create_example_project(tmp_path)

    # Error if no env specified and `prod` doesn't exist
    result = runner.invoke(cli, ["--paths", tmp_path, "run"])
    assert result.exit_code == 1
    assert "Error: Environment 'prod' was not found." in result.output

    # Create dev environment but DO NOT backfill
    runner.invoke(cli, ["--paths", tmp_path, "plan", "dev", "--skip-backfill"], input="y\n")

    # Confirm backfill occurs when we `run` non-backfilled `dev` env
    result = runner.invoke(cli, ["--paths", tmp_path, "run", "dev"])
    assert result.exit_code == 0
    assert "All model batches have been executed successfully" in result.output

    # Create and backfill `prod` environment
    runner.invoke(cli, ["--paths", tmp_path, "plan", "--no-prompts", "--auto-apply"])

    # No error and no output if `prod` environment exists and cron has not elapsed
    result = runner.invoke(cli, ["--paths", tmp_path, "run"])
    assert result.exit_code == 0
    assert result.output == ""

    rmtree(tmp_path)
