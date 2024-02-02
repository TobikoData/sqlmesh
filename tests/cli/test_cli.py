from shutil import rmtree

import pytest
from click.testing import CliRunner

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.cli.main import cli


@pytest.fixture(scope="session")
def runner():
    return CliRunner()


@pytest.fixture
def example_project_fixture(tmp_path):
    """
    Sets up CLI tests requiring a real SQLMesh project by:
        - Creating the SQLMesh example project in the tmp_path directory
        - Overwriting the config.yaml file so the duckdb database file will be created in the tmp_path directory
        - Returning the tmp_path directory
        - Cleaning up the tmp_path directory after the test
    """
    init_example_project(tmp_path, "duckdb")
    with open(tmp_path / "config.yaml", "w") as f:
        f.write(
            f"""gateways:
    local:
        connection:
            type: duckdb
            database: {tmp_path}/db.db

default_gateway: local

model_defaults:
    dialect: duckdb
"""
        )
    yield tmp_path
    rmtree(tmp_path)


def test_plan_no_config(runner, tmp_path):
    result = runner.invoke(cli, ["--paths", tmp_path, "plan"])
    assert result.exit_code == 1
    assert "Error: SQLMesh project config could not be found" in result.output


def test_plan(runner, example_project_fixture):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--paths", example_project_fixture, "plan", "--no-prompts", "--auto-apply"]
    )
    assert result.exit_code == 0
