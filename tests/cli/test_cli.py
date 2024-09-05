import logging
from contextlib import contextmanager
from os import path
from pathlib import Path

import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.cli.main import cli
from sqlmesh.core.context import Context

FREEZE_TIME = "2023-01-01 00:00:00"

pytestmark = pytest.mark.slow


@pytest.fixture(scope="session")
def runner() -> CliRunner:
    return CliRunner()


@contextmanager
def disable_logging():
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)


def create_example_project(temp_dir) -> None:
    """
    Sets up CLI tests requiring a real SQLMesh project by:
        - Creating the SQLMesh example project in the temp_dir directory
        - Overwriting the config.yaml file so the duckdb database file will be created in the temp_dir directory
    """
    init_example_project(temp_dir, "duckdb")
    with open(temp_dir / "config.yaml", "w", encoding="utf-8") as f:
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


def update_incremental_model(temp_dir) -> None:
    with open(temp_dir / "models" / "incremental_model.sql", "w", encoding="utf-8") as f:
        f.write(
            """
MODEL (
  name sqlmesh_example.incremental_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  id,
  item_id,
  'a' as new_col,
  event_date,
FROM
  sqlmesh_example.seed_model
WHERE
  event_date between @start_date and @end_date
"""
        )


def update_full_model(temp_dir) -> None:
    with open(temp_dir / "models" / "full_model.sql", "w", encoding="utf-8") as f:
        f.write(
            """
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id + 1 as item_id,
  count(distinct id) AS num_orders,
FROM
  sqlmesh_example.incremental_model
GROUP BY item_id
"""
        )


def init_prod_and_backfill(runner, temp_dir) -> None:
    result = runner.invoke(
        cli, ["--log-file-dir", temp_dir, "--paths", temp_dir, "plan", "--auto-apply"]
    )
    assert_plan_success(result)
    assert path.exists(temp_dir / "db.db")


def assert_duckdb_test(result) -> None:
    assert "Successfully Ran 1 tests against duckdb" in result.output


def assert_new_env(result, new_env="prod", from_env="prod") -> None:
    assert f"New environment `{new_env}` will be created from `{from_env}`" in result.output


def assert_model_versions_created(result) -> None:
    assert "All model versions have been created successfully" in result.output


def assert_model_batches_executed(result) -> None:
    assert "All model batches have been executed successfully" in result.output


def assert_target_env_updated(result) -> None:
    assert "The target environment has been updated successfully" in result.output


def assert_backfill_success(result) -> None:
    assert_model_versions_created(result)
    assert_model_batches_executed(result)
    assert_target_env_updated(result)


def assert_plan_success(result, new_env="prod", from_env="prod") -> None:
    assert result.exit_code == 0
    assert_duckdb_test(result)
    assert_new_env(result, new_env, from_env)
    assert_backfill_success(result)


def assert_virtual_update(result) -> None:
    assert "Virtual Update executed successfully" in result.output


def test_version(runner, tmp_path):
    from sqlmesh import __version__ as SQLMESH_VERSION

    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--version"])
    assert result.exit_code == 0
    assert SQLMESH_VERSION in result.output


def test_plan_no_config(runner, tmp_path):
    # Error if no SQLMesh project config is found
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan"])
    assert result.exit_code == 1
    assert "Error: SQLMesh project config could not be found" in result.output


def test_plan(runner, tmp_path):
    create_example_project(tmp_path)

    # Example project models have start dates, so there are no date prompts
    # for the `prod` environment.
    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan"], input="y\n"
    )
    assert_plan_success(result)


def test_plan_skip_tests(runner, tmp_path):
    create_example_project(tmp_path)

    # Successful test run message should not appear with `--skip-tests`
    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--skip-tests"], input="y\n"
    )
    assert result.exit_code == 0
    assert "Successfully Ran 1 tests against duckdb" not in result.output
    assert_new_env(result)
    assert_backfill_success(result)


def test_plan_restate_model(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    # plan with no changes and full_model restated
    # Input: enter for backfill start date prompt, enter for end date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "plan",
            "--restate-model",
            "sqlmesh_example.full_model",
        ],
        input="\n\ny\n",
    )
    assert result.exit_code == 0
    assert_duckdb_test(result)
    assert "No differences when compared to `prod`" in result.output
    assert "sqlmesh_example.full_model evaluated in" in result.output
    assert_backfill_success(result)


@pytest.mark.parametrize("flag", ["--skip-backfill", "--dry-run"])
def test_plan_skip_backfill(runner, tmp_path, flag):
    create_example_project(tmp_path)

    # plan for `prod` errors if `--skip-backfill` is passed without --no-gaps
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", flag])
    assert result.exit_code == 1
    assert (
        "Error: When targeting the production environment either the backfill should not be skipped or the lack of data gaps should be enforced (--no-gaps flag)."
        in result.output
    )

    # plan executes virtual update without executing model batches
    # Input: `y` to perform virtual update
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", flag, "--no-gaps"],
        input="y\n",
    )
    assert result.exit_code == 0
    assert_virtual_update(result)
    assert "All model batches have been executed successfully" not in result.output


def test_plan_auto_apply(runner, tmp_path):
    create_example_project(tmp_path)

    # plan for `prod` runs end-to-end with no user input with `--auto-apply`
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--auto-apply"]
    )
    assert_plan_success(result)

    # confirm verbose output not present
    assert "sqlmesh_example.seed_model created" not in result.output
    assert "sqlmesh_example.seed_model promoted" not in result.output


def test_plan_verbose(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--verbose"], input="y\n"
    )
    assert_plan_success(result)
    assert "sqlmesh_example.seed_model created" in result.output
    assert "sqlmesh_example.seed_model promoted" in result.output


def test_plan_dev(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: enter for backfill start date prompt, enter for end date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev"], input="\n\ny\n"
    )
    assert_plan_success(result, "dev")


def test_plan_dev_start_date(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: enter for backfill end date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--start", "2023-01-01"],
        input="\ny\n",
    )
    assert_plan_success(result, "dev")
    assert "sqlmesh_example__dev.full_model: 2023-01-01" in result.output
    assert "sqlmesh_example__dev.incremental_model: 2023-01-01" in result.output


def test_plan_dev_end_date(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: enter for backfill start date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--end", "2023-01-01"],
        input="\ny\n",
    )
    assert_plan_success(result, "dev")
    assert "sqlmesh_example__dev.full_model: 2020-01-01 - 2023-01-01" in result.output
    assert "sqlmesh_example__dev.incremental_model: 2020-01-01 - 2023-01-01" in result.output


def test_plan_dev_create_from(runner, tmp_path):
    create_example_project(tmp_path)

    # create dev environment and backfill
    runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "plan",
            "dev",
            "--no-prompts",
            "--auto-apply",
        ],
    )

    # create dev2 environment from dev environment
    # Input: `y` to apply and virtual update
    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "plan",
            "dev2",
            "--create-from",
            "dev",
            "--include-unmodified",
        ],
        input="y\n",
    )
    assert result.exit_code == 0
    assert_new_env(result, "dev2", "dev")
    assert_model_versions_created(result)
    assert_target_env_updated(result)
    assert_virtual_update(result)


def test_plan_dev_no_prompts(runner, tmp_path):
    create_example_project(tmp_path)

    # plan for non-prod environment doesn't prompt to apply and doesn't
    # backfill if only `--no-prompts` is passed
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--no-prompts"]
    )
    assert result.exit_code == 0
    assert "Apply - Backfill Tables [y/n]: " not in result.output
    assert "All model versions have been created successfully" not in result.output
    assert "All model batches have been executed successfully" not in result.output
    assert "The target environment has been updated successfully" not in result.output


def test_plan_dev_auto_apply(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: enter for backfill start date prompt, enter for end date prompt
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--auto-apply"],
        input="\n\n",
    )
    assert_plan_success(result, "dev")


def test_plan_dev_no_changes(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    # Error if no changes made and `--include-unmodified` is not passed
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev"])
    assert result.exit_code == 1
    assert (
        "Error: No changes were detected. Make a change or run with --include-unmodified"
        in result.output
    )

    # No error if no changes made and `--include-unmodified` is passed
    # Input: `y` to apply and virtual update
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--include-unmodified"],
        input="y\n",
    )
    assert result.exit_code == 0
    assert_new_env(result, "dev")
    assert_target_env_updated(result)
    assert_virtual_update(result)


def test_plan_nonbreaking(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_incremental_model(tmp_path)

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan"], input="y\n"
    )
    assert result.exit_code == 0
    assert "Summary of differences against `prod`" in result.output
    assert "+  'a' AS new_col" in result.output
    assert "Directly Modified: sqlmesh_example.incremental_model (Non-breaking)" in result.output
    assert "sqlmesh_example.full_model (Indirect Non-breaking)" in result.output
    assert "sqlmesh_example.incremental_model evaluated in" in result.output
    assert "sqlmesh_example.full_model evaluated in" not in result.output
    assert_backfill_success(result)


def test_plan_nonbreaking_noautocategorization(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_incremental_model(tmp_path)

    # Input: `2` to classify change as non-breaking, `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--no-auto-categorization"],
        input="2\ny\n",
    )
    assert result.exit_code == 0
    assert (
        "[1] [Breaking] Backfill sqlmesh_example.incremental_model and indirectly \nmodified children"
        in result.output
    )
    assert (
        "[2] [Non-breaking] Backfill sqlmesh_example.incremental_model but not indirectly\nmodified children"
        in result.output
    )
    assert_backfill_success(result)


def test_plan_nonbreaking_nodiff(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_incremental_model(tmp_path)

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--no-diff"], input="y\n"
    )
    assert result.exit_code == 0
    assert "+  'a' AS new_col" not in result.output
    assert_backfill_success(result)


def test_plan_breaking(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_full_model(tmp_path)

    # full_model change makes test fail, so we pass `--skip-tests`
    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--skip-tests"], input="y\n"
    )
    assert result.exit_code == 0
    assert "+  item_id + 1 AS item_id," in result.output
    assert "Directly Modified: sqlmesh_example.full_model (Breaking)" in result.output
    assert "sqlmesh_example.full_model evaluated in" in result.output
    assert "sqlmesh_example.incremental_model evaluated in" not in result.output
    assert_backfill_success(result)


def test_plan_dev_select(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_incremental_model(tmp_path)
    update_full_model(tmp_path)

    # full_model change makes test fail, so we pass `--skip-tests`
    # Input: enter for backfill start date prompt, enter for end date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "plan",
            "dev",
            "--skip-tests",
            "--select-model",
            "sqlmesh_example.incremental_model",
        ],
        input="\n\ny\n",
    )
    assert result.exit_code == 0
    # incremental_model diff present
    assert "+  'a' AS new_col" in result.output
    assert (
        "Directly Modified: sqlmesh_example__dev.incremental_model (Non-breaking)" in result.output
    )
    # full_model diff not present
    assert "+  item_id + 1 AS item_id," not in result.output
    assert "Directly Modified: sqlmesh_example__dev.full_model (Breaking)" not in result.output
    # only incremental_model backfilled
    assert "sqlmesh_example__dev.incremental_model evaluated in" in result.output
    assert "sqlmesh_example__dev.full_model evaluated in" not in result.output
    assert_backfill_success(result)


def test_plan_dev_backfill(runner, tmp_path):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    update_incremental_model(tmp_path)
    update_full_model(tmp_path)

    # full_model change makes test fail, so we pass `--skip-tests`
    # Input: enter for backfill start date prompt, enter for end date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "plan",
            "dev",
            "--skip-tests",
            "--backfill-model",
            "sqlmesh_example.incremental_model",
        ],
        input="\n\ny\n",
    )
    assert result.exit_code == 0
    assert_new_env(result, "dev")
    # both model diffs present
    assert "+  item_id + 1 AS item_id," in result.output
    assert "Directly Modified: sqlmesh_example__dev.full_model (Breaking)" in result.output
    assert "+  'a' AS new_col" in result.output
    assert (
        "Directly Modified: sqlmesh_example__dev.incremental_model (Non-breaking)" in result.output
    )
    # only incremental_model backfilled
    assert "sqlmesh_example__dev.incremental_model evaluated in" in result.output
    assert "sqlmesh_example__dev.full_model evaluated in" not in result.output
    assert_backfill_success(result)


def test_run_no_prod(runner, tmp_path):
    create_example_project(tmp_path)

    # Error if no env specified and `prod` doesn't exist
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run"])
    assert result.exit_code == 1
    assert "Error: Environment 'prod' was not found." in result.output


@pytest.mark.parametrize("flag", ["--skip-backfill", "--dry-run"])
@freeze_time(FREEZE_TIME)
def test_run_dev(runner, tmp_path, flag):
    create_example_project(tmp_path)

    # Create dev environment but DO NOT backfill
    # Input: `y` for virtual update
    runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", flag],
        input="y\n",
    )

    # Confirm backfill occurs when we run non-backfilled dev env
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run", "dev"])
    assert result.exit_code == 0
    assert_model_batches_executed(result)


@freeze_time(FREEZE_TIME)
def test_run_cron_not_elapsed(runner, tmp_path, caplog):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    # No error and no output if `prod` environment exists and cron has not elapsed
    with disable_logging():
        result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run"])
    assert result.exit_code == 0
    assert result.output.strip() == "Run finished for environment 'prod'"


def test_run_cron_elapsed(runner, tmp_path):
    create_example_project(tmp_path)

    # Create and backfill `prod` environment
    with freeze_time("2023-01-01 23:59:00"):
        init_prod_and_backfill(runner, tmp_path)

    # Run `prod` environment with daily cron elapsed
    with freeze_time("2023-01-02 00:01:00"):
        result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run"])

    assert result.exit_code == 0
    assert_model_batches_executed(result)


def test_clean(runner, tmp_path):
    # Create and backfill `prod` environment
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    # Confirm cache exists
    cache_path = Path(tmp_path) / ".cache"
    assert cache_path.exists()
    assert len(list(cache_path.iterdir())) > 0

    # Invoke the clean command
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "clean"])

    # Confirm cache was cleared
    assert result.exit_code == 0
    assert not cache_path.exists()


def test_table_name(runner, tmp_path):
    # Create and backfill `prod` environment
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)
    with disable_logging():
        result = runner.invoke(
            cli,
            [
                "--log-file-dir",
                tmp_path,
                "--paths",
                tmp_path,
                "table_name",
                "sqlmesh_example.full_model",
            ],
        )
    assert result.exit_code == 0
    assert result.output.startswith("db.sqlmesh__sqlmesh_example.sqlmesh_example__full_model__")


def test_info_on_new_project_does_not_create_state_sync(runner, tmp_path):
    create_example_project(tmp_path)

    # Invoke the info command
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "info"])
    assert result.exit_code == 0

    context = Context(paths=tmp_path)

    # Confirm that the state sync tables haven't been created
    assert not context.engine_adapter.table_exists("sqlmesh._snapshots")
    assert not context.engine_adapter.table_exists("sqlmesh._environments")
    assert not context.engine_adapter.table_exists("sqlmesh._intervals")
    assert not context.engine_adapter.table_exists("sqlmesh._plan_dags")
    assert not context.engine_adapter.table_exists("sqlmesh._versions")
