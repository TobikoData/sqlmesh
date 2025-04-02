import logging
from contextlib import contextmanager
from os import getcwd, path, remove
from pathlib import Path
from shutil import rmtree
from click import ClickException
import pytest
from click.testing import CliRunner
import time_machine
import json

from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.cli.main import cli
from sqlmesh.core.context import Context
from sqlmesh.integrations.dlt import generate_dlt_models
from sqlmesh.utils.date import now_ds, time_like_to_str, timedelta, to_datetime, yesterday_ds

FREEZE_TIME = "2023-01-01 00:00:00 UTC"

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

plan:
  no_prompts: false
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


def assert_new_env(result, new_env="prod", from_env="prod", initialize=True) -> None:
    assert (
        f"`{new_env}` environment will be initialized"
        if initialize
        else f"New environment `{new_env}` will be created from `{from_env}`"
    ) in result.output


def assert_physical_layer_updated(result) -> None:
    assert "Physical layer updated" in result.output


def assert_model_batches_executed(result) -> None:
    assert "Model batches executed" in result.output


def assert_virtual_layer_updated(result) -> None:
    assert "Virtual layer updated" in result.output


def assert_backfill_success(result) -> None:
    assert_physical_layer_updated(result)
    assert_model_batches_executed(result)
    assert_virtual_layer_updated(result)


def assert_plan_success(result, new_env="prod", from_env="prod") -> None:
    assert result.exit_code == 0
    assert_duckdb_test(result)
    assert_new_env(result, new_env, from_env)
    assert_backfill_success(result)


def assert_virtual_update(result) -> None:
    assert "Virtual Update executed" in result.output


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


def test_plan_skip_linter(runner, tmp_path):
    create_example_project(tmp_path)

    with open(tmp_path / "config.yaml", "a", encoding="utf-8") as f:
        f.write(
            """linter:
        enabled: True
        rules: "ALL"
    """
        )

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--skip-linter"], input="y\n"
    )

    assert result.exit_code == 0
    assert "Linter warnings" not in result.output
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
    assert "No changes to plan: project files match the `prod` environment" in result.output
    assert "sqlmesh_example.full_model                           [full refresh" in result.output
    assert_model_batches_executed(result)
    assert_virtual_layer_updated(result)


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
    assert "Model batches executed" not in result.output


def test_plan_auto_apply(runner, tmp_path):
    create_example_project(tmp_path)

    # plan for `prod` runs end-to-end with no user input with `--auto-apply`
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--auto-apply"]
    )
    assert_plan_success(result)

    # confirm verbose output not present
    assert "sqlmesh_example.seed_model created" not in result.output
    assert "sqlmesh_example.seed_model updated" not in result.output


def test_plan_verbose(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--verbose"], input="y\n"
    )
    assert_plan_success(result)
    assert "sqlmesh_example.seed_model                           created" in result.output
    assert "sqlmesh_example.seed_model                           promoted" in result.output


def test_plan_very_verbose(runner, tmp_path, copy_to_temp_path):
    temp_path = copy_to_temp_path("examples/sushi")

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", temp_path[0], "--paths", temp_path[0], "plan", "-v"],
        input="y\n",
    )
    assert result.exit_code == 0
    # models needing backfill list is still abbreviated with regular VERBOSE, so this should not be present
    assert "sushi.customers: [full refresh]" not in result.output

    # Input: `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", temp_path[0], "--paths", temp_path[0], "plan", "-vv"],
        input="y\n",
    )
    assert result.exit_code == 0
    # models needing backfill list is complete with VERY_VERBOSE, so this should be present
    assert "sushi.customers: [full refresh]" in result.output


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
    assert "sqlmesh_example__dev.full_model: [full refresh]" in result.output
    assert "sqlmesh_example__dev.incremental_model: [2023-01-01" in result.output


def test_plan_dev_end_date(runner, tmp_path):
    create_example_project(tmp_path)

    # Input: enter for backfill start date prompt, `y` to apply and backfill
    result = runner.invoke(
        cli,
        ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--end", "2023-01-01"],
        input="\ny\n",
    )
    assert_plan_success(result, "dev")
    assert "sqlmesh_example__dev.full_model: [full refresh]" in result.output
    assert "sqlmesh_example__dev.incremental_model: [2020-01-01 - 2023-01-01]" in result.output


def test_plan_dev_create_from_virtual(runner, tmp_path):
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
    assert_new_env(result, "dev2", "dev", initialize=False)
    assert_virtual_layer_updated(result)
    assert_virtual_update(result)


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
    # make model change
    update_incremental_model(tmp_path)

    # create dev2 environment from dev
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
            "--no-prompts",
            "--auto-apply",
        ],
    )

    assert result.exit_code == 0
    assert_new_env(result, "dev2", "dev", initialize=False)
    assert "Differences from the `dev` environment:" in result.output


def test_plan_dev_bad_create_from(runner, tmp_path):
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
    # make model change
    update_incremental_model(tmp_path)

    # create dev2 environment from non-existent dev3
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
            "dev3",
            "--no-prompts",
            "--auto-apply",
        ],
    )

    assert result.exit_code == 0
    assert_new_env(result, "dev2", "dev")
    assert (
        "[WARNING] The environment name 'dev3' was passed to the `plan` command's `--create-from` argument, but 'dev3' does not exist. Initializing new environment 'dev2' from scratch."
        in result.output.replace("\n", "")
    )


def test_plan_dev_no_prompts(runner, tmp_path):
    create_example_project(tmp_path)

    # plan for non-prod environment doesn't prompt for dates but prompts to apply
    result = runner.invoke(
        cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "dev", "--no-prompts"]
    )
    assert "Apply - Backfill Tables [y/n]: " in result.output
    assert "Physical layer updated" not in result.output
    assert "Model batches executed" not in result.output
    assert "The target environment has been updated" not in result.output


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
        "Error: Creating a new environment requires a change, but project files match the `prod` environment. Make a change or use the --include-unmodified flag to create a new environment without changes."
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
    assert_new_env(result, "dev", initialize=False)
    assert_virtual_layer_updated(result)
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
    assert "Differences from the `prod` environment" in result.output
    assert "+  'a' AS new_col" in result.output
    assert "Directly Modified: sqlmesh_example.incremental_model (Non-breaking)" in result.output
    assert "sqlmesh_example.full_model (Indirect Non-breaking)" in result.output
    assert "sqlmesh_example.incremental_model                    [insert" in result.output
    assert "sqlmesh_example.full_model evaluated [full refresh" not in result.output
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
    assert "sqlmesh_example.full_model                           [full refresh" in result.output
    assert "sqlmesh_example.incremental_model                     [insert" not in result.output
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
    assert "sqlmesh_example__dev.incremental_model               [insert" in result.output
    assert "sqlmesh_example__dev.full_model                    [full refresh" not in result.output
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
    assert_new_env(result, "dev", initialize=False)
    # both model diffs present
    assert "+  item_id + 1 AS item_id," in result.output
    assert "Directly Modified: sqlmesh_example__dev.full_model (Breaking)" in result.output
    assert "+  'a' AS new_col" in result.output
    assert (
        "Directly Modified: sqlmesh_example__dev.incremental_model (Non-breaking)" in result.output
    )
    # only incremental_model backfilled
    assert "sqlmesh_example__dev.incremental_model               [insert" in result.output
    assert "sqlmesh_example__dev.full_model                      [full refresh" not in result.output
    assert_backfill_success(result)


def test_run_no_prod(runner, tmp_path):
    create_example_project(tmp_path)

    # Error if no env specified and `prod` doesn't exist
    result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run"])
    assert result.exit_code == 1
    assert "Error: Environment 'prod' was not found." in result.output


@pytest.mark.parametrize("flag", ["--skip-backfill", "--dry-run"])
@time_machine.travel(FREEZE_TIME)
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


@time_machine.travel(FREEZE_TIME)
def test_run_cron_not_elapsed(runner, tmp_path, caplog):
    create_example_project(tmp_path)
    init_prod_and_backfill(runner, tmp_path)

    # No error if `prod` environment exists and cron has not elapsed
    with disable_logging():
        result = runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "run"])
    assert result.exit_code == 0

    assert (
        "No models are ready to run. Please wait until a model `cron` interval has \nelapsed.\n\nNext run will be ready at "
        in result.output.strip()
    )


def test_run_cron_elapsed(runner, tmp_path):
    create_example_project(tmp_path)

    # Create and backfill `prod` environment
    with time_machine.travel("2023-01-01 23:59:00 UTC", tick=False) as traveler:
        runner = CliRunner()
        init_prod_and_backfill(runner, tmp_path)

        # Run `prod` environment with daily cron elapsed
        traveler.move_to("2023-01-02 00:01:00 UTC")
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


def test_dlt_pipeline_errors(runner, tmp_path):
    # Error if no pipeline is provided
    result = runner.invoke(cli, ["--paths", tmp_path, "init", "-t", "dlt", "duckdb"])
    assert (
        "Error: DLT pipeline is a required argument to generate a SQLMesh project from DLT"
        in result.output
    )

    # Error if the pipeline provided is not correct
    result = runner.invoke(
        cli,
        ["--paths", tmp_path, "init", "-t", "dlt", "--dlt-pipeline", "missing_pipeline", "duckdb"],
    )
    assert "Error: Could not attach to pipeline" in result.output


@time_machine.travel(FREEZE_TIME)
def test_dlt_filesystem_pipeline(tmp_path):
    import dlt

    root_dir = path.abspath(getcwd())
    storage_path = root_dir + "/temp_storage"
    if path.exists(storage_path):
        rmtree(storage_path)

    filesystem_pipeline = dlt.pipeline(
        pipeline_name="filesystem_pipeline",
        destination=dlt.destinations.filesystem("file://" + storage_path),
    )
    info = filesystem_pipeline.run([{"item_id": 1}], table_name="equipment")
    assert not info.has_failed_jobs

    init_example_project(tmp_path, "athena", ProjectTemplate.DLT, "filesystem_pipeline")

    # Validate generated sqlmesh config and models
    config_path = tmp_path / "config.yaml"
    equipment_model_path = tmp_path / "models/incremental_equipment.sql"
    dlt_loads_model_path = tmp_path / "models/incremental__dlt_loads.sql"

    assert config_path.exists()
    assert equipment_model_path.exists()
    assert dlt_loads_model_path.exists()

    expected_incremental_model = """MODEL (
  name filesystem_pipeline_dataset_sqlmesh.incremental_equipment,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(c.item_id AS BIGINT) AS item_id,
  CAST(c._dlt_load_id AS VARCHAR) AS _dlt_load_id,
  CAST(c._dlt_id AS VARCHAR) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  filesystem_pipeline_dataset.equipment as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
"""

    with open(equipment_model_path) as file:
        incremental_model = file.read()

    assert incremental_model == expected_incremental_model

    expected_config = (
        "gateways:\n"
        "  athena:\n"
        "    connection:\n"
        "      # For more information on configuring the connection to your execution engine, visit:\n"
        "      # https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#connections\n"
        "      # https://sqlmesh.readthedocs.io/en/stable/integrations/engines/athena/#connection-options\n"
        "      type: athena\n"
        "      # concurrent_tasks: 4\n"
        "      # register_comments: False\n"
        "      # pre_ping: False\n"
        "      # pretty_sql: False\n"
        "      # aws_access_key_id: \n"
        "      # aws_secret_access_key: \n"
        "      # role_arn: \n"
        "      # role_session_name: \n"
        "      # region_name: \n"
        "      # work_group: \n"
        "      # s3_staging_dir: \n"
        "      # schema_name: \n"
        "      # catalog_name: \n"
        "      # s3_warehouse_location: \n\n\n"
        "default_gateway: athena\n\n"
        "model_defaults:\n"
        "  dialect: athena\n"
        f"  start: {yesterday_ds()}\n"
    )

    with open(config_path) as file:
        config = file.read()

    assert config == expected_config

    if path.exists(storage_path):
        rmtree(storage_path)


@time_machine.travel(FREEZE_TIME)
def test_plan_dlt(runner, tmp_path):
    from dlt.common.pipeline import get_dlt_pipelines_dir

    root_dir = path.abspath(getcwd())
    pipeline_path = root_dir + "/examples/sushi_dlt/sushi_pipeline.py"
    dataset_path = root_dir + "/sushi.duckdb"

    if path.exists(dataset_path):
        remove(dataset_path)

    with open(pipeline_path) as file:
        exec(file.read())

    # This should fail since it won't be able to locate the pipeline in this path
    with pytest.raises(ClickException, match=r".*Could not attach to pipeline*"):
        init_example_project(
            tmp_path, "duckdb", ProjectTemplate.DLT, "sushi", dlt_path="./dlt2/pipelines"
        )

    # By setting the pipelines path where the pipeline directory is located, it should work
    dlt_path = get_dlt_pipelines_dir()
    init_example_project(tmp_path, "duckdb", ProjectTemplate.DLT, "sushi", dlt_path=dlt_path)

    expected_config = f"""gateways:
  duckdb:
    connection:
      type: duckdb
      database: {dataset_path}

default_gateway: duckdb

model_defaults:
  dialect: duckdb
  start: {yesterday_ds()}
"""

    with open(tmp_path / "config.yaml") as file:
        config = file.read()

    expected_incremental_model = """MODEL (
  name sushi_dataset_sqlmesh.incremental_sushi_types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  grain (id),
);

SELECT
  CAST(c.id AS BIGINT) AS id,
  CAST(c.name AS TEXT) AS name,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  sushi_dataset.sushi_types as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
"""

    dlt_sushi_types_model_path = tmp_path / "models/incremental_sushi_types.sql"
    dlt_loads_model_path = tmp_path / "models/incremental__dlt_loads.sql"
    dlt_waiters_model_path = tmp_path / "models/incremental_waiters.sql"
    dlt_sushi_fillings_model_path = tmp_path / "models/incremental_sushi_menu__fillings.sql"
    dlt_sushi_twice_nested_model_path = (
        tmp_path / "models/incremental_sushi_menu__details__ingredients.sql"
    )

    with open(dlt_sushi_types_model_path) as file:
        incremental_model = file.read()

    expected_dlt_loads_model = """MODEL (
  name sushi_dataset_sqlmesh.incremental__dlt_loads,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(c.load_id AS TEXT) AS load_id,
  CAST(c.schema_name AS TEXT) AS schema_name,
  CAST(c.status AS BIGINT) AS status,
  CAST(c.inserted_at AS TIMESTAMP) AS inserted_at,
  CAST(c.schema_version_hash AS TEXT) AS schema_version_hash,
  TO_TIMESTAMP(CAST(c.load_id AS DOUBLE)) as _dlt_load_time
FROM
  sushi_dataset._dlt_loads as c
WHERE
  TO_TIMESTAMP(CAST(c.load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
"""

    with open(dlt_loads_model_path) as file:
        dlt_loads_model = file.read()

    expected_nested_fillings_model = """MODEL (
  name sushi_dataset_sqlmesh.incremental_sushi_menu__fillings,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(c.value AS TEXT) AS value,
  CAST(c._dlt_root_id AS TEXT) AS _dlt_root_id,
  CAST(c._dlt_parent_id AS TEXT) AS _dlt_parent_id,
  CAST(c._dlt_list_idx AS BIGINT) AS _dlt_list_idx,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(p._dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  sushi_dataset.sushi_menu__fillings as c
JOIN
  sushi_dataset.sushi_menu as p
ON
  c._dlt_parent_id = p._dlt_id
WHERE
  TO_TIMESTAMP(CAST(p._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
"""

    with open(dlt_sushi_fillings_model_path) as file:
        nested_model = file.read()

    # Validate generated config and models
    assert config == expected_config
    assert dlt_loads_model_path.exists()
    assert dlt_sushi_types_model_path.exists()
    assert dlt_waiters_model_path.exists()
    assert dlt_sushi_fillings_model_path.exists()
    assert dlt_sushi_twice_nested_model_path.exists()
    assert dlt_loads_model == expected_dlt_loads_model
    assert incremental_model == expected_incremental_model
    assert nested_model == expected_nested_fillings_model

    try:
        # Plan prod and backfill
        result = runner.invoke(
            cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan", "--auto-apply"]
        )

        assert result.exit_code == 0
        assert_backfill_success(result)

        # Remove and update with missing model
        remove(dlt_waiters_model_path)
        assert not dlt_waiters_model_path.exists()

        # Update with force = False will generate only the missing model
        context = Context(paths=tmp_path)
        assert generate_dlt_models(context, "sushi", [], False) == [
            "sushi_dataset_sqlmesh.incremental_waiters"
        ]
        assert dlt_waiters_model_path.exists()

        # Remove all models
        remove(dlt_waiters_model_path)
        remove(dlt_loads_model_path)
        remove(dlt_sushi_types_model_path)
        remove(dlt_sushi_fillings_model_path)
        remove(dlt_sushi_twice_nested_model_path)

        # Update to generate a specific model: sushi_types.
        # Also validate using the dlt_path that the pipelines are located.
        assert generate_dlt_models(context, "sushi", ["sushi_types"], False, dlt_path) == [
            "sushi_dataset_sqlmesh.incremental_sushi_types"
        ]

        # Only the sushi_types should be generated now
        assert not dlt_waiters_model_path.exists()
        assert not dlt_loads_model_path.exists()
        assert not dlt_sushi_fillings_model_path.exists()
        assert not dlt_sushi_twice_nested_model_path.exists()
        assert dlt_sushi_types_model_path.exists()

        # Update with force = True will generate all models and overwrite existing ones
        generate_dlt_models(context, "sushi", [], True)
        assert dlt_loads_model_path.exists()
        assert dlt_sushi_types_model_path.exists()
        assert dlt_waiters_model_path.exists()
        assert dlt_sushi_fillings_model_path.exists()
        assert dlt_sushi_twice_nested_model_path.exists()
    finally:
        remove(dataset_path)


@time_machine.travel(FREEZE_TIME)
def test_init_project_dialects(tmp_path):
    dialect_to_config = {
        "redshift": "# concurrent_tasks: 4\n      # register_comments: True\n      # pre_ping: False\n      # pretty_sql: False\n      # user: \n      # password: \n      # database: \n      # host: \n      # port: \n      # source_address: \n      # unix_sock: \n      # ssl: \n      # sslmode: \n      # timeout: \n      # tcp_keepalive: \n      # application_name: \n      # preferred_role: \n      # principal_arn: \n      # credentials_provider: \n      # region: \n      # cluster_identifier: \n      # iam: \n      # is_serverless: \n      # serverless_acct_id: \n      # serverless_work_group: \n      # enable_merge: ",
        "bigquery": "# concurrent_tasks: 1\n      # register_comments: True\n      # pre_ping: False\n      # pretty_sql: False\n      # method: oauth\n      # project: \n      # execution_project: \n      # quota_project: \n      # location: \n      # keyfile: \n      # keyfile_json: \n      # token: \n      # refresh_token: \n      # client_id: \n      # client_secret: \n      # token_uri: \n      # scopes: \n      # impersonated_service_account: \n      # job_creation_timeout_seconds: \n      # job_execution_timeout_seconds: \n      # job_retries: 1\n      # job_retry_deadline_seconds: \n      # priority: \n      # maximum_bytes_billed: ",
        "snowflake": "account: \n      # concurrent_tasks: 4\n      # register_comments: True\n      # pre_ping: False\n      # pretty_sql: False\n      # user: \n      # password: \n      # warehouse: \n      # database: \n      # role: \n      # authenticator: \n      # token: \n      # host: \n      # port: \n      # application: Tobiko_SQLMesh\n      # private_key: \n      # private_key_path: \n      # private_key_passphrase: \n      # session_parameters: ",
        "databricks": "# concurrent_tasks: 1\n      # register_comments: True\n      # pre_ping: False\n      # pretty_sql: False\n      # server_hostname: \n      # http_path: \n      # access_token: \n      # auth_type: \n      # oauth_client_id: \n      # oauth_client_secret: \n      # catalog: \n      # http_headers: \n      # session_configuration: \n      # databricks_connect_server_hostname: \n      # databricks_connect_access_token: \n      # databricks_connect_cluster_id: \n      # databricks_connect_use_serverless: False\n      # force_databricks_connect: False\n      # disable_databricks_connect: False\n      # disable_spark_session: False",
        "postgres": "host: \n      user: \n      password: \n      port: \n      database: \n      # concurrent_tasks: 4\n      # register_comments: True\n      # pre_ping: True\n      # pretty_sql: False\n      # keepalives_idle: \n      # connect_timeout: 10\n      # role: \n      # sslmode: \n      # application_name: ",
    }

    for dialect, expected_config in dialect_to_config.items():
        init_example_project(tmp_path, dialect=dialect)

        config_start = f"gateways:\n  {dialect}:\n    connection:\n      # For more information on configuring the connection to your execution engine, visit:\n      # https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#connections\n      # https://sqlmesh.readthedocs.io/en/stable/integrations/engines/{dialect}/#connection-options\n      type: {dialect}\n      "
        config_end = f"\n\n\ndefault_gateway: {dialect}\n\nmodel_defaults:\n  dialect: {dialect}\n  start: {yesterday_ds()}\n"

        with open(tmp_path / "config.yaml") as file:
            config = file.read()

            assert config == f"{config_start}{expected_config}{config_end}"

            remove(tmp_path / "config.yaml")


@time_machine.travel(FREEZE_TIME)
def test_environments(runner, tmp_path):
    create_example_project(tmp_path)
    ttl = time_like_to_str(to_datetime(now_ds()) + timedelta(days=7))

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

    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "environments",
        ],
    )
    assert result.exit_code == 0
    assert f"Number of SQLMesh environments are: 1\ndev - {ttl}\n" in result.output

    # # create dev2 environment from dev environment
    # # Input: `y` to apply and virtual update
    runner.invoke(
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

    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "environments",
        ],
    )
    assert result.exit_code == 0
    assert f"Number of SQLMesh environments are: 2\ndev - {ttl}\ndev2 - {ttl}\n" in result.output

    # Example project models have start dates, so there are no date prompts
    # for the `prod` environment.
    # Input: `y` to apply and backfill
    runner.invoke(cli, ["--log-file-dir", tmp_path, "--paths", tmp_path, "plan"], input="y\n")
    result = runner.invoke(
        cli,
        [
            "--log-file-dir",
            tmp_path,
            "--paths",
            tmp_path,
            "environments",
        ],
    )
    assert result.exit_code == 0
    assert (
        f"Number of SQLMesh environments are: 3\ndev - {ttl}\ndev2 - {ttl}\nprod - No Expiry\n"
        in result.output
    )


def test_lint(runner, tmp_path):
    create_example_project(tmp_path)

    with open(tmp_path / "config.yaml", "a", encoding="utf-8") as f:
        f.write(
            """linter:
    enabled: True
    rules: "ALL"
"""
        )

    result = runner.invoke(cli, ["--paths", tmp_path, "lint"])
    assert result.output.count("Linter errors for") == 2
    assert result.exit_code == 1

    # Test with specific model
    result = runner.invoke(
        cli, ["--paths", tmp_path, "lint", "--model", "sqlmesh_example.seed_model"]
    )
    assert result.output.count("Linter errors for") == 1
    assert result.exit_code == 1

    # Test with multiple models
    result = runner.invoke(
        cli,
        [
            "--paths",
            tmp_path,
            "lint",
            "--model",
            "sqlmesh_example.seed_model",
            "--model",
            "sqlmesh_example.incremental_model",
        ],
    )
    assert result.output.count("Linter errors for") == 2
    assert result.exit_code == 1


def test_state_export(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # create some state
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    # export it
    result = runner.invoke(
        cli,
        ["--paths", str(tmp_path), "state", "export", "-o", str(state_export_file), "--no-confirm"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # verify output
    assert "Gateway: local" in result.output
    assert "Type: duckdb" in result.output
    assert "Exporting versions" in result.output
    assert "Exporting snapshots" in result.output
    assert "Exporting environments" in result.output
    assert "State exported successfully" in result.output

    assert state_export_file.exists()
    assert len(state_export_file.read_text()) > 0


def test_state_export_specific_environments(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # create prod
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    (tmp_path / "models" / "new_model.sql").write_text(
        """
    MODEL (
        name sqlmesh_example.new_model,
        kind FULL
    );

    SELECT 1;
    """
    )

    # create dev env with new model
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "dev",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    # export non existent env - should fail
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--environment",
            "nonexist",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "No such environment: nonexist" in result.output

    # export dev, should contain original snapshots + new one
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--environment",
            "dev",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Environment: dev" in result.output
    assert "State exported successfully" in result.output

    state = json.loads(state_export_file.read_text(encoding="utf8"))
    assert len(state["snapshots"]) == 4
    assert any("new_model" in s["name"] for s in state["snapshots"])
    assert len(state["environments"]) == 1
    assert "dev" in state["environments"]
    assert "prod" not in state["environments"]


def test_state_export_local(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # note: we have not plan+applied at all, we are just exporting local state
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--local",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Exporting local state" in result.output
    assert "the resulting file cannot be imported" in result.output
    assert "State exported successfully" in result.output

    state = json.loads(state_export_file.read_text(encoding="utf8"))
    assert len(state["snapshots"]) == 3
    assert not state["metadata"]["importable"]
    assert len(state["environments"]) == 0

    # test mutually exclusive with --environment
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--environment",
            "foo",
            "--local",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    assert "Cannot specify both --environment and --local" in result.output


def test_state_import(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # create some state
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    # export it
    result = runner.invoke(
        cli,
        ["--paths", str(tmp_path), "state", "export", "-o", str(state_export_file), "--no-confirm"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # import it back
    result = runner.invoke(
        cli,
        ["--paths", str(tmp_path), "state", "import", "-i", str(state_export_file), "--no-confirm"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert "Gateway: local" in result.output
    assert "Type: duckdb" in result.output
    assert "Importing versions" in result.output
    assert "Importing snapshots" in result.output
    assert "Importing environments" in result.output
    assert "State imported successfully" in result.output

    # plan should have no changes
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
        ],
    )
    assert result.exit_code == 0
    assert "No changes to plan" in result.output


def test_state_import_replace(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # prod
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    (tmp_path / "models" / "new_model.sql").write_text(
        """
    MODEL (
        name sqlmesh_example.new_model,
        kind FULL
    );

    SELECT 1;
    """
    )

    # create dev with new model
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "plan",
            "dev",
            "--no-prompts",
            "--auto-apply",
        ],
    )
    assert result.exit_code == 0

    # prove both dev and prod exist
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "environments",
        ],
    )
    assert result.exit_code == 0
    assert "dev -" in result.output
    assert "prod -" in result.output

    # export just prod
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--environment",
            "prod",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # import it back with --replace
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "import",
            "-i",
            str(state_export_file),
            "--replace",
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "State imported successfully" in result.output

    # prove only prod exists now
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "environments",
        ],
    )
    assert result.exit_code == 0
    assert "dev -" not in result.output
    assert "prod -" in result.output


def test_state_import_local(runner: CliRunner, tmp_path: Path) -> None:
    create_example_project(tmp_path)

    state_export_file = tmp_path / "state_export.json"

    # local state export
    result = runner.invoke(
        cli,
        [
            "--paths",
            str(tmp_path),
            "state",
            "export",
            "--local",
            "-o",
            str(state_export_file),
            "--no-confirm",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # import should fail - local state is not importable
    result = runner.invoke(
        cli,
        ["--paths", str(tmp_path), "state", "import", "-i", str(state_export_file), "--no-confirm"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "State file is marked as not importable" in result.output
    assert "Aborting" in result.output
