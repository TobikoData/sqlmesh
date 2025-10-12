import typing as t
from pathlib import Path
import pytest
from sqlmesh_dbt.operations import create
from sqlmesh_dbt.console import DbtCliConsole
from sqlmesh.utils import yaml
from sqlmesh.utils.errors import SQLMeshError
import time_machine
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.core.config.common import VirtualEnvironmentMode
from tests.dbt.conftest import EmptyProjectCreator
import logging

pytestmark = pytest.mark.slow


class PlanCapturingConsole(DbtCliConsole):
    def plan(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: t.Optional[str],
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        self.plan_builder = plan_builder
        self.auto_apply = auto_apply
        self.default_catalog = default_catalog
        self.no_diff = no_diff
        self.no_prompts = no_prompts

        # normal console starts applying the plan here; we dont because we just want to capture the parameters
        # and check they were set correctly


def test_create_sets_and_persists_default_start_date(jaffle_shop_duckdb: Path):
    with time_machine.travel("2020-01-02 00:00:00 UTC"):
        from sqlmesh.utils.date import yesterday_ds, to_ds

        assert yesterday_ds() == "2020-01-01"

        operations = create()

        assert operations.context.config.model_defaults.start
        assert to_ds(operations.context.config.model_defaults.start) == "2020-01-01"
        assert all(
            to_ds(model.start) if model.start else None == "2020-01-01"
            for model in operations.context.models.values()
            if not model.kind.is_seed
        )

    # check that the date set on the first invocation persists to future invocations
    from sqlmesh.utils.date import yesterday_ds, to_ds

    assert yesterday_ds() != "2020-01-01"

    operations = create()

    assert operations.context.config.model_defaults.start
    assert to_ds(operations.context.config.model_defaults.start) == "2020-01-01"
    assert all(
        to_ds(model.start) if model.start else None == "2020-01-01"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )


def test_create_uses_configured_start_date_if_supplied(jaffle_shop_duckdb: Path):
    sqlmesh_yaml = jaffle_shop_duckdb / "sqlmesh.yml"

    with sqlmesh_yaml.open("w") as f:
        yaml.dump({"model_defaults": {"start": "2023-12-12"}}, f)

    operations = create()

    assert operations.context.config.model_defaults.start == "2023-12-12"
    assert all(
        model.start == "2023-12-12"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )


def test_create_can_specify_profile_and_target(jaffle_shop_duckdb: Path):
    with pytest.raises(SQLMeshError, match=r"Profile 'foo' not found"):
        create(profile="foo")

    with pytest.raises(
        SQLMeshError, match=r"Target 'prod' not specified in profiles for 'jaffle_shop'"
    ):
        create(profile="jaffle_shop", target="prod")

    dbt_project = create(profile="jaffle_shop", target="dev").project

    assert dbt_project.context.profile_name == "jaffle_shop"
    assert dbt_project.context.target_name == "dev"


def test_default_options(jaffle_shop_duckdb: Path):
    operations = create()

    config = operations.context.config
    dbt_project = operations.project

    assert config.plan.always_recreate_environment is True
    assert config.virtual_environment_mode == VirtualEnvironmentMode.DEV_ONLY
    assert config.model_defaults.start is not None
    assert config.model_defaults.dialect == dbt_project.context.target.dialect


def test_create_can_set_project_variables(jaffle_shop_duckdb: Path):
    (jaffle_shop_duckdb / "models" / "test_model.sql").write_text("""
       select '{{ var('foo') }}' as a
    """)

    dbt_project = create(vars={"foo": "bar"})
    assert dbt_project.context.config.variables["foo"] == "bar"

    test_model = dbt_project.context.models['"jaffle_shop"."main"."test_model"']
    query = test_model.render_query()
    assert query is not None
    assert query.sql() == "SELECT 'bar' AS \"a\""


def test_run_option_mapping(jaffle_shop_duckdb: Path):
    operations = create(project_dir=jaffle_shop_duckdb)
    console = PlanCapturingConsole()
    operations.context.console = console

    plan = operations.run()
    standalone_audit_name = "relationships_orders_customer_id__customer_id__ref_customers_"
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill is None
    assert {s.name for s in plan.snapshots} == {k for k in operations.context.snapshots}

    plan = operations.run(select=["stg_orders+"])
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill == {
        '"jaffle_shop"."main"."customers"',
        '"jaffle_shop"."main"."orders"',
        '"jaffle_shop"."main"."stg_orders"',
    }
    assert {s.name for s in plan.snapshots} == (
        plan.selected_models_to_backfill | {standalone_audit_name}
    )

    plan = operations.run(select=["stg_orders+"], exclude=["customers"])
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill == {
        '"jaffle_shop"."main"."orders"',
        '"jaffle_shop"."main"."stg_orders"',
    }
    assert {s.name for s in plan.snapshots} == (
        plan.selected_models_to_backfill | {standalone_audit_name}
    )

    plan = operations.run(exclude=["customers"])
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill == {k for k in operations.context.snapshots} - {
        '"jaffle_shop"."main"."customers"'
    } - {standalone_audit_name}
    assert {s.name for s in plan.snapshots} == (
        plan.selected_models_to_backfill | {standalone_audit_name}
    )

    plan = operations.run(empty=True)
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is True
    assert plan.selected_models_to_backfill is None
    assert {s.name for s in plan.snapshots} == {k for k in operations.context.snapshots}


def test_run_option_mapping_dev(jaffle_shop_duckdb: Path):
    # create prod so that dev has something to compare against
    operations = create(project_dir=jaffle_shop_duckdb)
    operations.run()

    (jaffle_shop_duckdb / "models" / "new_model.sql").write_text("select 1")

    operations = create(project_dir=jaffle_shop_duckdb)

    console = PlanCapturingConsole()
    operations.context.console = console

    plan = operations.run(environment="dev")
    assert plan.environment.name == "dev"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.include_unmodified is False
    assert plan.context_diff.create_from == "prod"
    assert plan.context_diff.is_new_environment is True
    assert console.plan_builder._enable_preview is True
    assert plan.end_bounded is True
    assert plan.ignore_cron is False
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill == {'"jaffle_shop"."main"."new_model"'}

    plan = operations.run(environment="dev", empty=True)
    assert plan.environment.name == "dev"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.include_unmodified is False
    assert plan.context_diff.create_from == "prod"
    assert plan.context_diff.is_new_environment is True
    assert console.plan_builder._enable_preview is True
    assert plan.end_bounded is True
    assert plan.ignore_cron is False
    assert plan.skip_backfill is True
    assert plan.selected_models_to_backfill == {'"jaffle_shop"."main"."new_model"'}

    plan = operations.run(environment="dev", select=["stg_orders+"])
    assert plan.environment.name == "dev"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.include_unmodified is False
    assert plan.context_diff.create_from == "prod"
    assert plan.context_diff.is_new_environment is True
    assert console.plan_builder._enable_preview is True
    # dev plans with --select have run=True, ignore_cron=True set
    # as opposed to dev plans that dont have a specific selector
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    # note: the new model in the dev environment is ignored in favour of the explicitly selected ones
    assert plan.selected_models_to_backfill == {
        '"jaffle_shop"."main"."customers"',
        '"jaffle_shop"."main"."orders"',
        '"jaffle_shop"."main"."stg_orders"',
    }


@pytest.mark.parametrize(
    "env_name,vde_mode",
    [
        ("prod", VirtualEnvironmentMode.DEV_ONLY),
        ("prod", VirtualEnvironmentMode.FULL),
        ("dev", VirtualEnvironmentMode.DEV_ONLY),
        ("dev", VirtualEnvironmentMode.FULL),
    ],
)
def test_run_option_full_refresh(
    create_empty_project: EmptyProjectCreator, env_name: str, vde_mode: VirtualEnvironmentMode
):
    # create config file prior to load
    project_path, models_path = create_empty_project(project_name="test")

    config_path = project_path / "sqlmesh.yaml"
    config = yaml.load(config_path)
    config["virtual_environment_mode"] = vde_mode.value

    with config_path.open("w") as f:
        yaml.dump(config, f)

    (models_path / "model_a.sql").write_text("select 1")
    (models_path / "model_b.sql").write_text("select 2")

    operations = create(project_dir=project_path)

    assert operations.context.config.virtual_environment_mode == vde_mode

    console = PlanCapturingConsole()
    operations.context.console = console

    plan = operations.run(environment=env_name, full_refresh=True)

    # both models added as backfills + restatements regardless of env / vde mode setting
    assert plan.environment.name == env_name
    assert len(plan.restatements) == 2
    assert list(plan.restatements)[0].name == '"test"."main"."model_a"'
    assert list(plan.restatements)[1].name == '"test"."main"."model_b"'

    assert plan.requires_backfill
    assert not plan.empty_backfill
    assert not plan.skip_backfill
    assert plan.models_to_backfill == set(['"test"."main"."model_a"', '"test"."main"."model_b"'])

    if vde_mode == VirtualEnvironmentMode.DEV_ONLY:
        # We do not clear intervals across all model versions in the default DEV_ONLY mode, even when targeting prod,
        # because dev data is hardcoded to preview only so by definition and can never be deployed
        assert not plan.restate_all_snapshots
    else:
        if env_name == "prod":
            # in FULL mode, we do it for prod
            assert plan.restate_all_snapshots
        else:
            # but not dev
            assert not plan.restate_all_snapshots


def test_run_option_full_refresh_with_selector(jaffle_shop_duckdb: Path):
    operations = create(project_dir=jaffle_shop_duckdb)
    assert len(operations.context.models) > 5

    console = PlanCapturingConsole()
    operations.context.console = console

    plan = operations.run(select=["stg_customers"], full_refresh=True)
    assert len(plan.restatements) == 1
    assert list(plan.restatements)[0].name == '"jaffle_shop"."main"."stg_customers"'

    assert plan.requires_backfill
    assert not plan.empty_backfill
    assert not plan.skip_backfill
    assert plan.models_to_backfill == set(['"jaffle_shop"."main"."stg_customers"'])


def test_create_sets_concurrent_tasks_based_on_threads(create_empty_project: EmptyProjectCreator):
    project_dir, _ = create_empty_project(project_name="test")

    # add a postgres target because duckdb overrides to concurrent_tasks=1 regardless of what gets specified
    profiles_yml_file = project_dir / "profiles.yml"
    profiles_yml = yaml.load(profiles_yml_file)
    profiles_yml["test"]["outputs"]["postgres"] = {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "dbname": "test",
        "schema": "test",
    }
    profiles_yml_file.write_text(yaml.dump(profiles_yml))

    operations = create(project_dir=project_dir, target="postgres")

    assert operations.context.concurrent_tasks == 1  # 1 is the default

    operations = create(project_dir=project_dir, threads=16, target="postgres")

    assert operations.context.concurrent_tasks == 16
    assert all(
        g.connection and g.connection.concurrent_tasks == 16
        for g in operations.context.config.gateways.values()
    )


def test_create_configures_log_level(create_empty_project: EmptyProjectCreator):
    project_dir, _ = create_empty_project()

    create(project_dir=project_dir, log_level="info")
    assert logging.getLogger("sqlmesh").getEffectiveLevel() == logging.INFO

    create(project_dir=project_dir, log_level="error")
    assert logging.getLogger("sqlmesh").getEffectiveLevel() == logging.ERROR
