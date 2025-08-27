import typing as t
from pathlib import Path
import pytest
from sqlmesh_dbt.operations import create
from sqlmesh.utils import yaml
from sqlmesh.utils.errors import SQLMeshError
import time_machine
from sqlmesh.core.console import NoopConsole
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.core.config.common import VirtualEnvironmentMode

pytestmark = pytest.mark.slow


class PlanCapturingConsole(NoopConsole):
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
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill is None
    assert {s.name for s in plan.snapshots} == {k for k in operations.context.snapshots}

    plan = operations.run(select=["main.stg_orders+"])
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
    assert {s.name for s in plan.snapshots} == plan.selected_models_to_backfill

    plan = operations.run(select=["main.stg_orders+"], exclude=["main.customers"])
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
    assert {s.name for s in plan.snapshots} == plan.selected_models_to_backfill

    plan = operations.run(exclude=["main.customers"])
    assert plan.environment.name == "prod"
    assert console.no_prompts is True
    assert console.no_diff is True
    assert console.auto_apply is True
    assert plan.end_bounded is False
    assert plan.ignore_cron is True
    assert plan.skip_backfill is False
    assert plan.selected_models_to_backfill == {k for k in operations.context.snapshots} - {
        '"jaffle_shop"."main"."customers"'
    }
    assert {s.name for s in plan.snapshots} == plan.selected_models_to_backfill

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

    plan = operations.run(environment="dev", select=["main.stg_orders+"])
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
