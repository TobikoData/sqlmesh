from __future__ import annotations
import typing as t
from rich.progress import Progress
from pathlib import Path
import logging
from sqlmesh_dbt import selectors

if t.TYPE_CHECKING:
    # important to gate these to be able to defer importing sqlmesh until we need to
    from sqlmesh.core.context import Context
    from sqlmesh.dbt.project import Project
    from sqlmesh_dbt.console import DbtCliConsole
    from sqlmesh.core.model import Model
    from sqlmesh.core.plan import Plan

logger = logging.getLogger(__name__)


class DbtOperations:
    def __init__(self, sqlmesh_context: Context, dbt_project: Project, debug: bool = False):
        self.context = sqlmesh_context
        self.project = dbt_project
        self.debug = debug

    def list_(
        self,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
    ) -> None:
        # dbt list prints:
        # - models
        # - "data tests" (audits) for those models
        # it also applies selectors which is useful for testing selectors
        selected_models = list(self._selected_models(select, exclude).values())
        self.console.list_models(selected_models)

    def run(
        self,
        environment: t.Optional[str] = None,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        full_refresh: bool = False,
        empty: bool = False,
    ) -> Plan:
        return self.context.plan(
            **self._plan_options(
                environment=environment,
                select=select,
                exclude=exclude,
                full_refresh=full_refresh,
                empty=empty,
            )
        )

    def _selected_models(
        self, select: t.Optional[t.List[str]] = None, exclude: t.Optional[t.List[str]] = None
    ) -> t.Dict[str, Model]:
        if sqlmesh_selector := selectors.to_sqlmesh(select or [], exclude or []):
            if self.debug:
                self.console.print(f"dbt --select: {select}")
                self.console.print(f"dbt --exclude: {exclude}")
                self.console.print(f"sqlmesh equivalent: '{sqlmesh_selector}'")
            model_selector = self.context._new_selector()
            selected_models = {
                fqn: model
                for fqn, model in self.context.models.items()
                if fqn in model_selector.expand_model_selections([sqlmesh_selector])
            }
        else:
            selected_models = dict(self.context.models)

        return selected_models

    def _plan_options(
        self,
        environment: t.Optional[str] = None,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        empty: bool = False,
        full_refresh: bool = False,
    ) -> t.Dict[str, t.Any]:
        import sqlmesh.core.constants as c

        # convert --select and --exclude to a selector expression for the SQLMesh selector engine
        select_models = None
        if sqlmesh_selector := selectors.to_sqlmesh(select or [], exclude or []):
            select_models = [sqlmesh_selector]

        is_dev = environment and environment != c.PROD
        is_prod = not is_dev

        options: t.Dict[str, t.Any] = {}

        if is_prod or (is_dev and select_models):
            # prod plans should "catch up" before applying the changes so that after the command finishes prod is the latest it can be
            # dev plans *with* selectors should do the same as the user is saying "specifically update these models to the latest"
            # dev plans *without* selectors should just have the defaults of never exceeding prod as the user is saying "just create this env" without focusing on any specific models
            options.update(
                dict(
                    # always catch the data up to latest rather than only operating on what has been loaded before
                    run=True,
                    # don't taking cron schedules into account when deciding what models to run, do everything even if it just ran
                    ignore_cron=True,
                )
            )

        if is_dev:
            options.update(
                dict(
                    # don't create views for all of prod in the dev environment
                    include_unmodified=False,
                    # always plan from scratch against prod rather than planning against the previous state of an existing dev environment
                    # this results in the full scope of changes vs prod always being shown on the local branch
                    create_from=c.PROD,
                    always_recreate_environment=True,
                    # setting enable_preview=None enables dev previews of forward_only changes for dbt projects IF the target engine supports cloning
                    # if we set enable_preview=True here, this enables dev previews in all cases.
                    # In the case of dbt default INCREMENTAL_UNMANAGED models, this will cause incremental models to be fully rebuilt (potentially a very large computation)
                    # just to have the results thrown away on promotion to prod because dev previews are not promotable.
                    #
                    # TODO: if the user "upgrades" to an INCREMENTAL_BY_TIME_RANGE by defining a "time_column", we can inject leak guards to compute
                    # just a preview instead of the whole thing like we would in a native project, but the enable_preview setting is at the plan level
                    # and not the individual model level so we currently have no way of doing this selectively
                    enable_preview=None,
                )
            )

        if empty:
            # dbt --empty adds LIMIT 0 to the queries, resulting in empty tables
            # this lines up with --skip-backfill in SQLMesh
            options["skip_backfill"] = True

            if is_prod:
                # to prevent the following error:
                # > ConfigError: When targeting the production environment either the backfill should not be skipped or
                # > the lack of data gaps should be enforced (--no-gaps flag).
                options["no_gaps"] = True

        if full_refresh:
            # TODO: handling this requires some updates in the engine to enable restatements+changes in the same plan without affecting prod
            # if the plan targets dev
            pass

        return dict(
            environment=environment,
            select_models=select_models,
            # dont output a diff of model changes
            no_diff=True,
            # don't throw up any prompts like "set the effective date" - use defaults
            no_prompts=True,
            # start doing work immediately (since no_diff is set, there isnt really anything for the user to say yes/no to)
            auto_apply=True,
            **options,
        )

    @property
    def console(self) -> DbtCliConsole:
        console = self.context.console
        from sqlmesh_dbt.console import DbtCliConsole

        if not isinstance(console, DbtCliConsole):
            raise ValueError(f"Expecting dbt cli console, got: {console}")

        return console

    def close(self) -> None:
        self.context.close()


def create(
    project_dir: t.Optional[Path] = None,
    profile: t.Optional[str] = None,
    target: t.Optional[str] = None,
    vars: t.Optional[t.Dict[str, t.Any]] = None,
    debug: bool = False,
) -> DbtOperations:
    with Progress(transient=True) as progress:
        # Indeterminate progress bar before SQLMesh import to provide feedback to the user that something is indeed happening
        load_task_id = progress.add_task("Loading engine", total=None)

        from sqlmesh import configure_logging
        from sqlmesh.core.context import Context
        from sqlmesh.dbt.loader import DbtLoader
        from sqlmesh.core.console import set_console
        from sqlmesh_dbt.console import DbtCliConsole
        from sqlmesh.utils.errors import SQLMeshError

        configure_logging(force_debug=debug)
        set_console(DbtCliConsole())

        progress.update(load_task_id, description="Loading project", total=None)

        project_dir = project_dir or Path.cwd()
        init_project_if_required(project_dir)

        sqlmesh_context = Context(
            paths=[project_dir],
            config_loader_kwargs=dict(profile=profile, target=target, variables=vars),
            load=True,
        )

        dbt_loader = sqlmesh_context._loaders[0]
        if not isinstance(dbt_loader, DbtLoader):
            raise SQLMeshError(f"Unexpected loader type: {type(dbt_loader)}")

        # so that DbtOperations can query information from the DBT project files in order to invoke SQLMesh correctly
        dbt_project = dbt_loader._projects[0]

        return DbtOperations(sqlmesh_context, dbt_project, debug=debug)


def init_project_if_required(project_dir: Path) -> None:
    """
    SQLMesh needs a start date to as the starting point for calculating intervals on incremental models, amongst other things

    Rather than forcing the user to update their config manually or having a default that is not saved between runs,
    we can generate a basic SQLMesh config if it doesnt exist.

    This is preferable to trying to inject config into `dbt_project.yml` because it means we have full control over the file
    and dont need to worry about accidentally reformatting it or accidentally clobbering other config
    """
    from sqlmesh.cli.project_init import init_example_project, ProjectTemplate
    from sqlmesh.core.config.common import ALL_CONFIG_FILENAMES
    from sqlmesh.core.console import get_console

    if not any(f.exists() for f in [project_dir / file for file in ALL_CONFIG_FILENAMES]):
        get_console().log_warning("No existing SQLMesh config detected; creating one")
        init_example_project(path=project_dir, engine_type=None, template=ProjectTemplate.DBT)
