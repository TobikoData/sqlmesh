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
    from sqlmesh.core.plan import Plan, PlanBuilder

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
        models: t.Optional[t.List[str]] = None,
        resource_type: t.Optional[str] = None,
    ) -> None:
        # dbt list prints:
        # - models
        # - "data tests" (audits) for those models
        # it also applies selectors which is useful for testing selectors
        selected_models = list(
            self._selected_models(select, exclude, models, resource_type).values()
        )
        self.console.list_models(
            selected_models, {k: v.node for k, v in self.context.snapshots.items()}
        )

    def run(
        self,
        environment: t.Optional[str] = None,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        models: t.Optional[t.List[str]] = None,
        resource_type: t.Optional[str] = None,
        full_refresh: bool = False,
        empty: bool = False,
    ) -> Plan:
        consolidated_select, consolidated_exclude = selectors.consolidate(
            select or [], exclude or [], models or [], resource_type
        )

        plan_builder = self._plan_builder(
            environment=environment,
            select=consolidated_select,
            exclude=consolidated_exclude,
            full_refresh=full_refresh,
            empty=empty,
        )

        plan = plan_builder.build()

        self.console.plan(
            plan_builder,
            default_catalog=self.context.default_catalog,
            # start doing work immediately (since no_diff is set, there isnt really anything for the user to say yes/no to)
            auto_apply=True,
            # dont output a diff of model changes
            no_diff=True,
            # don't throw up any prompts like "set the effective date" - use defaults
            no_prompts=True,
        )

        return plan

    def _plan_builder(
        self,
        environment: t.Optional[str] = None,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        full_refresh: bool = False,
        empty: bool = False,
    ) -> PlanBuilder:
        return self.context.plan_builder(
            **self._plan_builder_options(
                environment=environment,
                select=select,
                exclude=exclude,
                full_refresh=full_refresh,
                empty=empty,
            )
        )

    def _selected_models(
        self,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        models: t.Optional[t.List[str]] = None,
        resource_type: t.Optional[str] = None,
    ) -> t.Dict[str, Model]:
        if sqlmesh_selector := selectors.to_sqlmesh(
            *selectors.consolidate(select or [], exclude or [], models or [], resource_type)
        ):
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

    def _plan_builder_options(
        self,
        # upstream dbt options
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        empty: bool = False,
        full_refresh: bool = False,
        # sqlmesh extra options
        environment: t.Optional[str] = None,
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
                    # always plan from scratch against prod. note that this is coupled with the `always_recreate_environment=True` setting in the default config file.
                    # the result is that rather than planning against the previous state of an existing dev environment, the full scope of changes vs prod are always shown
                    create_from=c.PROD,
                    # Always enable dev previews for incremental / forward-only models.
                    # Due to how DBT does incrementals (INCREMENTAL_UNMANAGED on the SQLMesh engine), this will result in the full model being refreshed
                    # with the entire dataset, which can potentially be very large. If this is undesirable, users have two options:
                    #  - work around this using jinja to conditionally add extra filters to the WHERE clause or a LIMIT to the model query
                    #  - upgrade to SQLMesh's incremental models, where we have variables for the start/end date and inject leak guards to
                    #    limit the amount of data backfilled
                    #
                    # Note: enable_preview=True is *different* behaviour to the `sqlmesh` CLI, which uses enable_preview=None.
                    # This means the `sqlmesh` CLI will only enable dev previews for dbt projects if the target adapter supports cloning,
                    # whereas we enable it unconditionally here
                    enable_preview=True,
                )
            )

        if empty:
            # `dbt --empty` adds LIMIT 0 to the queries, resulting in empty tables. In addition, it happily clobbers existing tables regardless of if they are populated.
            # This *partially* lines up with --skip-backfill in SQLMesh, which indicates to not populate tables if they happened to be created/updated as part of this plan.
            # However, if a table already exists and has data in it, there is no change so SQLMesh will not recreate the table and thus it will not be cleared.
            # Currently, SQLMesh has no way to say "restate with empty data", because --restate-model coupled with --skip-backfill ends up being a no-op
            options["skip_backfill"] = True

            self.console.log_warning(
                "dbt's `--empty` drops the tables for all selected models and replaces them with empty ones.\n"
                "This can easily result in accidental data loss, so SQLMesh limits this to only new or modified models and leaves the tables for existing unmodified models alone.\n\n"
                "If you were creating empty tables to preview model changes, please consider using `--environment` to preview these changes in an isolated Virtual Data Environment instead.\n\n"
                "Otherwise, if you really do want dbt's `--empty` behaviour of clearing every selected table, please file an issue on GitHub so we can better understand the use-case.\n"
            )

            if full_refresh:
                # --full-refresh is implemented in terms of "add every model as a restatement"
                # however, `--empty` sets skip_backfill=True, which causes the BackfillStage of the plan to be skipped.
                # the re-processing of data intervals happens in the BackfillStage, so if it gets skipped, restatements become a no-op
                raise ValueError("`--full-refresh` alongside `--empty` is not currently supported.")

        if full_refresh:
            options.update(
                dict(
                    # Add every selected model as a restatement to force them to get repopulated from scratch
                    restate_models=[m.dbt_fqn for m in self.context.models.values() if m.dbt_fqn]
                    if not select_models
                    else select_models,
                    # by default in SQLMesh, restatements only operate on what has been committed to state.
                    # in order to emulate dbt, we need to use the local filesystem instead, so we override this default
                    always_include_local_changes=True,
                )
            )

        return dict(
            environment=environment,
            select_models=select_models,
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
    profiles_dir: t.Optional[Path] = None,
    profile: t.Optional[str] = None,
    target: t.Optional[str] = None,
    vars: t.Optional[t.Dict[str, t.Any]] = None,
    threads: t.Optional[int] = None,
    debug: bool = False,
    log_level: t.Optional[str] = None,
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
        from sqlmesh.core.selector import DbtSelector

        # clear any existing handlers set up by click/rich as defaults so that once SQLMesh logging config is applied,
        # we dont get duplicate messages logged from things like console.log_warning()
        root_logger = logging.getLogger()
        while root_logger.hasHandlers():
            root_logger.removeHandler(root_logger.handlers[0])

        configure_logging(force_debug=debug, log_level=log_level)
        set_console(DbtCliConsole())

        progress.update(load_task_id, description="Loading project", total=None)

        project_dir = project_dir or Path.cwd()
        init_project_if_required(project_dir)

        sqlmesh_context = Context(
            paths=[project_dir],
            config_loader_kwargs=dict(
                profile=profile,
                target=target,
                variables=vars,
                threads=threads,
                profiles_dir=profiles_dir,
            ),
            load=True,
            # DbtSelector selects based on dbt model fqn's rather than SQLMesh model names
            selector=DbtSelector,
        )

        dbt_loader = sqlmesh_context._loaders[0]
        if not isinstance(dbt_loader, DbtLoader):
            raise SQLMeshError(f"Unexpected loader type: {type(dbt_loader)}")

        # so that DbtOperations can query information from the DBT project files in order to invoke SQLMesh correctly
        dbt_project = dbt_loader._projects[0]

        return DbtOperations(sqlmesh_context, dbt_project, debug=debug)


def init_project_if_required(project_dir: Path, start: t.Optional[str] = None) -> None:
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
        init_example_project(
            path=project_dir, engine_type=None, template=ProjectTemplate.DBT, start=start
        )
