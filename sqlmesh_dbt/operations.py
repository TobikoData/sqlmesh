from __future__ import annotations
import typing as t
from rich.progress import Progress
from pathlib import Path

if t.TYPE_CHECKING:
    # important to gate these to be able to defer importing sqlmesh until we need to
    from sqlmesh.core.context import Context
    from sqlmesh.dbt.project import Project
    from sqlmesh_dbt.console import DbtCliConsole


class DbtOperations:
    def __init__(self, sqlmesh_context: Context, dbt_project: Project):
        self.context = sqlmesh_context
        self.project = dbt_project

    def list_(self) -> None:
        for _, model in self.context.models.items():
            self.console.print(model.name)

    def run(self, select: t.Optional[str] = None, full_refresh: bool = False) -> None:
        # A dbt run both updates data and changes schemas and has no way of rolling back so more closely maps to a SQLMesh forward-only plan
        # TODO: if --full-refresh specified, mark incrementals as breaking instead of forward_only?

        # TODO: we need to either convert DBT selector syntax to SQLMesh selector syntax
        # or make the model selection engine configurable
        select_models = None
        if select:
            if "," in select:
                select_models = select.split(",")
            else:
                select_models = select.split(" ")

        self.context.plan(
            select_models=select_models,
            forward_only=True,
            no_auto_categorization=True,  # everything is breaking / foward-only
            effective_from=self.context.config.model_defaults.start,
            run=True,
            auto_apply=True,
        )

    @property
    def console(self) -> DbtCliConsole:
        console = self.context.console
        from sqlmesh_dbt.console import DbtCliConsole

        if not isinstance(console, DbtCliConsole):
            raise ValueError(f"Expecting dbt cli console, got: {console}")

        return console


def create(
    project_dir: t.Optional[Path] = None, profiles_dir: t.Optional[Path] = None, debug: bool = False
) -> DbtOperations:
    with Progress(transient=True) as progress:
        # Indeterminate progress bar before SQLMesh import to provide feedback to the user that something is indeed happening
        load_task_id = progress.add_task("Loading engine", total=None)

        from sqlmesh import configure_logging
        from sqlmesh.core.context import Context
        from sqlmesh.dbt.loader import sqlmesh_config, DbtLoader
        from sqlmesh.core.console import set_console
        from sqlmesh_dbt.console import DbtCliConsole
        from sqlmesh.utils.errors import SQLMeshError

        configure_logging(force_debug=debug)
        set_console(DbtCliConsole())

        progress.update(load_task_id, description="Loading project", total=None)

        # inject default start date if one is not specified to prevent the user from having to do anything
        _inject_default_start_date(project_dir)

        config = sqlmesh_config(
            project_root=project_dir,
            # do we want to use a local duckdb for state?
            # warehouse state has a bunch of overhead to initialize, is slow for ongoing operations and will create tables that perhaps the user was not expecting
            # on the other hand, local state is not portable
            state_connection=None,
        )

        sqlmesh_context = Context(
            config=config,
            load=True,
        )

        # this helps things which want a default project-level start date, like the "effective from date" for forward-only plans
        if not sqlmesh_context.config.model_defaults.start:
            min_start_date = min(
                (
                    model.start
                    for model in sqlmesh_context.models.values()
                    if model.start is not None
                ),
                default=None,
            )
            sqlmesh_context.config.model_defaults.start = min_start_date

        dbt_loader = sqlmesh_context._loaders[0]
        if not isinstance(dbt_loader, DbtLoader):
            raise SQLMeshError(f"Unexpected loader type: {type(dbt_loader)}")

        # so that DbtOperations can query information from the DBT project files in order to invoke SQLMesh correctly
        dbt_project = dbt_loader._projects[0]

        return DbtOperations(sqlmesh_context, dbt_project)


def _inject_default_start_date(project_dir: t.Optional[Path] = None) -> None:
    """
    SQLMesh needs a start date to as the starting point for calculating intervals on incremental models

    Rather than forcing the user to update their config manually or having a default that is not saved between runs,
    we can inject it automatically to the dbt_project.yml file
    """
    from sqlmesh.dbt.project import PROJECT_FILENAME, load_yaml
    from sqlmesh.utils.yaml import dump
    from sqlmesh.utils.date import yesterday_ds

    project_yaml_path = (project_dir or Path.cwd()) / PROJECT_FILENAME
    if project_yaml_path.exists():
        loaded_project_file = load_yaml(project_yaml_path)
        start_date_keys = ("start", "+start")
        if "models" in loaded_project_file and all(
            k not in loaded_project_file["models"] for k in start_date_keys
        ):
            loaded_project_file["models"]["+start"] = yesterday_ds()
            # todo: this may format the file differently, is that acceptable?
            with project_yaml_path.open("w") as f:
                dump(loaded_project_file, f)
