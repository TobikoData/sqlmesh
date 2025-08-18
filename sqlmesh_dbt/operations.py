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
            no_auto_categorization=True,  # everything is breaking / foward-only
            run=True,
            no_diff=True,
            no_prompts=True,
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
    project_dir: t.Optional[Path] = None,
    profile: t.Optional[str] = None,
    target: t.Optional[str] = None,
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
            config_loader_kwargs=dict(profile=profile, target=target),
            load=True,
        )

        dbt_loader = sqlmesh_context._loaders[0]
        if not isinstance(dbt_loader, DbtLoader):
            raise SQLMeshError(f"Unexpected loader type: {type(dbt_loader)}")

        # so that DbtOperations can query information from the DBT project files in order to invoke SQLMesh correctly
        dbt_project = dbt_loader._projects[0]

        return DbtOperations(sqlmesh_context, dbt_project)


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
