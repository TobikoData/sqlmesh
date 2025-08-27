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
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        full_refresh: bool = False,
    ) -> None:
        select_models = None

        if sqlmesh_selector := selectors.to_sqlmesh(select or [], exclude or []):
            select_models = [sqlmesh_selector]

        self.context.plan(
            select_models=select_models,
            run=True,
            no_diff=True,
            no_prompts=True,
            auto_apply=True,
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
