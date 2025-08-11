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
        from sqlmesh.core.config import ModelDefaultsConfig

        configure_logging(force_debug=debug)
        set_console(DbtCliConsole())

        progress.update(load_task_id, description="Loading project", total=None)

        cli_config = get_or_create_sqlmesh_config(project_dir)
        # todo: we will need to build this out when we start storing more than model_defaults
        model_defaults = (
            ModelDefaultsConfig.model_validate(cli_config["model_defaults"])
            if "model_defaults" in cli_config
            else None
        )

        config = sqlmesh_config(
            project_root=project_dir,
            # This triggers warehouse state. Users will probably find this very slow
            state_connection=None,
            model_defaults=model_defaults,
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


def get_or_create_sqlmesh_config(project_dir: t.Optional[Path] = None) -> t.Dict[str, t.Any]:
    """
    SQLMesh needs a start date to as the starting point for calculating intervals on incremental models, amongst other things

    Rather than forcing the user to update their config manually or having a default that is not saved between runs,
    we can store sqlmesh-specific things in a `sqlmesh.yaml` file. This is preferable to trying to inject config into `dbt_project.yml`
    because it means we have full control over the file and dont need to worry about accidentally reformatting it or accidentally
    clobbering other config
    """
    import sqlmesh.utils.yaml as yaml
    from sqlmesh.utils.date import yesterday_ds
    from sqlmesh.core.config import ModelDefaultsConfig

    potential_filenames = [
        (project_dir or Path.cwd()) / f"sqlmesh.{ext}" for ext in ("yaml", "yml")
    ]

    sqlmesh_yaml_file = next((f for f in potential_filenames if f.exists()), potential_filenames[0])

    if not sqlmesh_yaml_file.exists():
        with sqlmesh_yaml_file.open("w") as f:
            yaml.dump({"model_defaults": ModelDefaultsConfig(start=yesterday_ds()).dict()}, f)

    return yaml.load(sqlmesh_yaml_file, render_jinja=False)
