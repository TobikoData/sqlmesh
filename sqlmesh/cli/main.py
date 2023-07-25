from __future__ import annotations

import logging
import os
import sys
import typing as t

import click

from sqlmesh import enable_logging
from sqlmesh.cli import error_handler
from sqlmesh.cli import options as opt
from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.core.context import Context
from sqlmesh.utils import debug_mode_enabled
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import MissingDependencyError


def _sqlmesh_version() -> str:
    try:
        from sqlmesh import __version__

        return __version__
    except ImportError:
        return "0.0.0"


@click.group(no_args_is_help=True)
@click.version_option(version=_sqlmesh_version(), message="%(version)s")
@opt.paths
@opt.config
@click.option(
    "--gateway",
    type=str,
    help="The name of the gateway.",
)
@click.option(
    "--ignore-warnings",
    is_flag=True,
    help="Ignore warnings.",
)
@click.pass_context
@error_handler
def cli(
    ctx: click.Context,
    paths: t.List[str],
    config: t.Optional[str] = None,
    gateway: t.Optional[str] = None,
    ignore_warnings: bool = False,
) -> None:
    """SQLMesh command line tool."""
    if ctx.invoked_subcommand == "version":
        return

    load = True

    if len(paths) == 1:
        path = os.path.abspath(paths[0])
        if ctx.invoked_subcommand == "init":
            ctx.obj = path
            return
        elif ctx.invoked_subcommand in ("create_external_models", "migrate", "rollback"):
            load = False

    # Delegates the execution of the --help option to the corresponding subcommand
    if "--help" in sys.argv:
        return

    if debug_mode_enabled():
        import faulthandler
        import signal

        # Enable threadumps.
        faulthandler.enable()
        # Windows doesn't support register so we check for it here
        if hasattr(faulthandler, "register"):
            faulthandler.register(signal.SIGUSR1.value)
        enable_logging(level=logging.DEBUG)
    elif ignore_warnings:
        logging.getLogger().setLevel(logging.ERROR)

    context = Context(
        paths=paths,
        config=config,
        gateway=gateway,
        load=load,
    )

    if load and not context.models:
        raise click.ClickException(
            f"`{paths}` doesn't seem to have any models... cd into the proper directory or specify the path(s) with -p."
        )

    ctx.obj = context


@cli.command("init")
@click.argument("sql_dialect", required=False)
@click.option(
    "-t",
    "--template",
    type=str,
    help="Project template. Supported values: airflow, dbt, default.",
)
@click.pass_context
@error_handler
def init(
    ctx: click.Context, sql_dialect: t.Optional[str] = None, template: t.Optional[str] = None
) -> None:
    """Create a new SQLMesh repository."""
    try:
        project_template = ProjectTemplate(template.lower() if template else "default")
    except ValueError:
        raise click.ClickException(f"Invalid project template '{template}'")
    init_example_project(ctx.obj, dialect=sql_dialect, template=project_template)


@cli.command("render")
@click.argument("model")
@opt.start_time
@opt.end_time
@opt.execution_time
@opt.expand
@click.option(
    "--dialect",
    type=str,
    help="The SQL dialect to render the query as.",
)
@click.pass_context
@error_handler
def render(
    ctx: click.Context,
    model: str,
    start: TimeLike,
    end: TimeLike,
    execution_time: t.Optional[TimeLike] = None,
    expand: t.Optional[t.Union[bool, t.Iterable[str]]] = None,
    dialect: t.Optional[str] = None,
) -> None:
    """Renders a model's query, optionally expanding referenced models."""
    rendered = ctx.obj.render(
        model,
        start=start,
        end=end,
        execution_time=execution_time,
        expand=expand,
    )

    sql = rendered.sql(pretty=True, dialect=ctx.obj.config.dialect if dialect is None else dialect)
    ctx.obj.console.show_sql(sql)


@cli.command("evaluate")
@click.argument("model")
@opt.start_time
@opt.end_time
@opt.execution_time
@click.option(
    "--limit",
    type=int,
    help="The number of rows which the query should be limited to.",
)
@click.pass_context
@error_handler
def evaluate(
    ctx: click.Context,
    model: str,
    start: TimeLike,
    end: TimeLike,
    execution_time: t.Optional[TimeLike] = None,
    limit: t.Optional[int] = None,
) -> None:
    """Evaluate a model and return a dataframe with a default limit of 1000."""
    df = ctx.obj.evaluate(
        model,
        start=start,
        end=end,
        execution_time=execution_time,
        limit=limit,
    )
    ctx.obj.console.log_success(df)


@cli.command("format")
@click.option(
    "-t",
    "--transpile",
    type=str,
    help="Transpile project models to the specified dialect.",
)
@click.option(
    "--new-line",
    is_flag=True,
    help="Include a new line at the end of each file.",
)
@click.pass_context
@error_handler
def format(ctx: click.Context, transpile: t.Optional[str] = None, new_line: bool = False) -> None:
    """Format all models in a given directory."""
    ctx.obj.format(transpile, new_line)


@cli.command("diff")
@click.argument("environment")
@click.pass_context
@error_handler
def diff(ctx: click.Context, environment: t.Optional[str] = None) -> None:
    """Show the diff between the current context and a given environment."""
    ctx.obj.diff(environment)


@cli.command("plan")
@click.argument("environment", required=False)
@opt.start_time
@opt.end_time
@opt.execution_time
@click.option(
    "--create-from",
    type=str,
    help="The environment to create the target environment from if it doesn't exist. Default: prod.",
)
@click.option(
    "--skip-tests",
    is_flag=True,
    help="Skip tests prior to generating the plan if they are defined.",
)
@click.option(
    "--restate-model",
    "-r",
    type=str,
    multiple=True,
    help="Restate data for specified models and models downstream from the one specified. For production environment, all related model versions will have their intervals wiped, but only the current versions will be backfilled. For development environment, only the current model versions will be affected.",
)
@click.option(
    "--no-gaps",
    is_flag=True,
    help="Ensure that new snapshots have no data gaps when comparing to existing snapshots for matching models in the target environment.",
)
@click.option(
    "--skip-backfill",
    is_flag=True,
    help="Skip the backfill step.",
)
@click.option(
    "--forward-only",
    is_flag=True,
    help="Create a plan for forward-only changes.",
)
@click.option(
    "--effective-from",
    type=str,
    required=False,
    help="The effective date from which to apply forward-only changes on production.",
)
@click.option(
    "--no-prompts",
    is_flag=True,
    help="Disable interactive prompts for the backfill time range. Please note that if this flag is set and there are uncategorized changes, plan creation will fail.",
)
@click.option(
    "--auto-apply",
    is_flag=True,
    help="Automatically apply the new plan after creation.",
)
@click.option(
    "--no-auto-categorization",
    is_flag=True,
    help="Disable automatic change categorization.",
    default=None,
)
@click.option(
    "--include-unmodified",
    is_flag=True,
    help="Include unmodified models in the target environment.",
    default=None,
)
@click.pass_context
@error_handler
def plan(ctx: click.Context, environment: t.Optional[str] = None, **kwargs: t.Any) -> None:
    """Plan a migration of the current context's models with the given environment."""
    context = ctx.obj
    restate_models = kwargs.pop("restate_model", None)
    context.plan(environment, restate_models=restate_models, **kwargs)


@cli.command("run")
@click.argument("environment", required=False)
@opt.start_time
@opt.end_time
@click.option("--skip-janitor", is_flag=True, help="Skip the janitor task.")
@click.option(
    "--ignore-cron",
    is_flag=True,
    help="Run for all missing intervals, ignoring individual cron schedules.",
)
@click.pass_context
@error_handler
def run(ctx: click.Context, environment: t.Optional[str] = None, **kwargs: t.Any) -> None:
    """Evaluates the DAG of models using the built-in scheduler."""
    context = ctx.obj
    context.run(environment, **kwargs)


@cli.command("invalidate")
@click.argument("environment", required=True)
@click.pass_context
@error_handler
def invalidate(ctx: click.Context, environment: str) -> None:
    """Invalidates the target environment, forcing its removal during the next run of the janitor process."""
    context = ctx.obj
    context.invalidate_environment(environment)


@cli.command("dag")
@opt.file
@click.pass_context
@error_handler
def dag(ctx: click.Context, file: str) -> None:
    """
    Renders the dag using graphviz.

    This command requires a manual install of both the python and system graphviz package.
    """
    rendered_dag_path = ctx.obj.render_dag(file)
    if rendered_dag_path:
        ctx.obj.console.log_success(f"Generated the dag to {rendered_dag_path}")


@cli.command("test")
@opt.match_pattern
@opt.verbose
@click.argument("tests", nargs=-1)
@click.pass_obj
@error_handler
def test(obj: Context, k: t.List[str], verbose: bool, tests: t.List[str]) -> None:
    """Run model unit tests."""
    # Set Python unittest verbosity level
    result = obj.test(match_patterns=k, tests=tests, verbose=verbose)
    if not result.wasSuccessful():
        exit(1)


@cli.command("audit")
@click.option(
    "--model",
    "models",
    multiple=True,
    help="A model to audit. Multiple models can be audited.",
)
@opt.start_time
@opt.end_time
@opt.execution_time
@click.pass_obj
@error_handler
def audit(
    obj: Context,
    models: t.Iterator[str],
    start: TimeLike,
    end: TimeLike,
    execution_time: t.Optional[TimeLike] = None,
) -> None:
    """Run audits."""
    obj.audit(models=models, start=start, end=end, execution_time=execution_time)


@cli.command("fetchdf")
@click.argument("sql")
@click.pass_context
@error_handler
def fetchdf(ctx: click.Context, sql: str) -> None:
    """Runs a sql query and displays the results."""
    context = ctx.obj
    context.console.log_success(context.fetchdf(sql))


@cli.command("info")
@click.pass_obj
@error_handler
def info(obj: Context) -> None:
    """
    Print information about a SQLMesh project.

    Includes counts of project models and macros and connection tests for the data warehouse and test runner.
    """
    obj.print_info()


@cli.command("ide")
@click.option(
    "--host",
    type=str,
    help="Bind socket to this host. Default: 127.0.0.1",
)
@click.option(
    "--port",
    type=int,
    help="Bind socket to this port. Default: 8000",
)
@click.pass_context
@error_handler
def ide(
    ctx: click.Context,
    host: t.Optional[str],
    port: t.Optional[int],
) -> None:
    """
    Start a browser-based SQLMesh IDE.

    WARNING: soft-deprecated, please use `sqlmesh ui` instead.
    """
    click.echo(
        click.style("WARNING", fg="yellow") + ":  soft-deprecated, please use `sqlmesh ui` instead."
    )

    ctx.forward(ui)


@cli.command("ui")
@click.option(
    "--host",
    type=str,
    help="Bind socket to this host. Default: 127.0.0.1",
)
@click.option(
    "--port",
    type=int,
    help="Bind socket to this port. Default: 8000",
)
@click.pass_obj
@error_handler
def ui(
    obj: Context,
    host: t.Optional[str],
    port: t.Optional[int],
) -> None:
    """Start a browser-based SQLMesh UI."""
    try:
        import uvicorn
    except ModuleNotFoundError as e:
        raise MissingDependencyError(
            "Missing UI dependencies. Run `pip install 'sqlmesh[web]'` to install them."
        ) from e

    host = host or "127.0.0.1"
    port = 8000 if port is None else port
    os.environ["PROJECT_PATH"] = str(obj.path)
    uvicorn.run(
        "web.server.main:app",
        host=host,
        port=port,
        log_level="info",
        timeout_keep_alive=300,
    )


@cli.command("migrate")
@click.pass_context
@error_handler
def migrate(ctx: click.Context) -> None:
    """Migrate SQLMesh to the current running version."""
    ctx.obj.migrate()


@cli.command("rollback")
@click.pass_obj
@error_handler
def rollback(obj: Context) -> None:
    """Rollback SQLMesh to the previous migration."""
    obj.rollback()


@cli.command("create_external_models")
@click.pass_obj
@error_handler
def create_external_models(obj: Context) -> None:
    """Create a schema file containing external model schemas."""
    obj.create_external_models()


@cli.command("table_diff")
@click.argument("source_to_target", required=True, metavar="SOURCE:TARGET")
@click.argument("model", required=False)
@click.option(
    "-o",
    "--on",
    type=str,
    multiple=True,
    help="The column to join on. Can be specified multiple times. The model grain will be used if not specified.",
)
@click.option(
    "--where",
    type=str,
    help="An optional where statement to filter results.",
)
@click.option(
    "--limit",
    type=int,
    default=20,
    help="The limit of the sample dataframe.",
)
@click.pass_obj
@error_handler
def table_diff(
    obj: Context, source_to_target: str, model: t.Optional[str], **kwargs: t.Any
) -> None:
    """Show the diff between two tables."""
    source, target = source_to_target.split(":")
    obj.table_diff(
        source=source,
        target=target,
        model_or_snapshot=model,
        **kwargs,
    )


@cli.command("prompt")
@click.argument("prompt")
@click.option(
    "-e",
    "--evaluate",
    is_flag=True,
    help="Evaluate the generated SQL query and display the results.",
)
@click.option(
    "-t",
    "--temperature",
    type=float,
    help="Sampling temperature. 0.0 - precise and predictable, 0.5 - balanced, 1.0 - creative. Default: 0.7",
    default=0.7,
)
@opt.verbose
@click.pass_context
@error_handler
def prompt(
    ctx: click.Context, prompt: str, evaluate: bool, temperature: float, verbose: bool
) -> None:
    """Uses LLM to generate a SQL query from a prompt."""
    from sqlmesh.integrations.llm import LLMIntegration

    context = ctx.obj

    llm_integration = LLMIntegration(
        context.models.values(),
        context.engine_adapter.dialect,
        temperature=temperature,
        verbose=verbose,
    )
    query = llm_integration.query(prompt)

    context.console.log_status_update(query)
    if evaluate:
        context.console.log_success(context.fetchdf(query))


if __name__ == "__main__":
    cli()
