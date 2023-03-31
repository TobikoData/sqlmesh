from __future__ import annotations

import os
import sys
import typing as t

import click

from sqlmesh.cli import error_handler
from sqlmesh.cli import options as opt
from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.core.context import Context
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import MissingDependencyError


@click.group(no_args_is_help=True)
@opt.path
@opt.config
@click.option(
    "--connection",
    type=str,
    help="The name of the connection.",
)
@click.option(
    "--test-connection",
    type=str,
    help="The name of the connection to use for tests.",
)
@click.pass_context
@error_handler
def cli(
    ctx: click.Context,
    path: str,
    config: t.Optional[str] = None,
    connection: t.Optional[str] = None,
    test_connection: t.Optional[str] = None,
) -> None:
    """SQLMesh command line tool."""
    if ctx.invoked_subcommand == "version":
        return

    path = os.path.abspath(path)
    if ctx.invoked_subcommand == "init":
        ctx.obj = path
        return

    # Delegates the execution of the --help option to the corresponding subcommand
    if "--help" in sys.argv:
        return

    context = Context(
        path=path,
        config=config,
        connection=connection,
        test_connection=test_connection,
    )

    if not context.models:
        raise click.ClickException(
            f"`{path}` doesn't seem to have any models... cd into the proper directory or specify the path with --path."
        )

    ctx.obj = context


@cli.command("init")
@click.option(
    "-t",
    "--template",
    type=str,
    help="Project template. Support values: airflow, default.",
)
@click.pass_context
@error_handler
def init(ctx: click.Context, template: t.Optional[str] = None) -> None:
    """Create a new SQLMesh repository."""
    try:
        project_template = ProjectTemplate(template.lower() if template else "default")
    except ValueError:
        raise click.ClickException(f"Invalid project template '{template}'")
    init_example_project(ctx.obj, template=project_template)


@cli.command("render")
@click.argument("model")
@opt.start_time
@opt.end_time
@opt.latest_time
@opt.expand
@click.pass_context
@error_handler
def render(
    ctx: click.Context,
    model: str,
    start: TimeLike,
    end: TimeLike,
    latest: t.Optional[TimeLike] = None,
    expand: t.Optional[t.Union[bool, t.Iterable[str]]] = None,
) -> None:
    """Renders a model's query, optionally expanding referenced models."""
    snapshot = ctx.obj.snapshots.get(model)

    if not snapshot:
        raise click.ClickException(f"Model `{model}` not found.")

    rendered = ctx.obj.render(
        snapshot,
        start=start,
        end=end,
        latest=latest,
        expand=expand,
    )

    sql = rendered.sql(pretty=True, dialect=ctx.obj.dialect)
    ctx.obj.console.show_sql(sql)


@cli.command("evaluate")
@click.argument("model")
@opt.start_time
@opt.end_time
@opt.latest_time
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
    latest: t.Optional[TimeLike] = None,
    limit: t.Optional[int] = None,
) -> None:
    """Evaluate a model and return a dataframe with a default limit of 1000."""
    df = ctx.obj.evaluate(
        model,
        start=start,
        end=end,
        latest=latest,
        limit=limit,
    )
    ctx.obj.console.log_success(df)


@cli.command("format")
@click.pass_context
@error_handler
def format(ctx: click.Context) -> None:
    """Format all models in a given directory."""
    ctx.obj.format()


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
@click.option(
    "--create-from",
    type=str,
    help="The environment to create the target environment from if it doesn't exist. Default: prod.",
)
@click.option(
    "--skip-tests",
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
@click.option("--skip-janitor", is_flag=True, help="Skip the jantitor task.")
@click.pass_context
@error_handler
def run(ctx: click.Context, environment: t.Optional[str] = None, **kwargs: t.Any) -> None:
    """Evaluates the DAG of models using the built-in scheduler."""
    context = ctx.obj
    context.run(environment, **kwargs)


@cli.command("dag")
@opt.file
@click.pass_context
@error_handler
def dag(ctx: click.Context, file: str) -> None:
    """
    Renders the dag using graphviz.

    This command requires a manual install of both the python and system graphviz package.
    """
    ctx.obj.render_dag(file)


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
@opt.latest_time
@click.pass_obj
@error_handler
def audit(
    obj: Context,
    models: t.Iterator[str],
    start: TimeLike,
    end: TimeLike,
    latest: t.Optional[TimeLike] = None,
) -> None:
    """Run audits."""
    obj.audit(models=models, start=start, end=end, latest=latest)


@cli.command("fetchdf")
@click.argument("sql")
@click.pass_context
@error_handler
def fetchdf(ctx: click.Context, sql: str) -> None:
    """Runs a sql query and displays the results."""
    context = ctx.obj
    context.console.log_success(context.fetchdf(sql))


@cli.command("version")
@error_handler
def version() -> None:
    """Print version."""
    try:
        from sqlmesh import __version__

        print(__version__)
    except ImportError:
        print("Version is not available")


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
@click.pass_obj
@error_handler
def ide(
    obj: Context,
    host: t.Optional[str],
    port: t.Optional[int],
) -> None:
    """Start a browser-based SQLMesh IDE."""
    try:
        import uvicorn
    except ModuleNotFoundError as e:
        raise MissingDependencyError(
            "Missing IDE dependencies. Run `pip install sqlmesh[web]` to install them."
        ) from e

    host = host or "127.0.0.1"
    port = 8000 if port is None else port
    os.environ["PROJECT_PATH"] = str(obj.path)
    uvicorn.run("web.server.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    cli()
