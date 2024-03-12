from __future__ import annotations

import logging
import os
import sys
import typing as t

import click

from sqlmesh import configure_logging
from sqlmesh.cli import error_handler
from sqlmesh.cli import options as opt
from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.core.config import load_configs
from sqlmesh.core.context import Context
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import MissingDependencyError

logger = logging.getLogger(__name__)


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
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug mode.",
)
@click.option(
    "--log-to-stdout",
    is_flag=True,
    help="Display logs in stdout.",
)
@click.pass_context
@error_handler
def cli(
    ctx: click.Context,
    paths: t.List[str],
    config: t.Optional[str] = None,
    gateway: t.Optional[str] = None,
    ignore_warnings: bool = False,
    debug: bool = False,
    log_to_stdout: bool = False,
) -> None:
    """SQLMesh command line tool."""
    if "--help" in sys.argv:
        return

    load = True

    if len(paths) == 1:
        path = os.path.abspath(paths[0])
        if ctx.invoked_subcommand in ("init", "ui"):
            ctx.obj = path
            return
        elif ctx.invoked_subcommand in ("create_external_models", "migrate", "rollback"):
            load = False

    configs = load_configs(config, Context.CONFIG_TYPE, paths)
    log_limit = list(configs.values())[0].log_limit
    configure_logging(debug, ignore_warnings, log_to_stdout, log_limit=log_limit)

    try:
        context = Context(
            paths=paths,
            config=configs,
            gateway=gateway,
            load=load,
        )
    except Exception:
        if debug:
            logger.exception("Failed to initialize SQLMesh context")
        raise

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
    help="Project template. Supported values: airflow, dbt, default, empty.",
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
@click.option("--no-format", is_flag=True, help="Disable fancy formatting of the query.")
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
    no_format: bool = False,
) -> None:
    """Render a model's query, optionally expanding referenced models."""
    rendered = ctx.obj.render(
        model,
        start=start,
        end=end,
        execution_time=execution_time,
        expand=expand,
    )

    sql = rendered.sql(pretty=True, dialect=ctx.obj.config.dialect if dialect is None else dialect)
    if no_format:
        print(sql)
    else:
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
    "--append-newline",
    is_flag=True,
    help="Include a newline at the end of each file.",
    default=None,
)
@click.option(
    "--normalize",
    is_flag=True,
    help="Whether or not to normalize identifiers to lowercase.",
    default=None,
)
@click.option(
    "--pad",
    type=int,
    help="Determines the pad size in a formatted string.",
)
@click.option(
    "--indent",
    type=int,
    help="Determines the indentation size in a formatted string.",
)
@click.option(
    "--normalize-functions",
    type=str,
    help="Whether or not to normalize all function names. Possible values are: 'upper', 'lower'",
)
@click.option(
    "--leading-comma",
    is_flag=True,
    help="Determines whether or not the comma is leading or trailing in select expressions. Default is trailing.",
    default=None,
)
@click.option(
    "--max-text-width",
    type=int,
    help="The max number of characters in a segment before creating new lines in pretty mode.",
)
@click.pass_context
@error_handler
def format(ctx: click.Context, **kwargs: t.Any) -> None:
    """Format all SQL models."""
    ctx.obj.format(**{k: v for k, v in kwargs.items() if v is not None})


@cli.command("diff")
@click.argument("environment")
@click.pass_context
@error_handler
def diff(ctx: click.Context, environment: t.Optional[str] = None) -> None:
    """Show the diff between the local state and the target environment."""
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
    default=None,
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
    default=None,
)
@click.option(
    "--auto-apply",
    is_flag=True,
    help="Automatically apply the new plan after creation.",
    default=None,
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
@click.option(
    "--select-model",
    type=str,
    multiple=True,
    help="Select specific model changes that should be included in the plan.",
)
@click.option(
    "--backfill-model",
    type=str,
    multiple=True,
    help="Backfill only the models whose names match the expression. This is supported only when targeting a development environment.",
)
@click.option(
    "--no-diff",
    is_flag=True,
    help="Hide text differences for changed models.",
    default=None,
)
@click.option(
    "--run",
    is_flag=True,
    help="Run latest intervals as part of the plan application (prod environment only).",
)
@click.option(
    "--enable-preview",
    is_flag=True,
    help="Enable preview for forward-only models when targeting a development environment.",
    default=None,
)
@opt.verbose
@click.pass_context
@error_handler
def plan(
    ctx: click.Context, verbose: bool, environment: t.Optional[str] = None, **kwargs: t.Any
) -> None:
    """Apply local changes to the target environment."""
    context = ctx.obj
    restate_models = kwargs.pop("restate_model") or None
    select_models = kwargs.pop("select_model") or None
    backfill_models = kwargs.pop("backfill_model") or None
    context.console.verbose = verbose
    context.plan(
        environment,
        restate_models=restate_models,
        select_models=select_models,
        backfill_models=backfill_models,
        **kwargs,
    )


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
    """Evaluate missing intervals for the target environment."""
    context = ctx.obj
    success = context.run(environment, **kwargs)
    if not success:
        raise click.ClickException("Run DAG Failed. See output for details.")


@cli.command("invalidate")
@click.argument("environment", required=True)
@click.option(
    "--sync",
    "-s",
    is_flag=True,
    help="Wait for the environment to be deleted before returning. If not specified, the environment will be deleted asynchronously by the janitor process. This option requires a connection to the data warehouse.",
)
@click.pass_context
@error_handler
def invalidate(ctx: click.Context, environment: str, **kwargs: t.Any) -> None:
    """Invalidate the target environment, forcing its removal during the next run of the janitor process."""
    context = ctx.obj
    context.invalidate_environment(environment, **kwargs)


@cli.command("dag")
@click.argument("file", required=True)
@click.option(
    "--select-model",
    type=str,
    multiple=True,
    help="Select specific models to include in the dag.",
)
@click.pass_context
@error_handler
def dag(ctx: click.Context, file: str, select_model: t.List[str]) -> None:
    """Render the DAG as an html file."""
    rendered_dag_path = ctx.obj.render_dag(file, select_model)
    if rendered_dag_path:
        ctx.obj.console.log_success(f"Generated the dag to {rendered_dag_path}")


@cli.command("create_test")
@click.argument("model")
@click.option(
    "-q",
    "--query",
    "queries",
    type=(str, str),
    multiple=True,
    required=True,
    help="Queries that will be used to generate data for the model's dependencies.",
)
@click.option(
    "-o",
    "--overwrite",
    "overwrite",
    is_flag=True,
    default=False,
    help="When true, the fixture file will be overwritten in case it already exists.",
)
@click.option(
    "-v",
    "--var",
    "variables",
    type=(str, str),
    multiple=True,
    help="Key-value pairs that will define variables needed by the model.",
)
@click.option(
    "-p",
    "--path",
    "path",
    help=(
        "The file path corresponding to the fixture, relative to the test directory. "
        "By default, the fixture will be created under the test directory and the file "
        "name will be inferred based on the test's name."
    ),
)
@click.option(
    "-n",
    "--name",
    "name",
    help="The name of the test that will be created. By default, it's inferred based on the model's name.",
)
@click.pass_obj
@error_handler
def create_test(
    obj: Context,
    model: str,
    queries: t.List[t.Tuple[str, str]],
    overwrite: bool = False,
    variables: t.Optional[t.List[t.Tuple[str, str]]] = None,
    name: t.Optional[str] = None,
    path: t.Optional[str] = None,
) -> None:
    """Generate a unit test fixture for a given model."""
    obj.create_test(
        model,
        input_queries=dict(queries),
        overwrite=overwrite,
        variables=dict(variables) if variables else None,
        name=name,
        path=path,
    )


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
    """Run audits for the target model(s)."""
    obj.audit(models=models, start=start, end=end, execution_time=execution_time)


@cli.command("fetchdf")
@click.argument("sql")
@click.pass_context
@error_handler
def fetchdf(ctx: click.Context, sql: str) -> None:
    """Run a SQL query and display the results."""
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


@cli.command("ui")
@click.option(
    "--host",
    type=str,
    default="127.0.0.1",
    help="Bind socket to this host. Default: 127.0.0.1",
)
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Bind socket to this port. Default: 8000",
)
@click.option(
    "--mode",
    type=click.Choice(["ide", "default", "docs", "plan"], case_sensitive=False),
    default="default",
    help="Mode to start the UI in. Default: default",
)
@click.pass_context
@error_handler
def ui(ctx: click.Context, host: str, port: int, mode: str) -> None:
    """Start a browser-based SQLMesh UI."""
    try:
        import uvicorn
    except ModuleNotFoundError as e:
        raise MissingDependencyError(
            "Missing UI dependencies. Run `pip install 'sqlmesh[web]'` to install them."
        ) from e

    os.environ["PROJECT_PATH"] = ctx.obj
    os.environ["UI_MODE"] = mode
    if ctx.parent:
        config = ctx.parent.params.get("config")
        gateway = ctx.parent.params.get("gateway")
        if config:
            os.environ["CONFIG"] = config
        if gateway:
            os.environ["GATEWAY"] = gateway
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
@click.option(
    "--show-sample",
    is_flag=True,
    help="Show a sample of the rows that differ. With many columns, the output can be very wide.",
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


@cli.command("rewrite")
@click.argument("sql")
@click.option(
    "--read",
    type=str,
    help="The input dialect of the sql string.",
)
@click.option(
    "--write",
    type=str,
    help="The output dialect of the sql string.",
)
@click.pass_obj
@error_handler
def rewrite(obj: Context, sql: str, read: str = "", write: str = "") -> None:
    """Rewrite a SQL expression with semantic references into an executable query.

    https://sqlmesh.readthedocs.io/en/latest/concepts/metrics/overview/
    """
    obj.console.show_sql(
        obj.rewrite(sql, dialect=read).sql(pretty=True, dialect=write or obj.config.dialect),
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


@cli.command("clean")
@click.pass_obj
@error_handler
def clean(obj: Context) -> None:
    """Clears the SQLMesh cache and any build artifacts."""
    obj.clear_caches()
