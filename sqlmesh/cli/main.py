from __future__ import annotations

import os
import typing as t

import click

from sqlmesh.cli import error_handler
from sqlmesh.cli import options as opt
from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.core.context import Context
from sqlmesh.core.test import run_all_model_tests, run_model_tests
from sqlmesh.utils.date import TimeLike


@click.group(no_args_is_help=True)
@opt.path
@opt.config
@click.pass_context
@error_handler
def cli(ctx, path, config=None) -> None:
    """SQLMesh command line tool."""
    path = os.path.abspath(path)
    if ctx.invoked_subcommand == "init":
        ctx.obj = path
        return

    if ctx.invoked_subcommand == "test" and not config:
        config = "test_config"
    context = Context(path=path, config=config)

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
def init(ctx, template: t.Optional[str] = None) -> None:
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
    ctx,
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
    ctx,
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
def format(ctx) -> None:
    """Format all models in a given directory."""
    ctx.obj.format()


@cli.command("diff")
@opt.environment
@click.pass_context
@error_handler
def diff(ctx, environment: t.Optional[str] = None) -> None:
    """Show the diff between the current context and a given environment."""
    ctx.obj.diff(environment)


@cli.command("plan")
@opt.environment
@opt.start_time
@opt.end_time
@click.option(
    "--from",
    "-f",
    "from_",
    type=str,
    help="The environment to base the plan on instead of local files.",
)
@click.option(
    "--skip-tests",
    help="Skip tests prior to generating the plan if they are defined.",
)
@click.option(
    "--restate-from",
    "-r",
    type=str,
    nargs="*",
    help="Restate all models that depend on these upstream tables. All snapshots that depend on these upstream tables will have their intervals wiped but only the current snapshots will be backfilled.",
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
    "--no-prompts",
    is_flag=True,
    help="Disable interactive prompts for the backfill time range. Please note that if this flag is set and there are uncategorized changes the plan creation will fail.",
)
@click.option(
    "--auto-apply",
    is_flag=True,
    help="Automatically apply the new plan after creation.",
)
@click.pass_context
@error_handler
def plan(ctx, environment: t.Optional[str] = None, **kwargs) -> None:
    """Plan a migration of the current context's models with the given environment."""
    context = ctx.obj
    context.plan(environment, **kwargs)


@cli.command("run")
@opt.start_time
@opt.end_time
@click.option(
    "--global-state",
    is_flag=True,
    help="If set loads the DAG from the persisted state, otherwise loads from the current local state.",
)
@click.pass_context
@error_handler
def run(ctx, environment: t.Optional[str] = None, **kwargs) -> None:
    """Evaluates the DAG of models using the built-in scheduler."""
    context = ctx.obj
    context.run(**kwargs)


@cli.command("dag")
@opt.file
@click.pass_context
@error_handler
def dag(ctx, file) -> None:
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
def test(obj, k, verbose, tests) -> None:
    """Run model unit tests."""
    # Set Python unittest verbosity level
    verbosity = 2 if verbose else 1
    if tests:
        run_model_tests(
            tests=tests,
            snapshots=obj.snapshots,
            engine_adapter=obj.engine_adapter,
            verbosity=verbosity,
            patterns=k,
            ignore_patterns=obj._ignore_patterns,
        )
    else:
        run_all_model_tests(
            path=obj.path,
            snapshots=obj.snapshots,
            engine_adapter=obj.engine_adapter,
            verbosity=verbosity,
            patterns=k,
            ignore_patterns=obj._ignore_patterns,
        )


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
    obj,
    models: t.Tuple[str],
    start: TimeLike,
    end: TimeLike,
    latest: t.Optional[TimeLike] = None,
) -> None:
    """Run audits."""
    obj.audit(models=models, start=start, end=end, latest=latest)


if __name__ == "__main__":
    cli()
