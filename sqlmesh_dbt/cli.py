import typing as t
import sys
import click
from sqlmesh_dbt.operations import DbtOperations, create
from sqlmesh_dbt.error import cli_global_error_handler, ErrorHandlingGroup
from pathlib import Path
from sqlmesh_dbt.options import YamlParamType
import functools


def _get_dbt_operations(
    ctx: click.Context, vars: t.Optional[t.Dict[str, t.Any]], threads: t.Optional[int] = None
) -> DbtOperations:
    if not isinstance(ctx.obj, functools.partial):
        raise ValueError(f"Unexpected click context object: {type(ctx.obj)}")

    dbt_operations = ctx.obj(vars=vars, threads=threads)

    if not isinstance(dbt_operations, DbtOperations):
        raise ValueError(f"Unexpected dbt operations type: {type(dbt_operations)}")

    @ctx.call_on_close
    def _cleanup() -> None:
        dbt_operations.close()

    return dbt_operations


vars_option = click.option(
    "--vars",
    type=YamlParamType(),
    help="Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file. This argument should be a YAML string, eg. '{my_variable: my_value}'",
)


select_option = click.option(
    "-s",
    "--select",
    multiple=True,
    help="Specify the nodes to include.",
)
model_option = click.option(
    "-m",
    "--models",
    "--model",
    multiple=True,
    help="Specify the model nodes to include; other nodes are excluded.",
)
exclude_option = click.option("--exclude", multiple=True, help="Specify the nodes to exclude.")

# TODO: expand this out into --resource-type/--resource-types and --exclude-resource-type/--exclude-resource-types
resource_types = [
    "metric",
    "semantic_model",
    "saved_query",
    "source",
    "analysis",
    "model",
    "test",
    "unit_test",
    "exposure",
    "snapshot",
    "seed",
    "default",
    "all",
]
resource_type_option = click.option(
    "--resource-type", type=click.Choice(resource_types, case_sensitive=False)
)


@click.group(cls=ErrorHandlingGroup, invoke_without_command=True)
@click.option("--profile", help="Which existing profile to load. Overrides output.profile")
@click.option("-t", "--target", help="Which target to load for the given profile")
@click.option(
    "-d",
    "--debug/--no-debug",
    default=False,
    help="Display debug logging during dbt execution. Useful for debugging and making bug reports events to help when debugging.",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warn", "error", "none"]),
    help="Specify the minimum severity of events that are logged to the console and the log file.",
)
@click.option(
    "--profiles-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Which directory to look in for the profiles.yml file. If not set, dbt will look in the current working directory first, then HOME/.dbt/",
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
)
@click.pass_context
@cli_global_error_handler
def dbt(
    ctx: click.Context,
    profile: t.Optional[str] = None,
    target: t.Optional[str] = None,
    debug: bool = False,
    log_level: t.Optional[str] = None,
    profiles_dir: t.Optional[Path] = None,
    project_dir: t.Optional[Path] = None,
) -> None:
    """
    An ELT tool for managing your SQL transformations and data models, powered by the SQLMesh engine.
    """

    if "--help" in sys.argv:
        # we dont need to import sqlmesh/load the project for CLI help
        return

    # we have a partially applied function here because subcommands might set extra options like --vars
    # that need to be known before we attempt to load the project
    ctx.obj = functools.partial(
        create,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        debug=debug,
        log_level=log_level,
    )

    if not ctx.invoked_subcommand:
        if profile or target:
            # trigger a project load to validate the specified profile / target
            ctx.obj()

        click.echo(
            f"No command specified. Run `{ctx.info_name} --help` to see the available commands."
        )


@dbt.command()
@select_option
@model_option
@exclude_option
@resource_type_option
@click.option(
    "-f",
    "--full-refresh",
    is_flag=True,
    default=False,
    help="If specified, sqlmesh will drop incremental models and fully-recalculate the incremental table from the model definition.",
)
@click.option(
    "--env",
    "--environment",
    help="Run against a specific Virtual Data Environment (VDE) instead of the main environment",
)
@click.option(
    "--empty/--no-empty", default=False, help="If specified, limit input refs and sources"
)
@click.option(
    "--threads",
    type=int,
    help="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
)
@vars_option
@click.pass_context
def run(
    ctx: click.Context,
    vars: t.Optional[t.Dict[str, t.Any]],
    threads: t.Optional[int],
    env: t.Optional[str] = None,
    **kwargs: t.Any,
) -> None:
    """Compile SQL and execute against the current target database."""
    _get_dbt_operations(ctx, vars, threads).run(environment=env, **kwargs)


@dbt.command(name="list")
@select_option
@model_option
@exclude_option
@resource_type_option
@vars_option
@click.pass_context
def list_(ctx: click.Context, vars: t.Optional[t.Dict[str, t.Any]], **kwargs: t.Any) -> None:
    """List the resources in your project"""
    _get_dbt_operations(ctx, vars).list_(**kwargs)


@dbt.command(name="ls", hidden=True)  # hidden alias for list
@click.pass_context
def ls(ctx: click.Context) -> None:
    """List the resources in your project"""
    ctx.forward(list_)


def _not_implemented(name: str) -> None:
    @dbt.command(name=name)
    def _not_implemented() -> None:
        """Not implemented"""
        click.echo(f"dbt {name} not implemented")


for subcommand in (
    "build",
    "clean",
    "clone",
    "compile",
    "debug",
    "deps",
    "docs",
    "init",
    "parse",
    "retry",
    "run-operation",
    "seed",
    "show",
    "snapshot",
    "source",
    "test",
):
    _not_implemented(subcommand)
