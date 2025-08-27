import typing as t
import sys
import click
from sqlmesh_dbt.operations import DbtOperations, create
from sqlmesh_dbt.error import cli_global_error_handler, ErrorHandlingGroup
from pathlib import Path
from sqlmesh_dbt.options import YamlParamType
import functools


def _get_dbt_operations(ctx: click.Context, vars: t.Optional[t.Dict[str, t.Any]]) -> DbtOperations:
    if not isinstance(ctx.obj, functools.partial):
        raise ValueError(f"Unexpected click context object: {type(ctx.obj)}")

    dbt_operations = ctx.obj(vars=vars)

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
    "-m",
    "--select",
    "--models",
    "--model",
    multiple=True,
    help="Specify the nodes to include.",
)
exclude_option = click.option("--exclude", multiple=True, help="Specify the nodes to exclude.")


@click.group(cls=ErrorHandlingGroup, invoke_without_command=True)
@click.option("--profile", help="Which existing profile to load. Overrides output.profile")
@click.option("-t", "--target", help="Which target to load for the given profile")
@click.option(
    "-d",
    "--debug/--no-debug",
    default=False,
    help="Display debug logging during dbt execution. Useful for debugging and making bug reports events to help when debugging.",
)
@click.pass_context
@cli_global_error_handler
def dbt(
    ctx: click.Context,
    profile: t.Optional[str] = None,
    target: t.Optional[str] = None,
    debug: bool = False,
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
        create, project_dir=Path.cwd(), profile=profile, target=target, debug=debug
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
@exclude_option
@click.option(
    "-f",
    "--full-refresh",
    help="If specified, dbt will drop incremental models and fully-recalculate the incremental table from the model definition.",
)
@vars_option
@click.pass_context
def run(ctx: click.Context, vars: t.Optional[t.Dict[str, t.Any]], **kwargs: t.Any) -> None:
    """Compile SQL and execute against the current target database."""
    _get_dbt_operations(ctx, vars).run(**kwargs)


@dbt.command(name="list")
@select_option
@exclude_option
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
