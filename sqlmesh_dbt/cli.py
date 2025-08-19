import typing as t
import sys
import click
from sqlmesh_dbt.operations import DbtOperations, create
from sqlmesh_dbt.error import cli_global_error_handler
from pathlib import Path


def _get_dbt_operations(ctx: click.Context) -> DbtOperations:
    if not isinstance(ctx.obj, DbtOperations):
        raise ValueError(f"Unexpected click context object: {type(ctx.obj)}")
    return ctx.obj


@click.group(invoke_without_command=True)
@click.option("--profile", help="Which existing profile to load. Overrides output.profile")
@click.option("-t", "--target", help="Which target to load for the given profile")
@click.pass_context
@cli_global_error_handler
def dbt(
    ctx: click.Context, profile: t.Optional[str] = None, target: t.Optional[str] = None
) -> None:
    """
    An ELT tool for managing your SQL transformations and data models, powered by the SQLMesh engine.
    """

    if "--help" in sys.argv:
        # we dont need to import sqlmesh/load the project for CLI help
        return

    # TODO: conditionally call create() if there are times we dont want/need to import sqlmesh and load a project
    ctx.obj = create(project_dir=Path.cwd(), profile=profile, target=target)

    if not ctx.invoked_subcommand:
        click.echo(
            f"No command specified. Run `{ctx.info_name} --help` to see the available commands."
        )


@dbt.command()
@click.option("-s", "-m", "--select", "--models", "--model", help="Specify the nodes to include.")
@click.option(
    "-f",
    "--full-refresh",
    help="If specified, dbt will drop incremental models and fully-recalculate the incremental table from the model definition.",
)
@click.pass_context
def run(ctx: click.Context, select: t.Optional[str], full_refresh: bool) -> None:
    """Compile SQL and execute against the current target database."""
    _get_dbt_operations(ctx).run(select=select, full_refresh=full_refresh)


@dbt.command(name="list")
@click.pass_context
def list_(ctx: click.Context) -> None:
    """List the resources in your project"""
    _get_dbt_operations(ctx).list_()


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
