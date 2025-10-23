import typing as t
import sys
import click
from click.core import ParameterSource
from sqlmesh_dbt.operations import DbtOperations, create
from sqlmesh_dbt.error import cli_global_error_handler, ErrorHandlingGroup
from pathlib import Path
from sqlmesh_dbt.options import global_options, run_options, list_options


def _get_dbt_operations(
    ctx: click.Context, **kwargs: t.Dict[str, t.Any]
) -> t.Tuple[DbtOperations, t.Dict[str, t.Any]]:
    if not isinstance(ctx.obj, dict):
        raise ValueError(f"Unexpected click context object: {type(ctx.obj)}")

    all_options = {
        **kwargs,
        # ctx.obj only contains global options that a user specifically provided, so these values take precedence
        # over **kwargs which contains all options (plus Click's defaults) whether or not they were explicitly set by a user
        **ctx.obj,
    }

    T = t.TypeVar("T")

    def _pop_global_option(name: str, expected_type: t.Type[T]) -> t.Optional[T]:
        value = all_options.pop(name, None)
        if value is not None and not isinstance(value, expected_type):
            raise ValueError(
                f"Expecting option '{name}' to be type '{expected_type}', however it was '{type(value)}'"
            )
        return value

    dbt_operations = create(
        project_dir=_pop_global_option("project_dir", Path),
        profiles_dir=_pop_global_option("profiles_dir", Path),
        profile=_pop_global_option("profile", str),
        target=_pop_global_option("target", str),
        vars=_pop_global_option("vars", dict),
        threads=_pop_global_option("threads", int),
        debug=_pop_global_option("debug", bool) or False,
        log_level=_pop_global_option("log_level", str),
    )

    if not isinstance(dbt_operations, DbtOperations):
        raise ValueError(f"Unexpected dbt operations type: {type(dbt_operations)}")

    @ctx.call_on_close
    def _cleanup() -> None:
        dbt_operations.close()

    # at this point, :all_options just contains what's left because we popped the global options
    return dbt_operations, all_options


@click.group(cls=ErrorHandlingGroup, invoke_without_command=True)
@global_options
@click.pass_context
@cli_global_error_handler
def dbt(ctx: click.Context, **kwargs: t.Any) -> None:
    """
    An ELT tool for managing your SQL transformations and data models, powered by the SQLMesh engine.
    """

    if "--help" in sys.argv:
        # we dont need to import sqlmesh/load the project for CLI help
        return

    # only keep track of the global options that have been explicitly set
    # the subcommand handlers get invoked with the default values of the global options (even if the option is specified top level)
    # so we capture any explicitly set options to use as overrides in the subcommand handlers
    ctx.obj = {
        k: v for k, v in kwargs.items() if ctx.get_parameter_source(k) != ParameterSource.DEFAULT
    }

    if not ctx.invoked_subcommand:
        if "profile" in ctx.obj or "target" in ctx.obj:
            # trigger a project load to validate the specified profile / target
            _get_dbt_operations(ctx)

        click.echo(
            f"No command specified. Run `{ctx.info_name} --help` to see the available commands."
        )


@dbt.command()
@global_options
@run_options
@click.pass_context
def run(
    ctx: click.Context,
    env: t.Optional[str] = None,
    **kwargs: t.Any,
) -> None:
    """Compile SQL and execute against the current target database."""
    ops, remaining_kwargs = _get_dbt_operations(ctx, **kwargs)
    ops.run(environment=env, **remaining_kwargs)


@dbt.command(name="list")
@global_options
@list_options
@click.pass_context
def list_(ctx: click.Context, **kwargs: t.Any) -> None:
    """List the resources in your project"""
    ops, remaining_kwargs = _get_dbt_operations(ctx, **kwargs)
    ops.list_(**remaining_kwargs)


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
