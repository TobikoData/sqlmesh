import typing as t
import logging
from functools import wraps
import click
import sys

logger = logging.getLogger(__name__)


def cli_global_error_handler(
    func: t.Callable[..., t.Any],
) -> t.Callable[..., t.Any]:
    @wraps(func)
    def wrapper(*args: t.List[t.Any], **kwargs: t.Any) -> t.Any:
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            # these imports are deliberately deferred to avoid the penalty of importing the `sqlmesh`
            # package up front for every CLI command
            from sqlmesh.utils.errors import SQLMeshError
            from sqlglot.errors import SqlglotError

            if isinstance(ex, (SQLMeshError, SqlglotError, ValueError)):
                click.echo(click.style("Error: " + str(ex), fg="red"))
                sys.exit(1)
            else:
                raise

    return wrapper


class ErrorHandlingGroup(click.Group):
    def add_command(self, cmd: click.Command, name: t.Optional[str] = None) -> None:
        if cmd.callback:
            cmd.callback = cli_global_error_handler(cmd.callback)
        super().add_command(cmd, name=name)
