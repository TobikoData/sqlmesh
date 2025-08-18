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
            # package for every CLI command
            from sqlmesh.utils.errors import SQLMeshError
            from sqlglot.errors import SqlglotError

            if isinstance(ex, (SQLMeshError, SqlglotError, ValueError)):
                click.echo(click.style("Error: " + str(ex), fg="red"))
                sys.exit(1)
            else:
                raise
        finally:
            from sqlmesh import Context

            context_or_obj = args[0]
            sqlmesh_context = (
                context_or_obj.obj if isinstance(context_or_obj, click.Context) else context_or_obj
            )
            if isinstance(sqlmesh_context, Context):
                sqlmesh_context.close()

    return wrapper
