import typing as t
from functools import wraps

import click
from sqlglot.errors import SqlglotError

from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.errors import SQLMeshError


def error_handler(func: t.Callable) -> t.Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NodeExecutionFailedError as ex:
            raise click.ClickException(str(ex.__cause__))
        except (SQLMeshError, SqlglotError) as ex:
            raise click.ClickException(str(ex))

    return wrapper
