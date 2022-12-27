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
            cause = ex.__cause__
            raise click.ClickException(f"Failed processing {ex.node}. {cause}")
        except (SQLMeshError, SqlglotError, ValueError) as ex:
            raise click.ClickException(str(ex))

    return wrapper
