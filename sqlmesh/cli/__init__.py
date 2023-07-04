import typing as t
from functools import wraps

import click
from sqlglot.errors import SqlglotError

from sqlmesh.utils import debug_mode_enabled
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.errors import SQLMeshError

DECORATOR_RETURN_TYPE = t.TypeVar("DECORATOR_RETURN_TYPE")


def error_handler(
    func: t.Callable[..., DECORATOR_RETURN_TYPE]
) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
    @wraps(func)
    def wrapper(*args: t.Any, **kwargs: t.Any) -> DECORATOR_RETURN_TYPE:
        try:
            return func(*args, **kwargs)
        except NodeExecutionFailedError as ex:
            cause = ex.__cause__
            raise click.ClickException(f"Failed processing {ex.node}. {cause}")
        except (SQLMeshError, SqlglotError, ValueError) as ex:
            raise click.ClickException(str(ex))

    return wrapper if not debug_mode_enabled() else func
