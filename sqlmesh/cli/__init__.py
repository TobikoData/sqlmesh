import logging
import typing as t
from functools import wraps

import click
from sqlglot.errors import SqlglotError

from sqlmesh.core.context import Context
from sqlmesh.utils import debug_mode_enabled
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.errors import SQLMeshError

DECORATOR_RETURN_TYPE = t.TypeVar("DECORATOR_RETURN_TYPE")


logger = logging.getLogger(__name__)


def error_handler(
    func: t.Callable[..., DECORATOR_RETURN_TYPE]
) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
    @wraps(func)
    def wrapper(*args: t.List[t.Any], **kwargs: t.Any) -> DECORATOR_RETURN_TYPE:
        context_or_obj = args[0]
        sqlmesh_context = (
            context_or_obj.obj if isinstance(context_or_obj, click.Context) else context_or_obj
        )
        if not isinstance(sqlmesh_context, Context):
            sqlmesh_context = None
        handler = _debug_exception_handler if debug_mode_enabled() else _default_exception_handler
        return handler(sqlmesh_context, lambda: func(*args, **kwargs))

    return wrapper


def _default_exception_handler(
    context: t.Optional[Context], func: t.Callable[[], DECORATOR_RETURN_TYPE]
) -> DECORATOR_RETURN_TYPE:
    try:
        return func()
    except NodeExecutionFailedError as ex:
        cause = ex.__cause__
        raise click.ClickException(f"Failed processing {ex.node}. {cause}")
    except (SQLMeshError, SqlglotError, ValueError) as ex:
        raise click.ClickException(str(ex))
    finally:
        if context:
            context.close()


def _debug_exception_handler(
    context: t.Optional[Context], func: t.Callable[[], DECORATOR_RETURN_TYPE]
) -> DECORATOR_RETURN_TYPE:
    try:
        return func()
    except Exception:
        logger.exception("Unhandled exception")
        raise
    finally:
        if context:
            context.close()
