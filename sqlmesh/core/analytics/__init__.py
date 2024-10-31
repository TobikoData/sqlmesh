from __future__ import annotations

import atexit
import os
import typing as t
from functools import wraps

from sqlmesh.core.analytics.collector import AnalyticsCollector
from sqlmesh.core.analytics.dispatcher import AsyncEventDispatcher, NoopEventDispatcher
from sqlmesh.utils import str_to_bool

if t.TYPE_CHECKING:
    import sys

    if sys.version_info >= (3, 10):
        from typing import ParamSpec
    else:
        from typing_extensions import ParamSpec

    _P = ParamSpec("_P")
    _T = t.TypeVar("_T")


def init_collector() -> AnalyticsCollector:
    dispatcher = (
        NoopEventDispatcher()
        if str_to_bool(os.getenv("SQLMESH__DISABLE_ANONYMIZED_ANALYTICS", "false"))
        else AsyncEventDispatcher()
    )
    return AnalyticsCollector(dispatcher=dispatcher)


collector = init_collector()


atexit.register(collector.shutdown, flush=True)


def disable_analytics() -> None:
    global collector
    collector.shutdown(flush=False)
    collector = AnalyticsCollector(dispatcher=NoopEventDispatcher())


def cli_analytics(func: t.Callable[_P, _T]) -> t.Callable[_P, _T]:
    @wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        import click
        from click.core import ParameterSource

        arg_keys = set()
        command_chain = []

        cli_context: click.Context = click.get_current_context()
        cli_context_cursor: t.Optional[click.Context] = cli_context
        while cli_context_cursor:
            arg_keys |= {
                k
                for k, source in cli_context_cursor._parameter_source.items()
                if source == ParameterSource.COMMANDLINE
            }
            command_chain.append(cli_context_cursor.info_name)
            cli_context_cursor = cli_context_cursor.parent

        command_name, *parent_command_names = command_chain

        common_context: t.Dict[str, t.Any] = {
            "command_name": command_name,
            "command_args": arg_keys,
            "parent_command_names": parent_command_names,
        }

        if "github" in parent_command_names:
            cicd_bot_config = None
            github_controller = cli_context.obj.get("github")
            if github_controller:
                cicd_bot_config = github_controller._context.config.cicd_bot
            collector.on_cicd_command(**common_context, cicd_bot_config=cicd_bot_config)
        else:
            collector.on_cli_command(**common_context)

        return func(*args, **kwargs)

    return wrapper


def python_api_analytics(func: t.Callable[_P, _T]) -> t.Callable[_P, _T]:
    @wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        import inspect

        from sqlmesh import magics

        should_log = True

        try:
            stack = inspect.stack()
        except Exception:
            stack = []

        for frame in stack:
            if "click/" in frame.filename or frame.filename == magics.__file__:
                # Magics and CLI are reported separately.
                should_log = False
                break

        if should_log:
            collector.on_python_api_command(command_name=func.__name__, command_args=kwargs)

        return func(*args, **kwargs)

    return wrapper
