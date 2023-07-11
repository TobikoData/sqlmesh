from __future__ import annotations

import typing as t
from enum import auto
from pathlib import Path

from sqlglot.helper import AutoName


class ErrorLevel(AutoName):
    IGNORE = auto()
    WARN = auto()
    RAISE = auto()


class SQLMeshError(Exception):
    pass


class ConfigError(SQLMeshError):
    pass


class MissingDependencyError(SQLMeshError):
    """Local environment is missing a required dependency for the given operation"""


class MacroEvalError(SQLMeshError):
    pass


class PlanError(SQLMeshError):
    pass


class NoChangesPlanError(PlanError):
    pass


class MissingContextException(Exception):
    pass


class SnapshotVersionError(SQLMeshError):
    pass


class MagicError(SQLMeshError):
    pass


class AuditConfigError(ConfigError):
    pass


class AuditError(SQLMeshError):
    pass


class NotificationTargetError(SQLMeshError):
    pass


class ApiError(SQLMeshError):
    pass


class ApiClientError(ApiError):
    pass


class ApiServerError(ApiError):
    pass


class NotFoundError(ApiClientError):
    pass


class CICDBotError(SQLMeshError):
    pass


class ParsetimeAdapterCallError(SQLMeshError):
    pass


def raise_config_error(
    msg: str,
    location: t.Optional[str | Path] = None,
    error_type: t.Type[ConfigError] = ConfigError,
) -> None:
    if location:
        raise error_type(f"{msg} at '{location}'")
    raise error_type(msg)
