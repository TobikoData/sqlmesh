from __future__ import annotations

import typing as t
from enum import auto
from pathlib import Path

from sqlglot import exp
from sqlglot.helper import AutoName

if t.TYPE_CHECKING:
    from sqlmesh.core.model import Model


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


class UncategorizedPlanError(PlanError):
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
    def __init__(
        self,
        audit_name: str,
        count: int,
        query: exp.Subqueryable,
        model: t.Optional[Model] = None,
    ) -> None:
        self.audit_name = audit_name
        self.model = model
        self.count = count
        self.query = query

    def __str__(self) -> str:
        model_str = f" for model '{self.model_name}'" if self.model_name else ""
        return f"Audit '{self.audit_name}'{model_str} failed.\nGot {self.count} results, expected 0.\n{self.query}"

    @property
    def model_name(self) -> t.Optional[str]:
        return self.model.name if self.model else None


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


class EngineAdapterError(SQLMeshError):
    pass


class UnsupportedCatalogOperationError(EngineAdapterError):
    pass


class CircuitBreakerError(SQLMeshError):
    def __init__(self) -> None:
        super().__init__("Circuit breaker has been triggered.")


def raise_config_error(
    msg: str,
    location: t.Optional[str | Path] = None,
    error_type: t.Type[ConfigError] = ConfigError,
) -> None:
    if location:
        raise error_type(f"{msg} at '{location}'")
    raise error_type(msg)
