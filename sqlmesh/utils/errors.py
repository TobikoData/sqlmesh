from __future__ import annotations

import typing as t
from enum import auto
from pathlib import Path

from sqlglot import exp
from sqlglot.helper import AutoName

if t.TYPE_CHECKING:
    from requests.models import Response

    from sqlmesh.core.model import Model


class ErrorLevel(AutoName):
    IGNORE = auto()
    WARN = auto()
    RAISE = auto()


class SQLMeshError(Exception):
    pass


class ConfigError(SQLMeshError):
    location: t.Optional[Path] = None

    def __init__(self, message: str | Exception, location: t.Optional[Path] = None) -> None:
        super().__init__(message)
        if location:
            self.location = Path(location) if isinstance(location, str) else location


class ModelBlockFieldValidationMissingFieldsError(ConfigError):
    """Raised when required fields are missing from a model block."""

    missing_fields: t.Set[str]

    def __init__(self, path: Path, missing_fields: t.Set[str]) -> None:
        super().__init__(
            self.message(missing_fields),
            path,
        )
        self.missing_fields = missing_fields

    @staticmethod
    def message(missing_fields: t.Set[str]) -> str:
        field_names = "'" + "', '".join(missing_fields) + "'"
        return f"Please add required field{'s' if len(missing_fields) > 1 else ''} {field_names} to the model block."


class ModeBlockExtraFields(ConfigError):
    """Raised when there are extra fields in a model block that are not defined in the model schema. If there are close
    matches, this tries to recommend them"""

    def __init__(self, path: t.Optional[Path], extra_fields: t.Dict[str, t.Optional[str]]) -> None:
        super().__init__(
            self.message(extra_fields),
            path,
        )
        self.extra_fields = extra_fields

    @staticmethod
    def message(extra_fields: t.Dict[str, t.Optional[str]]) -> str:
        extra_field_names = "'" + "', '".join(extra_fields.keys()) + "'"

        close_matches = {field: match for field, match in extra_fields.items() if match is not None}

        if len(close_matches) == 1:
            similar_msg = ". Did you mean '" + list(close_matches.values())[0] + "'?"
        elif close_matches:
            similar = [
                f"- {field}: Did you mean '{match}'?" for field, match in close_matches.items()
            ]
            similar_msg = "\n\n  " + "\n  ".join(similar)
        else:
            similar_msg = ""

        return f"Invalid field name{'s' if len(extra_fields) > 1 else ''} present in the model block: {extra_field_names}{similar_msg}"


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


class ConflictingPlanError(PlanError):
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
        audit_args: t.Dict[t.Any, t.Any],
        count: int,
        query: exp.Query,
        model: t.Optional[Model] = None,
        # the dialect of the engine adapter that evaluated the audit query
        adapter_dialect: t.Optional[str] = None,
    ) -> None:
        self.audit_name = audit_name
        self.audit_args = audit_args
        self.model = model
        self.count = count
        self.query = query
        self.adapter_dialect = adapter_dialect

        super().__init__(
            f"'{self.audit_name}' audit error: {self.count} {'row' if self.count == 1 else 'rows'} failed"
        )

    @property
    def model_name(self) -> t.Optional[str]:
        return self.model.name if self.model else None

    def sql(self, dialect: t.Optional[str] = None, **opts: t.Any) -> str:
        """
        Returns the rendered audit query that failed.

        Args:
            dialect: the dialect of the output SQL string, by default,
                     this will use the dialect of the engine adapter that ran the query.
            opts: other `sqlglot.generator.Generator` options.

        Returns:
            The SQL string.
        """
        return self.query.sql(dialect=dialect or self.adapter_dialect, **opts)


class NodeAuditsErrors(SQLMeshError):
    def __init__(self, errors: t.List[AuditError]) -> None:
        self.errors = errors

        super().__init__(f"Audits failed: {', '.join([e.audit_name for e in errors])}")


class TestError(SQLMeshError):
    __test__ = False  # prevent pytest trying to collect this as a test class
    pass


class DestructiveChangeError(SQLMeshError):
    pass


class NotificationTargetError(SQLMeshError):
    pass


class ApiError(SQLMeshError):
    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.code = code


class ApiClientError(ApiError):
    pass


class ApiServerError(ApiError):
    pass


class NotFoundError(ApiClientError):
    def __init__(self, message: str) -> None:
        super().__init__(message, 404)


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
        super().__init__("Circuit breaker triggered.")


class PythonModelEvalError(SQLMeshError):
    pass


class MissingDefaultCatalogError(SQLMeshError):
    pass


class LinterError(SQLMeshError):
    pass


class SignalEvalError(SQLMeshError):
    """Errors when evaluating a signal that is because of a user mistake and not a SQLMesh bug."""

    pass


def raise_config_error(
    msg: str,
    location: t.Optional[Path] = None,
    error_type: t.Type[ConfigError] = ConfigError,
) -> None:
    if location:
        raise error_type(f"{msg} at '{location}'")
    raise error_type(msg, location=location)


def raise_for_status(response: Response) -> None:
    if response.status_code == 404:
        raise NotFoundError(response.text)
    if 400 <= response.status_code < 500:
        raise ApiClientError(response.text, response.status_code)
    if 500 <= response.status_code < 600:
        raise ApiServerError(response.text, response.status_code)


def format_destructive_change_msg(
    snapshot_name: str,
    dropped_column_names: t.List[str],
    alter_expressions: t.List[exp.Alter],
    dialect: str,
    error: bool = True,
) -> str:
    dropped_column_str = "', '".join(dropped_column_names)
    dropped_column_msg = (
        f" that drops column{'s' if dropped_column_names and len(dropped_column_names) > 1 else ''} '{dropped_column_str}'"
        if dropped_column_str
        else ""
    )

    alter_expr_msg = "\n\nSchema changes:\n  " + "\n  ".join(
        [alter.sql(dialect) for alter in alter_expressions]
    )

    warning_msg = (
        f"Plan requires a destructive change to forward-only model '{snapshot_name}'s schema"
    )
    err_msg = "\n\nTo allow the destructive change, set the model's `on_destructive_change` setting to `warn` or `allow` or include the model in the plan's `--allow-destructive-model` option.\n"

    return f"\n{warning_msg}{dropped_column_msg}.{alter_expr_msg}{err_msg if error else ''}"
