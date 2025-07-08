from lsprotocol.types import Diagnostic, DiagnosticSeverity, Range, Position

from sqlmesh.core.linter.helpers import get_range_of_model_block
from sqlmesh.lsp.helpers import to_lsp_range
from sqlmesh.lsp.uri import URI
from sqlmesh.utils.errors import (
    ConfigError,
    ModeBlockExtraFields,
    ModelBlockFieldValidationMissingFieldsError,
)
import typing as t

type ContextFailedError = (
    str | ConfigError | ModeBlockExtraFields | ModelBlockFieldValidationMissingFieldsError
)


def contextErrorToDiagnostic(
    error: Exception,
) -> t.Tuple[t.Optional[t.Tuple[str, Diagnostic]], ContextFailedError]:
    if isinstance(error, ModelBlockFieldValidationMissingFieldsError):
        return missingErrorToDiagnostic(error), error
    if isinstance(error, ModeBlockExtraFields):
        return extraFieldsErrorToDiagnostic(error), error
    if isinstance(error, ConfigError):
        return configErrorToDiagnostic(error), error
    return None, str(error)


def missingErrorToDiagnostic(
    error: ModelBlockFieldValidationMissingFieldsError,
) -> t.Optional[t.Tuple[str, Diagnostic]]:
    if error.location is None:
        return None
    uri = URI.from_path(error.location).value
    with open(error.location, "r") as file:
        content = file.read()
    model_block = get_range_of_model_block(content)
    if model_block is None:
        return None
    return uri, Diagnostic(
        message=str(error),
        range=to_lsp_range(model_block),
        severity=DiagnosticSeverity.Error,
        source="SQLMesh",
    )


def extraFieldsErrorToDiagnostic(
    error: ModeBlockExtraFields,
) -> t.Optional[t.Tuple[str, Diagnostic]]:
    if error.location is None:
        return None
    uri = URI.from_path(error.location).value
    with open(error.location, "r") as file:
        content = file.read()
    model_block = get_range_of_model_block(content)
    if model_block is None:
        return None
    return uri, Diagnostic(
        message=str(error),
        range=to_lsp_range(model_block),
        severity=DiagnosticSeverity.Error,
        source="SQLMesh",
    )


def configErrorToDiagnostic(
    error: ConfigError,
) -> t.Optional[t.Tuple[str, Diagnostic]]:
    if error.location is None:
        return None
    uri = URI.from_path(error.location).value
    return uri, Diagnostic(
        range=Range(
            start=Position(
                line=0,
                character=0,
            ),
            end=Position(
                line=0,
                character=0,
            ),
        ),
        message=str(error),
        severity=DiagnosticSeverity.Error,
        source="SQLMesh",
    )
