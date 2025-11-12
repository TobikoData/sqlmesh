from lsprotocol.types import Diagnostic, DiagnosticSeverity, Range, Position

from sqlmesh.lsp.uri import URI
from sqlmesh.utils.errors import (
    ConfigError,
)
import typing as t

ContextFailedError = t.Union[str, ConfigError, Exception]


def context_error_to_diagnostic(
    error: t.Union[Exception, ContextFailedError],
    uri_filter: t.Optional[URI] = None,
) -> t.Tuple[t.Optional[t.Tuple[str, Diagnostic]], ContextFailedError]:
    """
    Convert an error to a diagnostic message.
    If the error is a ConfigError, it will be converted to a diagnostic message.

    uri_filter is used to filter diagnostics by URI. If present, only diagnostics
    with a matching URI will be returned.
    """
    if isinstance(error, ConfigError):
        return config_error_to_diagnostic(error), error
    return None, str(error)


def config_error_to_diagnostic(
    error: ConfigError,
    uri_filter: t.Optional[URI] = None,
) -> t.Optional[t.Tuple[str, Diagnostic]]:
    if error.location is None:
        return None
    uri = URI.from_path(error.location).value
    if uri_filter and uri != uri_filter.value:
        return None
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
