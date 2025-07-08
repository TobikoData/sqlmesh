from lsprotocol.types import Diagnostic
from sqlmesh.utils.errors import ConfigError, ModeBlockExtraFields, ModelBlockFieldValidationMissingFieldsError
import typing as t

type ContextFailedError = str | ConfigError | ModeBlockExtraFields | ModelBlockFieldValidationMissingFieldsError

def contextErrorToDiagnostic(error: Exception,) -> t.Tuple[t.Optional[t.Tuple[str, Diagnostic]], ContextFailedError]:
    if isinstance(error, ModelBlockFieldValidationMissingFieldsError):
        return missingErrorToDiagnostic(error), error
    if isinstance(error, ModeBlockExtraFields)
        return extraFieldsErrorToDiagnostic(error), error
    if isinstance(error, ConfigError):
        return configErrorToDiagnostic(error), error
    return None, str(error)


def missingErrorToDiagnostic(
        error: ModelBlockFieldValidationMissingFieldsError,
) ->t.Optional[t.Tuple[str, Diagnostic]]:
    raise NotImplementedError()

def extraFieldsErrorToDiagnostic(
        error: ModeBlockExtraFields,
) -> t.Optional[t.Tuple[str, Diagnostic]]:
    raise NotImplementedError()

def configErrorToDiagnostic(
        error: ConfigError,
) ->t.Optional[t.Tuple[str, Diagnostic]]:
    return Diagnostic(

    )
    raise NotImplementedError()

