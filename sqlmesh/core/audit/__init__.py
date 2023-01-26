import inspect
import typing as t
from types import ModuleType

from sqlmesh.core.audit import column
from sqlmesh.core.audit.definition import Audit, AuditResult


def _discover_audits(modules: t.Iterable[ModuleType]) -> t.Dict[str, Audit]:
    result = {}
    for module in modules:
        for _, value in inspect.getmembers(module, lambda v: isinstance(v, Audit)):
            result[value.name] = value
    return result


BUILT_IN_AUDITS = _discover_audits([column])
