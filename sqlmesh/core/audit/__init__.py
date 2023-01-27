import inspect
import typing as t
from types import ModuleType

from sqlmesh.core.audit import column, table
from sqlmesh.core.audit.definition import Audit, AuditResult


def _discover_audits(modules: t.Iterable[ModuleType]) -> t.Dict[str, Audit]:
    return {
        audit.name: audit
        for module in modules
        for _, audit in inspect.getmembers(module, lambda v: isinstance(v, Audit))
    }


BUILT_IN_AUDITS = _discover_audits([column, table])
