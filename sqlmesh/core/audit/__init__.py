import inspect
import typing as t
from types import ModuleType

from sqlmesh.core.audit import builtin
from sqlmesh.core.audit.definition import (
    Audit,
    AuditResult,
    ModelAudit,
    StandaloneAudit,
    load_audit,
    load_multiple_audits,
)


def _discover_audits(modules: t.Iterable[ModuleType]) -> t.Dict[str, Audit]:
    return {
        audit.name: audit
        for module in modules
        for _, audit in inspect.getmembers(module, lambda v: isinstance(v, ModelAudit))
    }


BUILT_IN_AUDITS = _discover_audits([builtin])
