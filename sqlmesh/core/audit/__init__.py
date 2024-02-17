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


def create_non_blocking_copy(audit: Audit) -> Audit:
    return audit.copy(update={"name": f"{audit.name}_non_blocking", "blocking": False})


def _discover_audits(modules: t.Iterable[ModuleType]) -> t.Dict[str, Audit]:
    return {
        audit.name: audit
        for module in modules
        for _, model_audit in inspect.getmembers(module, lambda v: isinstance(v, ModelAudit))
        for audit in (model_audit, create_non_blocking_copy(model_audit))
    }


BUILT_IN_AUDITS = _discover_audits([builtin])
