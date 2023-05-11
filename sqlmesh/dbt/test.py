from __future__ import annotations

import typing as t
from enum import Enum
from pathlib import Path

from pydantic import validator

from sqlmesh.core.audit import Audit
from sqlmesh.dbt.common import Dependencies, GeneralConfig, SqlStr

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


class Severity(str, Enum):
    """DBT test severity"""

    ERROR = "error"
    WARN = "warn"


class TestConfig(GeneralConfig):
    path: Path = Path()

    name: str
    sql: SqlStr
    test_kwargs: t.Dict[str, t.Any] = {}
    model_name: str
    column_name: t.Optional[str] = None
    severity: Severity = Severity.ERROR
    store_failures: t.Optional[bool] = None
    where: t.Optional[str] = None
    limit: t.Optional[str] = None
    fail_calc: str
    warn_if: str
    error_if: str
    dependencies: Dependencies = Dependencies()

    @validator("severity", pre=True)
    def _validate_severity(cls, v: t.Union[Severity, str]) -> Severity:
        if isinstance(v, Severity):
            return v
        return Severity(v.lower())

    def to_sqlmesh(self, test_context: DbtContext) -> Audit:
        jinja_macros = test_context.jinja_macros.trim(self.dependencies.macros)
        if self.test_kwargs:
            jinja_macros.global_objs.update()
        return Audit(name="", query="")
