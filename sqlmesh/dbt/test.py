from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, validator

import sqlmesh.core.dialect as d
from sqlmesh.core.audit import Audit
from sqlmesh.dbt.common import (
    Dependencies,
    GeneralConfig,
    SqlStr,
    context_for_dependencies,
    extract_jinja_config,
)

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
    dependencies: Dependencies = Dependencies()
    dialect: str = ""

    alias: t.Optional[str] = None
    schema_: t.Optional[str] = Field("", alias="schema")
    database: t.Optional[str] = None
    severity: Severity = Severity.ERROR
    store_failures: t.Optional[bool] = None
    where: t.Optional[str] = None
    limit: t.Optional[str] = None
    fail_calc: str
    warn_if: str
    error_if: str

    @validator("severity", pre=True)
    def _validate_severity(cls, v: t.Union[Severity, str]) -> Severity:
        if isinstance(v, Severity):
            return v
        return Severity(v.lower())

    def to_sqlmesh(self, context: DbtContext) -> Audit:
        test_context = context_for_dependencies(context, self.dependencies)

        jinja_macros = test_context.jinja_macros.trim(self.dependencies.macros)
        jinja_macros.global_objs.update(
            {
                "config": self.attribute_dict(),
                **test_context.jinja_globals,  # type: ignore
            }
        )

        sql_no_config, sql_config_only = extract_jinja_config(self.sql)
        sql_no_config = sql_no_config.replace("**_dbt_generic_test_kwargs", self._kwargs())
        expressions = d.parse(sql_no_config, default_dialect=self.dialect or context.dialect)

        skip = not self.enabled
        blocking = self.severity == Severity.ERROR

        return Audit(
            name=self.name,
            dialect=self.dialect,
            skip=skip,
            blocking=blocking,
            query=expressions[-1],
            expressions=expressions[0:-1],
            jinja_macros=jinja_macros,
            depends_on={test_context.refs[ref] for ref in self.dependencies.refs}.union(
                {test_context.sources[source].source_name for source in self.dependencies.sources}
            ),
        )

    def _kwargs(self) -> str:
        kwargs = {}
        for key, value in self.test_kwargs.items():
            if isinstance(value, str):
                # Mimic dbt kwargs logic
                no_braces = _remove_jinja_braces(value)
                jinja_function_regex = r"^\s*(env_var|ref|var|source|doc)\s*\(.+\)\s*$"
                if key != "column_name" and (
                    value != no_braces or re.match(jinja_function_regex, value)
                ):
                    kwargs[key] = no_braces
                else:
                    kwargs[key] = f"'{value}'"
            else:
                kwargs[key] = value

        return ", ".join(f"{key}={value}" for key, value in kwargs.items())


def _remove_jinja_braces(jinja_str: str) -> str:
    no_braces = jinja_str

    cursor = 0
    quotes: t.List[str] = []
    while cursor < len(no_braces):
        val = no_braces[cursor]
        if val in ('"', "'"):
            if quotes and quotes[-1] == val:
                quotes.pop()
            else:
                quotes.append(no_braces[cursor])
        if (
            cursor + 1 < len(no_braces)
            and no_braces[cursor : cursor + 2] in ("{{", "}}")
            and not quotes
        ):
            no_braces = no_braces[:cursor] + no_braces[cursor + 2 :]
        else:
            cursor += 1

    return no_braces.strip()
