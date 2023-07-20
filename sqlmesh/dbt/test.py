from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, validator

import sqlmesh.core.dialect as d
from sqlmesh.core.audit import Audit
from sqlmesh.dbt.common import Dependencies, GeneralConfig, SqlStr, extract_jinja_config

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


class Severity(str, Enum):
    """DBT test severity"""

    ERROR = "error"
    WARN = "warn"


class TestConfig(GeneralConfig):
    """
    TestConfig contains all the config paramters for a dbt test.

    Args:
        path: The file path to the test.
        name: The name of the test.
        sql: The test sql.
        test_kwargs: The kwargs passed into the test.
        owner: The name of the model under test.
        column_name: The name of the column under test.
        dependencies: The macros, refs, and sources the test depends upon.
        package_name: Name of the package that defines the test.
        alias: The alias for the materialized table where failures are stored (Not supported).
        schema: The schema for the materialized table where the failures are stored (Not supported).
        database: The database for the materilized table where the failures are stored (Not supported).
        severity: The severity of a failure: ERROR blocks execution and WARN continues execution.
        store_failures: Failures are stored in a materialized table when True (Not supported).
        where: Additional where clause to add to the test.
        limit: Additional limit clause to add to the test (Not supported).
        fail_calc: Custom calculation to use (default "count(*)") for displaying test failure (Not supported).
        warn_if: Conditional expression (default "!=0") to detect if warn condition met (Not supported).
        error_if: Conditional expression (default "!=0") to detect if error condition met (Not supported).
    """

    # SQLMesh fields
    path: Path = Path()
    name: str
    sql: SqlStr
    test_kwargs: t.Dict[str, t.Any] = {}
    owner: str
    column_name: t.Optional[str] = None
    dependencies: Dependencies = Dependencies()

    # dbt fields
    package_name: str = ""
    alias: t.Optional[str] = None
    schema_: t.Optional[str] = Field("", alias="schema")
    database: t.Optional[str] = None
    severity: Severity = Severity.ERROR
    store_failures: t.Optional[bool] = None
    where: t.Optional[str] = None
    limit: t.Optional[str] = None
    fail_calc: str = "count(*)"
    warn_if: str = "!=0"
    error_if: str = "!=0"

    @validator("severity", pre=True)
    def _validate_severity(cls, v: t.Union[Severity, str]) -> Severity:
        if isinstance(v, Severity):
            return v
        return Severity(v.lower())

    @validator("name", pre=True)
    def _lowercase_name(cls, v: str) -> str:
        return v.lower()

    def to_sqlmesh(self, context: DbtContext) -> Audit:
        """Convert dbt Test to SQLMesh Audit

        Args:
            context: Context for the dbt project
        Returns:
            SQLMesh Audit for this test
        """
        test_context = context.context_for_dependencies(self.dependencies)

        jinja_macros = test_context.jinja_macros.trim(
            self.dependencies.macros, package=self.package_name
        )
        jinja_macros.global_objs.update(
            {
                "config": self.attribute_dict,
                **test_context.jinja_globals,  # type: ignore
            }
        )

        sql_no_config, _sql_config_only = extract_jinja_config(self.sql)
        sql_no_config = sql_no_config.replace("**_dbt_generic_test_kwargs", self._kwargs())
        query = d.jinja_query(sql_no_config)

        skip = not self.enabled
        blocking = self.severity == Severity.ERROR

        audit = Audit(
            name=self.name,
            dialect=context.dialect,
            skip=skip,
            blocking=blocking,
            query=query,
            jinja_macros=jinja_macros,
        )
        audit._path = self.path
        return audit

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
                    kwargs[key] = f'"{escape_quotes(value)}"'
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


def escape_quotes(v: str) -> str:
    return v.replace('"', '\\"')
