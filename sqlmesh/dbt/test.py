from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field

import sqlmesh.core.dialect as d
from sqlmesh.core.audit import Audit, ModelAudit, StandaloneAudit
from sqlmesh.dbt.common import (
    Dependencies,
    GeneralConfig,
    SqlStr,
    extract_jinja_config,
    sql_str_validator,
)
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.pydantic import field_validator

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
        model_name: The name of the model this test is attached to. Do not set for singular tests.
        owner: The name of the model under test.
        stamp: An optional arbitrary string sequence used to create new audit versions without making
            changes to any of the functional components of the definition.
        cron: A cron string specifying how often the audit should be refreshed, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        interval_unit: The duration of an interval for the audit. By default, it is computed from the cron expression.
        column_name: The name of the column under test.
        dependencies: The macros, refs, and sources the test depends upon.
        package_name: Name of the package that defines the test.
        alias: The alias for the materialized table where failures are stored (Not supported).
        schema: The schema for the materialized table where the failures are stored (Not supported).
        database: The database for the materialized table where the failures are stored (Not supported).
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
    model_name: t.Optional[str] = None
    owner: t.Optional[str] = None
    stamp: t.Optional[str] = None
    cron: t.Optional[str] = None
    interval_unit: t.Optional[str] = None
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
    quoting: t.Dict[str, t.Optional[bool]] = {}

    _sql_validator = sql_str_validator

    @field_validator("severity", mode="before")
    @classmethod
    def _validate_severity(cls, v: t.Union[Severity, str]) -> Severity:
        if isinstance(v, Severity):
            return v
        return Severity(v.lower())

    @field_validator("name", mode="before")
    @classmethod
    def _lowercase_name(cls, v: str) -> str:
        return v.lower()

    @property
    def is_standalone(self) -> bool:
        return not self.model_name

    @property
    def sqlmesh_config_fields(self) -> t.Set[str]:
        return {"description", "owner", "stamp", "cron", "interval_unit"}

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
        jinja_macros.add_globals(
            {
                "config": self.config_attribute_dict,
                **test_context.jinja_globals,  # type: ignore
            }
        )

        sql_no_config, _sql_config_only = extract_jinja_config(self.sql)
        sql_no_config = sql_no_config.replace("**_dbt_generic_test_kwargs", self._kwargs())
        query = d.jinja_query(sql_no_config)

        skip = not self.enabled
        blocking = self.severity == Severity.ERROR

        audit: Audit
        if self.is_standalone:
            jinja_macros.add_globals({"this": self.relation_info})
            audit = StandaloneAudit(
                name=self.name,
                dialect=context.dialect,
                skip=skip,
                query=query,
                jinja_macros=jinja_macros,
                depends_on={
                    model.canonical_name(context) for model in test_context.refs.values()
                }.union(
                    {source.canonical_name(context) for source in test_context.sources.values()}
                ),
                tags=self.tags,
                default_catalog=context.target.database,
                **self.sqlmesh_config_kwargs,
            )
        else:
            audit = ModelAudit(
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

    @property
    def relation_info(self) -> AttributeDict:
        return AttributeDict(
            {
                "name": self.name,
                "database": self.database,
                "schema": self.schema_,
                "identifier": self.name,
                "type": None,
                "quote_policy": AttributeDict(),
            }
        )


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
