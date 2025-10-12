from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field
import sqlmesh.core.dialect as d
from sqlmesh.core.audit import Audit, ModelAudit, StandaloneAudit
from sqlmesh.core.node import DbtNodeInfo
from sqlmesh.dbt.common import (
    Dependencies,
    GeneralConfig,
    SqlStr,
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
        dialect: SQL dialect of the test query.
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

    __test__ = (
        False  # prevent pytest trying to collect this as a test class when it's imported in a test
    )

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
    dialect_: t.Optional[str] = Field(None, alias="dialect")

    # dbt fields
    unique_id: str = ""
    package_name: str = ""
    alias: t.Optional[str] = None
    fqn: t.List[str] = []
    schema_: t.Optional[str] = Field("", alias="schema")
    database: t.Optional[str] = None
    severity: Severity = Severity.ERROR
    store_failures: t.Optional[bool] = None
    where: t.Optional[str] = None
    limit: t.Optional[int] = None
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
    def canonical_name(self) -> str:
        return f"{self.package_name}.{self.name}".lower() if self.package_name else self.name

    @property
    def is_standalone(self) -> bool:
        # A test is standalone if:
        # 1. It has no model_name (already standalone), OR
        # 2. It references other models besides its own model
        if not self.model_name:
            return True

        # Check if test has references to other models
        # For versioned models, refs include version (e.g., "model_name_v1") but model_name may not
        self_refs = {self.model_name}
        for ref in self.dependencies.refs:
            # versioned models end in _vX
            if ref.startswith(f"{self.model_name}_v"):
                self_refs.add(ref)

        other_refs = {ref for ref in self.dependencies.refs if ref not in self_refs}
        return bool(other_refs)

    @property
    def sqlmesh_config_fields(self) -> t.Set[str]:
        return {"description", "owner", "stamp", "cron", "interval_unit"}

    def dialect(self, context: DbtContext) -> str:
        return self.dialect_ or context.default_dialect

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

        query = d.jinja_query(self.sql.replace("**_dbt_generic_test_kwargs", self._kwargs()))

        skip = not self.enabled
        blocking = self.severity == Severity.ERROR

        audit: Audit
        if self.is_standalone:
            jinja_macros.add_globals({"this": self.relation_info})
            audit = StandaloneAudit(
                name=self.name,
                dbt_node_info=self.node_info,
                dialect=self.dialect(context),
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
                dbt_node_info=self.node_info,
                dialect=self.dialect(context),
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
                # Multiline values will end with a newline. Remove it here.
                value = value.rstrip()
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

    @property
    def node_info(self) -> DbtNodeInfo:
        return DbtNodeInfo(
            unique_id=self.unique_id, name=self.name, fqn=".".join(self.fqn), alias=self.alias
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
