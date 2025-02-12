from __future__ import annotations

import typing as t
from abc import abstractmethod
from enum import Enum
from pathlib import Path

from pydantic import Field
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.model import Model
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
)
from sqlmesh.dbt.common import (
    DbtConfig,
    Dependencies,
    GeneralConfig,
    SqlStr,
    sql_str_validator,
)
from sqlmesh.dbt.relation import Policy, RelationType
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import field_validator

if t.TYPE_CHECKING:
    from sqlmesh.core.audit.definition import ModelAudit
    from sqlmesh.dbt.context import DbtContext


BMC = t.TypeVar("BMC", bound="BaseModelConfig")


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"
    SNAPSHOT = "snapshot"

    # Snowflake, https://docs.getdbt.com/reference/resource-configs/snowflake-configs#dynamic-tables
    DYNAMIC_TABLE = "dynamic_table"


class SnapshotStrategy(str, Enum):
    """DBT snapshot strategies"""

    TIMESTAMP = "timestamp"
    CHECK = "check"

    @property
    def is_timestamp(self) -> bool:
        return self == SnapshotStrategy.TIMESTAMP

    @property
    def is_check(self) -> bool:
        return self == SnapshotStrategy.CHECK


class Hook(DbtConfig):
    """
    Args:
        sql: The sql to execute.
        transaction: bool indicating if the hook is executed in the same transaction as the model query.
    """

    sql: SqlStr
    transaction: bool = True  # TODO not yet supported

    _sql_validator = sql_str_validator


class BaseModelConfig(GeneralConfig):
    """
    Args:
        owner: The owner of the model.
        stamp: An optional arbitrary string sequence used to create new model versions without making
            changes to any of the functional components of the definition.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        path: The file path of the model
        dependencies: The macro, source, var, and ref dependencies used to execute the model and its hooks
        name: Name of the model.
        package_name: Name of the package that defines the model.
        database: Database the model is stored in
        schema: Custom schema name added to the model schema name
        alias: Relation identifier for this model instead of the filename
        pre-hook: List of SQL statements to run before the model is built.
        post-hook: List of SQL statements to run after the model is built.
        full_refresh: Forces the model to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this model
        columns: Column information for the model
        quoting: Define which components of the qualified name (database, schema, identifier) to quote when resolving the ref() method
    """

    # sqlmesh fields
    owner: t.Optional[str] = None
    stamp: t.Optional[str] = None
    table_format: t.Optional[str] = None
    storage_format: t.Optional[str] = None
    path: Path = Path()
    dependencies: Dependencies = Dependencies()
    tests: t.List[TestConfig] = []
    dialect_: t.Optional[str] = Field(None, alias="dialect")
    grain: t.Union[str, t.List[str]] = []

    # DBT configuration fields
    name: str = ""
    package_name: str = ""
    schema_: str = Field("", alias="schema")
    database: t.Optional[str] = None
    alias: t.Optional[str] = None
    pre_hook: t.List[Hook] = Field([], alias="pre-hook")
    post_hook: t.List[Hook] = Field([], alias="post-hook")
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    columns: t.Dict[str, ColumnConfig] = {}
    quoting: t.Dict[str, t.Optional[bool]] = {}

    version: t.Optional[int] = None
    latest_version: t.Optional[int] = None

    _canonical_name: t.Optional[str] = None

    @field_validator("pre_hook", "post_hook", mode="before")
    @classmethod
    def _validate_hooks(cls, v: t.Union[str, t.List[t.Union[SqlStr, str]]]) -> t.List[Hook]:
        hooks = []
        for hook in ensure_list(v):
            if isinstance(hook, Hook):
                hooks.append(hook)
            elif isinstance(hook, str):
                hooks.append(Hook(sql=hook))
            elif isinstance(hook, dict):
                hooks.append(Hook(**hook))
            else:
                raise ConfigError(f"Invalid hook data: {hook}")

        return hooks

    @field_validator("grants", mode="before")
    @classmethod
    def _validate_grants(cls, v: t.Dict[str, str]) -> t.Dict[str, t.List[str]]:
        return {key: ensure_list(value) for key, value in v.items()}

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{
            "grants": UpdateStrategy.KEY_EXTEND,
            "path": UpdateStrategy.IMMUTABLE,
            "pre-hook": UpdateStrategy.EXTEND,
            "post-hook": UpdateStrategy.EXTEND,
            "columns": UpdateStrategy.KEY_EXTEND,
        },
    }

    @property
    def sql_no_config(self) -> SqlStr:
        return SqlStr("")

    @property
    def sql_embedded_config(self) -> SqlStr:
        return SqlStr("")

    @property
    def table_schema(self) -> str:
        """
        Get the full schema name
        """
        return self.schema_

    @property
    def table_name(self) -> str:
        """
        Get the table name
        """
        return self.alias or self.path.stem

    @property
    def config_name(self) -> str:
        """
        Get the model's config name (package_name.name)
        """
        return f"{self.package_name}.{self.name}"

    def dialect(self, context: DbtContext) -> str:
        return self.dialect_ or context.default_dialect

    def canonical_name(self, context: DbtContext) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        if not self._canonical_name:
            relation = context.create_relation(
                self.relation_info,
                quote_policy=Policy(database=False, schema=False, identifier=False),
            )
            if relation.database == context.target.database:
                relation = relation.include(database=False)
            self._canonical_name = relation.render()
        return self._canonical_name

    @property
    def model_materialization(self) -> Materialization:
        return Materialization.TABLE

    @property
    def relation_info(self) -> AttributeDict[str, t.Any]:
        if self.model_materialization == Materialization.VIEW:
            relation_type = RelationType.View
        elif self.model_materialization == Materialization.EPHEMERAL:
            relation_type = RelationType.CTE
        else:
            relation_type = RelationType.Table

        return AttributeDict(
            {
                "database": self.database,
                "schema": self.table_schema,
                "identifier": self.table_name,
                "type": relation_type.value,
                "quote_policy": AttributeDict(self.quoting),
            }
        )

    @property
    def tests_ref_source_dependencies(self) -> Dependencies:
        dependencies = Dependencies()
        for test in self.tests:
            dependencies = dependencies.union(test.dependencies)
        if self.name in dependencies.refs:
            dependencies.refs.remove(self.name)
        dependencies.macros = []
        return dependencies

    def check_for_circular_test_refs(self, context: DbtContext) -> None:
        """
        Checks for direct circular references between two models and raises an exception if found.
        This addresses the most common circular reference seen when importing a dbt project -
        relationship tests in both directions. In the future, we may want to increase coverage by
        checking for indirect circular references.

        Args:
            context: The dbt context this model resides within.

        Returns:
            None
        """
        for test in self.tests:
            for ref in test.dependencies.refs:
                model = context.refs[ref]
                if ref == self.name or ref in self.dependencies.refs:
                    continue
                elif self.name in model.dependencies.refs:
                    raise ConfigError(
                        f"Test '{test.name}' for model '{self.name}' depends on downstream model '{model.name}'."
                        " Move the test to the downstream model to avoid circular references."
                    )
                elif self.name in model.tests_ref_source_dependencies.refs:
                    circular_test = next(
                        test.name for test in model.tests if ref in test.dependencies.refs
                    )
                    raise ConfigError(
                        f"Circular reference detected between tests for models '{self.name}' and '{model.name}':"
                        f" '{test.name}' ({self.name}), '{circular_test}' ({model.name})."
                    )

    @property
    def sqlmesh_config_fields(self) -> t.Set[str]:
        return {"description", "owner", "stamp", "storage_format"}

    @property
    def node_name(self) -> str:
        resource_type = getattr(self, "resource_type", "model")
        node_name = f"{resource_type}.{self.package_name}.{self.name}"
        if self.version:
            node_name += f".v{self.version}"
        return node_name

    def sqlmesh_model_kwargs(
        self,
        context: DbtContext,
        column_types_override: t.Optional[t.Dict[str, ColumnConfig]] = None,
    ) -> t.Dict[str, t.Any]:
        """Get common sqlmesh model parameters"""
        self.check_for_circular_test_refs(context)
        model_dialect = self.dialect(context)
        model_context = context.context_for_dependencies(
            self.dependencies.union(self.tests_ref_source_dependencies)
        )
        jinja_macros = model_context.jinja_macros.trim(
            self.dependencies.macros, package=self.package_name
        )

        model_node: AttributeDict[str, t.Any] = AttributeDict(
            {
                k: v
                for k, v in context._manifest._manifest.nodes[self.node_name].to_dict().items()
                if k in self.dependencies.model_attrs
            }
            if context._manifest and self.node_name in context._manifest._manifest.nodes
            else {}
        )

        jinja_macros.add_globals(
            {
                "this": self.relation_info,
                "model": model_node,
                "schema": self.table_schema,
                "config": self.config_attribute_dict,
                **model_context.jinja_globals,  # type: ignore
            }
        )
        return {
            "audits": [(test.name, {}) for test in self.tests],
            "columns": column_types_to_sqlmesh(
                column_types_override or self.columns, self.dialect(context)
            )
            or None,
            "column_descriptions": column_descriptions_to_sqlmesh(self.columns) or None,
            "depends_on": {
                model.canonical_name(context) for model in model_context.refs.values()
            }.union({source.canonical_name(context) for source in model_context.sources.values()}),
            "jinja_macros": jinja_macros,
            "path": self.path,
            "pre_statements": [d.jinja_statement(hook.sql) for hook in self.pre_hook],
            "post_statements": [d.jinja_statement(hook.sql) for hook in self.post_hook],
            "tags": self.tags,
            "physical_schema_mapping": context.sqlmesh_config.physical_schema_mapping,
            "default_catalog": context.target.database,
            "grain": [d.parse_one(g, dialect=model_dialect) for g in ensure_list(self.grain)],
            **self.sqlmesh_config_kwargs,
        }

    @abstractmethod
    def to_sqlmesh(
        self, context: DbtContext, audit_definitions: t.Optional[t.Dict[str, ModelAudit]] = None
    ) -> Model:
        """Convert DBT model into sqlmesh Model"""
