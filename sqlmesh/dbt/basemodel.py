from __future__ import annotations

import typing as t
from abc import abstractmethod
from enum import Enum
from pathlib import Path
import logging

from pydantic import Field
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.config.common import VirtualEnvironmentMode
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
    RAW_CODE_KEY,
    SqlStr,
    sql_str_validator,
)
from sqlmesh.dbt.relation import Policy, RelationType
from sqlmesh.dbt.test import TestConfig
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.dag import find_path_with_dfs
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import field_validator

if t.TYPE_CHECKING:
    from sqlmesh.core.audit.definition import ModelAudit
    from sqlmesh.dbt.context import DbtContext


BMC = t.TypeVar("BMC", bound="BaseModelConfig")


logger = logging.getLogger(__name__)


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
    event_time: t.Optional[str] = None

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

        extras = {}
        if DBT_VERSION >= (1, 9, 0) and self.event_time:
            extras["event_time_filter"] = {
                "field_name": self.event_time,
            }

        return AttributeDict(
            {
                "database": self.database,
                "schema": self.table_schema,
                "identifier": self.table_name,
                "type": relation_type.value,
                "quote_policy": AttributeDict(self.quoting),
                **extras,
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

    def remove_tests_with_invalid_refs(self, context: DbtContext) -> None:
        """
        Removes tests that reference models or sources that do not exist in the context in order to match dbt behavior.

        Args:
            context: The dbt context this model resides within.

        Returns:
            None
        """
        self.tests = [
            test
            for test in self.tests
            if all(ref in context.refs for ref in test.dependencies.refs)
            and all(source in context.sources for source in test.dependencies.sources)
        ]

    def fix_circular_test_refs(self, context: DbtContext) -> None:
        """
        Checks for circular references between models and moves tests to break cycles.
        This handles both direct circular references (A -> B -> A) and indirect circular
        references (A -> B -> C -> A). Tests are moved to the model that appears latest
        in the dependency chain to ensure the cycle is broken.

        Args:
            context: The dbt context this model resides within.

        Returns:
            None
        """
        for test in self.tests.copy():
            for ref in test.dependencies.refs:
                if ref == self.name or ref in self.dependencies.refs:
                    continue

                # Check if moving this test would create or maintain a cycle
                cycle_path = self._find_circular_path(ref, context, set())
                if cycle_path:
                    # Find the model in the cycle that should receive the test
                    # We want to move to the model that appears latest in the dependency chain
                    target_model_name = self._select_target_model_for_test(cycle_path, context)
                    target_model = context.refs[target_model_name]

                    logger.info(
                        f"Moving test '{test.name}' from model '{self.name}' to '{target_model_name}' "
                        f"to avoid circular reference through path: {' -> '.join(cycle_path)}"
                    )
                    target_model.tests.append(test)
                    self.tests.remove(test)
                    break

    def _find_circular_path(
        self, ref: str, context: DbtContext, visited: t.Set[str]
    ) -> t.Optional[t.List[str]]:
        """
        Find if there's a circular dependency path from ref back to this model.

        Args:
            ref: The model name to start searching from
            context: The dbt context
            visited: Set of model names already visited in this path

        Returns:
            List of model names forming the circular path, or None if no cycle exists
        """
        # Build a graph of all models and their dependencies from the context
        graph: t.Dict[str, t.Set[str]] = {}

        def build_graph_from_node(node_name: str, current_visited: t.Set[str]) -> None:
            if node_name in current_visited or node_name in graph:
                return
            current_visited.add(node_name)

            model = context.refs[node_name]
            # Include both direct model dependencies and test dependencies
            all_refs = model.dependencies.refs | model.tests_ref_source_dependencies.refs
            graph[node_name] = all_refs.copy()

            # Recursively build graph for dependencies
            for dep in all_refs:
                build_graph_from_node(dep, current_visited)

        # Build the graph starting from the ref, including visited nodes to avoid infinite recursion
        build_graph_from_node(ref, visited.copy())

        # Add self.name to the graph if it's not already there
        if self.name not in graph:
            graph[self.name] = set()

        # Use the shared DFS function to find path from ref to self.name
        return find_path_with_dfs(graph, start_node=ref, target_node=self.name)

    def _select_target_model_for_test(self, cycle_path: t.List[str], context: DbtContext) -> str:
        """
        Select which model in the cycle should receive the test.
        We select the model that has the most downstream dependencies in the cycle

        Args:
            cycle_path: List of model names in the circular dependency path
            context: The dbt context

        Returns:
            Name of the model that should receive the test
        """
        # Count how many other models in the cycle each model depends on
        dependency_counts = {}

        for model_name in cycle_path:
            model = context.refs[model_name]
            all_refs = model.dependencies.refs | model.tests_ref_source_dependencies.refs
            count = len([ref for ref in all_refs if ref in cycle_path])
            dependency_counts[model_name] = count

        # Return the model with the fewest dependencies within the cycle
        # (i.e., the most downstream model in the cycle)
        if dependency_counts:
            return min(dependency_counts, key=dependency_counts.get)  # type: ignore
        # Fallback to the last model in the path
        return cycle_path[-1]

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
        self.remove_tests_with_invalid_refs(context)
        self.fix_circular_test_refs(context)

        dependencies = self.dependencies.copy()
        if dependencies.has_dynamic_var_names:
            # Include ALL variables as dependencies since we couldn't determine
            # precisely which variables are referenced in the model
            dependencies.variables |= set(context.variables)

        model_dialect = self.dialect(context)
        model_context = context.context_for_dependencies(
            dependencies.union(self.tests_ref_source_dependencies)
        )
        jinja_macros = model_context.jinja_macros.trim(
            dependencies.macros, package=self.package_name
        )
        jinja_macros.add_globals(self._model_jinja_context(model_context, dependencies))

        model_kwargs = {
            "audits": [(test.name, {}) for test in self.tests],
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

        # dbt doesn't respect the data_type field for DDL statements– instead, it optionally uses
        # it to validate the actual data types at runtime through contracts or external plugins.
        # Only the `columns_types` config of seed models is actually respected. We don't set the
        # columns attribute to self.columns intentionally in all other cases, as that could result
        # in unfaithful types when models are materialized.
        #
        # See:
        # - https://docs.getdbt.com/reference/resource-properties/columns
        # - https://docs.getdbt.com/reference/resource-configs/contract
        # - https://docs.getdbt.com/reference/resource-configs/column_types
        if column_types_override:
            model_kwargs["columns"] = (
                column_types_to_sqlmesh(column_types_override, self.dialect(context)) or None
            )

        return model_kwargs

    @abstractmethod
    def to_sqlmesh(
        self,
        context: DbtContext,
        audit_definitions: t.Optional[t.Dict[str, ModelAudit]] = None,
        virtual_environment_mode: VirtualEnvironmentMode = VirtualEnvironmentMode.default,
    ) -> Model:
        """Convert DBT model into sqlmesh Model"""

    def _model_jinja_context(
        self, context: DbtContext, dependencies: Dependencies
    ) -> t.Dict[str, t.Any]:
        if context._manifest and self.node_name in context._manifest._manifest.nodes:
            attributes = context._manifest._manifest.nodes[self.node_name].to_dict()
            if dependencies.model_attrs.all_attrs:
                model_node: AttributeDict[str, t.Any] = AttributeDict(attributes)
            else:
                model_node = AttributeDict(
                    filter(lambda kv: kv[0] in dependencies.model_attrs.attrs, attributes.items())
                )

            # We exclude the raw SQL code to reduce the payload size. It's still accessible through
            # the JinjaQuery instance stored in the resulting SQLMesh model's `query` field.
            model_node.pop(RAW_CODE_KEY, None)
        else:
            model_node = AttributeDict({})

        return {
            "this": self.relation_info,
            "model": model_node,
            "schema": self.table_schema,
            "config": self.config_attribute_dict,
            **context.jinja_globals,
        }
