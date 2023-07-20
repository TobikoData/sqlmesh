from __future__ import annotations

import typing as t
from abc import abstractmethod
from enum import Enum
from pathlib import Path

from dbt.contracts.relation import RelationType
from pydantic import Field, validator
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
    QuotingConfig,
    SqlStr,
)
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.errors import ConfigError

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


BMC = t.TypeVar("BMC", bound="BaseModelConfig")


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class Hook(DbtConfig):
    """
    Args:
        sql: The sql to execute.
        transaction: bool indicating if the hook is executed in the same transaction as the model query.
    """

    sql: SqlStr
    transaction: bool = True  # TODO not yet supported


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
    storage_format: t.Optional[str] = None
    path: Path = Path()
    dependencies: Dependencies = Dependencies()
    tests: t.List[TestConfig] = []

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
    quoting: QuotingConfig = Field(default_factory=QuotingConfig)

    @validator("pre_hook", "post_hook", pre=True)
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

    @validator("full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("grants", pre=True)
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

    @property
    def sql_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        return ".".join(
            part for part in (self.database, self.table_schema, self.table_name) if part
        )

    @property
    def model_materialization(self) -> Materialization:
        return Materialization.TABLE

    @property
    def model_dialect(self) -> t.Optional[str]:
        return None

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
                "quote_policy": AttributeDict(self.quoting.dict()),
            }
        )

    def model_function(self) -> AttributeDict[str, t.Any]:
        return AttributeDict({"config": self.attribute_dict})

    def sqlmesh_model_kwargs(self, context: DbtContext) -> t.Dict[str, t.Any]:
        """Get common sqlmesh model parameters"""
        model_context = context.context_for_dependencies(self.dependencies)
        jinja_macros = model_context.jinja_macros.trim(
            self.dependencies.macros, package=self.package_name
        )
        jinja_macros.global_objs.update(
            {
                "this": self.relation_info,
                "model": self.model_function(),
                "schema": self.table_schema,
                "config": self.attribute_dict,
                **model_context.jinja_globals,  # type: ignore
            }
        )

        optional_kwargs: t.Dict[str, t.Any] = {}
        for field in ("description", "owner", "stamp", "storage_format"):
            field_val = getattr(self, field, None) or self.meta.get(field, None)
            if field_val:
                optional_kwargs[field] = field_val

        return {
            "audits": [(test.name, {}) for test in self.tests],
            "columns": column_types_to_sqlmesh(self.columns) or None,
            "column_descriptions_": column_descriptions_to_sqlmesh(self.columns) or None,
            "depends_on": {model.sql_name for model in model_context.refs.values()}.union(
                {source.sql_name for source in model_context.sources.values()}
            ),
            "jinja_macros": jinja_macros,
            "path": self.path,
            "hash_raw_query": True,
            "pre_statements": [d.jinja_statement(hook.sql) for hook in self.pre_hook],
            "post_statements": [d.jinja_statement(hook.sql) for hook in self.post_hook],
            "tags": self.tags,
            **optional_kwargs,
        }

    @abstractmethod
    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Convert DBT model into sqlmesh Model"""
