from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import RelationType
from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    Model,
    ModelKind,
    ModelKindName,
    create_sql_model,
)
from sqlmesh.dbt.adapter import ParsetimeAdapter
from sqlmesh.dbt.builtin import create_builtins, generate_this
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
    yaml_to_columns,
)
from sqlmesh.dbt.common import DbtContext, Dependencies, GeneralConfig, SqlStr
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.date import date_dict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroReference


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class ModelConfig(GeneralConfig):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        path: The file path of the model
        target_schema: The schema for the profile target
        sql: The model sql
        time_column: The name of the time column
        start: The earliest date that the model will be backfileld for
        alias: Relation identifier for this model instead of the model filename
        cluster_by: Field(s) to use for clustering in data warehouses that support clustering
        database: Database the model is stored in
        full_refresh: Forces the model to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this model
        incremental_strategy: Strategy used to build the incremental model
        materialized: How the model will be materialized in the database
        post-hook: List of SQL statements to run after the model is built
        pre-hook: List of SQL statements to run before the model is built
        schema: Custom schema name added to the model schema name
        sql_header: SQL statement to inject above create table/view as
        unique_key: List of columns that define row uniqueness for the model
        columns: Columns within the model
    """

    # sqlmesh fields
    path: Path = Path()
    target_schema: str = ""
    sql: SqlStr = SqlStr("")
    time_column: t.Optional[str] = None
    filename: t.Optional[str] = None
    _dependencies: Dependencies = Dependencies()
    _variables: t.Dict[str, bool] = {}

    # DBT configuration fields
    start: t.Optional[str] = None
    alias: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    database: t.Optional[str] = None
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    incremental_strategy: t.Optional[str] = None
    materialized: Materialization = Materialization.VIEW
    post_hook: t.List[SqlStr] = Field([], alias="post-hook")
    pre_hook: t.List[SqlStr] = Field([], alias="pre-hook")
    schema_: t.Optional[str] = Field(None, alias="schema")
    sql_header: t.Optional[str] = None
    unique_key: t.Optional[t.List[str]] = None
    columns: t.Dict[str, ColumnConfig] = {}

    # redshift
    bind: t.Optional[bool] = None

    @validator(
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("pre_hook", "post_hook", pre=True)
    def _validate_hooks(cls, v: t.Union[str, t.List[t.Union[SqlStr, str]]]) -> t.List[SqlStr]:
        return [SqlStr(val) for val in ensure_list(v)]

    @validator("sql", pre=True)
    def _validate_sql(cls, v: t.Union[str, SqlStr]) -> SqlStr:
        return SqlStr(v)

    @validator("full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("materialized", pre=True)
    def _validate_materialization(cls, v: str) -> Materialization:
        return Materialization(v.lower())

    @validator("grants", pre=True)
    def _validate_grants(cls, v: t.Dict[str, str]) -> t.Dict[str, t.List[str]]:
        return {key: ensure_list(value) for key, value in v.items()}

    @validator("columns", pre=True)
    def _validate_columns(cls, v: t.Any) -> t.Dict[str, ColumnConfig]:
        if not isinstance(v, dict) or all(isinstance(col, ColumnConfig) for col in v.values()):
            return v

        return yaml_to_columns(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{
            "grants": UpdateStrategy.KEY_EXTEND,
            "path": UpdateStrategy.IMMUTABLE,
            "post-hook": UpdateStrategy.EXTEND,
            "pre-hook": UpdateStrategy.EXTEND,
            "sql": UpdateStrategy.IMMUTABLE,
            "time_column": UpdateStrategy.IMMUTABLE,
            "columns": UpdateStrategy.KEY_EXTEND,
        },
    }

    @property
    def table_schema(self) -> str:
        """
        Get the full schema name
        """
        return "_".join(part for part in (self.target_schema, self.schema_) if part)

    @property
    def table_name(self) -> str:
        """
        Get the table name
        """
        return self.alias or self.path.stem

    @property
    def model_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        return ".".join(
            part for part in (self.database, self.table_schema, self.table_name) if part
        )

    @property
    def model_kind(self) -> ModelKind:
        """
        Get the sqlmesh ModelKind

        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.materialized
        if materialization == Materialization.TABLE:
            return ModelKind(name=ModelKindName.FULL)
        if materialization == Materialization.VIEW:
            return ModelKind(name=ModelKindName.VIEW)
        if materialization == Materialization.INCREMENTAL:
            if self.time_column:
                return IncrementalByTimeRangeKind(time_column=self.time_column)
            if self.unique_key:
                return IncrementalByUniqueKeyKind(unique_key=self.unique_key)
            raise ConfigError(
                "SQLMesh ensures idempotent incremental loads and thus does not support append."
                " Add either an unique key (merge) or a time column (insert-overwrite)."
            )
        if materialization == Materialization.EPHEMERAL:
            return ModelKind(name=ModelKindName.EMBEDDED)
        raise ConfigError(f"{materialization.value} materialization not supported.")

    @property
    def sql_no_config(self) -> str:
        return re.sub(r"{{\s*config(.|\s)*?}}", "", self.sql).strip()

    def render_config(self: ModelConfig, context: DbtContext) -> ModelConfig:
        rendered = super().render_config(context)
        rendered = ModelSqlRenderer(context, rendered).enriched_config

        rendered_dependencies = rendered._dependencies
        for dependency in rendered_dependencies.refs:
            model = context.models.get(dependency)
            if model and model.materialized == Materialization.EPHEMERAL:
                rendered._dependencies = rendered._dependencies.union(
                    model.render_config(context)._dependencies
                )
                rendered._dependencies.refs.discard(dependency)

        return rendered

    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        dependencies = self._dependencies
        model_context = self._context_for_dependencies(context, dependencies)
        depends_on = {model_context.refs[ref] for ref in dependencies.refs}
        expressions = d.parse(self.sql_no_config)

        if not expressions:
            raise ConfigError(f"Model '{self.table_name}' must have a query.")

        return create_sql_model(
            self.model_name,
            expressions[-1],
            kind=self.model_kind,
            statements=expressions[0:-1],
            columns=column_types_to_sqlmesh(self.columns) or None,
            column_descriptions_=column_descriptions_to_sqlmesh(self.columns) or None,
            python_env=model_context.create_python_env({"this": generate_this(self)}),
            jinja_macros=model_context.jinja_macros.trim(dependencies.macros),
            depends_on=depends_on,
            start=self.start,
            path=self.path,
        )

    @property
    def relation_info(self) -> t.Dict[str, t.Any]:
        if self.materialized == Materialization.VIEW:
            relation_type = RelationType.View
        elif self.materialized == Materialization.EPHEMERAL:
            relation_type = RelationType.CTE
        else:
            relation_type = RelationType.Table

        return {
            "database": self.database,
            "schema": self.table_schema,
            "identifier": self.table_name,
            "type": relation_type.value,
        }

    def _context_for_dependencies(
        self, context: DbtContext, dependencies: Dependencies
    ) -> DbtContext:
        model_context = context.copy()

        model_context.sources = {
            name: value for name, value in context.sources.items() if name in dependencies.sources
        }
        model_context.seeds = {
            name: value for name, value in context.seeds.items() if name in dependencies.refs
        }
        model_context.models = {
            name: value for name, value in context.models.items() if name in dependencies.refs
        }
        model_context.variables = {
            name: value
            for name, value in context.variables.items()
            if name in dependencies.variables
        }

        return model_context


class ModelSqlRenderer:
    def __init__(self, context: DbtContext, config: ModelConfig):
        self.context = context
        self.config = config

        self._captured_dependencies: Dependencies = Dependencies()
        self._rendered_sql: t.Optional[str] = None
        self._enriched_config: ModelConfig = config.copy()

        self._jinja_globals = create_builtins(
            jinja_macros=context.jinja_macros,
            jinja_globals={
                **date_dict(c.EPOCH_DS, c.EPOCH_DS, c.EPOCH_DS),
                "config": self._config,
                "ref": self._ref,
                "var": self._var,
                "source": self._source,
                "this": generate_this(self.config),
            },
            engine_adapter=None,
        )

        # Set the adapter separately since it requires jinja globals to passed into it.
        self._jinja_globals["adapter"] = ModelSqlRenderer.TrackingAdapter(
            self,
            context.jinja_macros,
            jinja_globals=self._jinja_globals,
            dialect=context.engine_adapter.dialect if context.engine_adapter else "",
        )

    @property
    def enriched_config(self) -> ModelConfig:
        if self._rendered_sql is None:
            self.render()
            self._enriched_config._dependencies = self._enriched_config._dependencies.union(
                self._captured_dependencies
            )
        return self._enriched_config

    def render(self) -> str:
        if self._rendered_sql is None:
            registry = self.context.jinja_macros
            self._rendered_sql = (
                registry.build_environment(**self._jinja_globals)
                .from_string(self.config.sql)
                .render()
            )
        return self._rendered_sql

    def _ref(self, package_name: str, model_name: t.Optional[str] = None) -> BaseRelation:
        if package_name in self.context.models:
            relation = BaseRelation.create(**self.context.models[package_name].relation_info)
        elif package_name in self.context.seeds:
            relation = BaseRelation.create(**self.context.seeds[package_name].relation_info)
        else:
            raise ConfigError(
                f"Model '{package_name}' was not found for model '{self.config.table_name}'."
            )
        self._captured_dependencies.refs.add(package_name)
        return relation

    def _var(self, name: str, default: t.Optional[str] = None) -> t.Any:
        if default is None and name not in self.context.variables:
            raise ConfigError(
                f"Variable '{name}' was not found for model '{self.config.table_name}'."
            )
        self._captured_dependencies.variables.add(name)
        return self.context.variables.get(name, default)

    def _source(self, source_name: str, table_name: str) -> BaseRelation:
        full_name = ".".join([source_name, table_name])
        if full_name not in self.context.sources:
            raise ConfigError(
                f"Source '{full_name}' was not found for model '{self.config.table_name}'."
            )
        self._captured_dependencies.sources.add(full_name)
        return BaseRelation.create(**self.context.sources[full_name].relation_info)

    def _config(self, *args: t.Any, **kwargs: t.Any) -> str:
        if args and isinstance(args[0], dict):
            self._enriched_config = self._enriched_config.update_with(args[0])
        if kwargs:
            self._enriched_config = self._enriched_config.update_with(kwargs)
        return ""

    class TrackingAdapter(ParsetimeAdapter):
        def __init__(self, outer_self: ModelSqlRenderer, *args: t.Any, **kwargs: t.Any):
            super().__init__(*args, **kwargs)
            self.outer_self = outer_self
            self.context = outer_self.context

        def dispatch(self, name: str, package: t.Optional[str] = None) -> t.Callable:
            macros = (
                self.context.jinja_macros.packages.get(package, {})
                if package is not None
                else self.context.jinja_macros.root_macros
            )
            for target_name in macros:
                if target_name.endswith(f"__{name}"):
                    self.outer_self._captured_dependencies.macros.add(
                        MacroReference(package=package, name=target_name)
                    )
            return super().dispatch(name, package=package)
