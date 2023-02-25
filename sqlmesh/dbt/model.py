from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

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
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
    yaml_to_columns,
)
from sqlmesh.dbt.common import DbtContext, Dependencies, GeneralConfig, SqlStr
from sqlmesh.dbt.macros import MacroConfig
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.date import date_dict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import ENVIRONMENT
from sqlmesh.utils.metaprogramming import Executable, prepare_env


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
        table_name: Table name as stored in the database instead of the model filename
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
    table_name: t.Optional[str] = None
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
    def model_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        schema = "_".join(part for part in (self.target_schema, self.schema_) if part)
        return ".".join(part for part in (schema, self.alias or self.table_name) if part)

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

    def to_sqlmesh(
        self,
        context: DbtContext,
        models: t.Dict[str, ModelConfig],
        macros: t.Dict[str, MacroConfig],
    ) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        render_python_env = {
            **context.builtin_python_env,
            **{k: v.macro for k, v in macros.items()},
        }

        self._update_with_rendered_query(context, render_python_env)
        dependencies = self._all_dependencies(context, models, macros)
        model_context = self._context_for_dependencies(context, dependencies)

        python_env = {
            **model_context.builtin_python_env,
            **{k: v.macro for k, v in macros.items() if k in dependencies.macros},
        }

        depends_on = {model_context.refs[ref] for ref in dependencies.refs}

        expressions = d.parse(self.sql_no_config)

        return create_sql_model(
            self.model_name,
            expressions[-1],
            kind=self.model_kind,
            statements=expressions[0:-1],
            columns=column_types_to_sqlmesh(self.columns) or None,
            column_descriptions_=column_descriptions_to_sqlmesh(self.columns) or None,
            python_env=python_env,
            depends_on=depends_on,
            start=self.start,
        )

    def _all_dependencies(
        self,
        context: DbtContext,
        models: t.Dict[str, ModelConfig],
        macros: t.Dict[str, MacroConfig],
    ) -> Dependencies:
        """Recursively gather all the dependencies for this model, including any from ephemeral parents and macros"""
        dependencies: Dependencies = self._macro_dependencies(context, macros)

        for source in self._dependencies.sources:
            if source not in context.sources:
                raise ConfigError(f"Source {source} for model {self.table_name} not found.")

            dependencies.sources.add(source)

        for var in self._dependencies.variables:
            dependencies.variables.add(var)

        for dependency in self._dependencies.refs:
            """Add seed/model as a dependency"""
            if dependency not in context.refs:
                raise ConfigError(f"Ref {dependency} for Model {self.table_name} not found.")

            dependencies.refs.add(dependency)

            model = models.get(dependency)
            if model and model.materialized == Materialization.EPHEMERAL:
                parent_dependencies = model._all_dependencies(context, models, macros)
                dependencies = dependencies.union(parent_dependencies)

        return dependencies

    def _macro_dependencies(
        self,
        context: DbtContext,
        macros: t.Dict[str, MacroConfig],
    ) -> Dependencies:
        dependencies = Dependencies()

        def add_dependency(macro: str) -> None:
            """Add macro and everything it recursively uses as dependencies"""
            if macro not in macros:
                raise ConfigError(f"Macro '{macro}' for model '{self.table_name}' not found.")

            dependencies.macros.add(macro)

            macros_dependencies = macros[macro].dependencies

            for source in macros_dependencies.sources:
                if source not in context.sources:
                    raise ConfigError(f"Source '{source}' for macro '{macro}' not found.")
                dependencies.sources.add(source)

            for ref in macros_dependencies.refs:
                if ref not in context.refs:
                    raise ConfigError(f"Source '{source}' for macro '{macro}' not found.")
                dependencies.refs.add(ref)

            for var in macros_dependencies.variables:
                # TODO we should be checking if the dependency is optional
                dependencies.variables.add(var)

            for macro_call in macros_dependencies.macros:
                if macro_call not in macros:
                    raise ConfigError(f"Macro '{macro_call}' used by macro '{macro}' not found.")

                add_dependency(macro_call)

        for call in self._dependencies.macros:
            add_dependency(call)

        return dependencies

    def _update_with_rendered_query(
        self,
        context: DbtContext,
        python_env: t.Dict[str, Executable],
    ) -> None:
        def _ref(package_name: str, model_name: t.Optional[str] = None) -> str:
            if package_name not in context.refs:
                raise ConfigError(
                    f"Model '{package_name}' was not found for model '{self.table_name}'."
                )
            self._dependencies.refs.add(package_name)
            return context.refs[package_name]

        def _var(name: str, default: t.Optional[str] = None) -> t.Any:
            if default is None and name not in context.variables:
                raise ConfigError(f"Variable '{name}' was not found for model '{self.table_name}'.")
            self._dependencies.variables.add(name)
            return context.variables.get(name, default)

        def _source(source_name: str, table_name: str) -> str:
            full_name = ".".join([source_name, table_name])
            if full_name not in context.sources:
                raise ConfigError(
                    f"Source '{full_name}' was not found for model '{self.table_name}'."
                )
            self._dependencies.sources.add(full_name)
            return context.sources[full_name]

        def _config(*args: t.Any, **kwargs: t.Any) -> str:
            if args and isinstance(args[0], dict):
                self.replace(self.update_with(args[0]))
            if kwargs:
                self.replace(self.update_with(kwargs))
            return ""

        env = prepare_env(python_env)
        env["log"] = lambda msg, info=False: ""

        jinja_methods = {**env, **date_dict(c.EPOCH_DS, c.EPOCH_DS, c.EPOCH_DS)}

        # Overwrite conflicting jinja methods with the dependency capture version
        jinja_methods.update(
            {
                "config": _config,
                "ref": _ref,
                "var": _var,
                "source": _source,
            }
        )

        ENVIRONMENT.from_string("\n".join((*env[c.JINJA_MACROS], self.sql))).render(jinja_methods)

    def _context_for_dependencies(
        self, context: DbtContext, dependencies: Dependencies
    ) -> DbtContext:
        model_context = context.copy()

        model_context.sources = {
            name: value for name, value in context.sources.items() if name in dependencies.sources
        }
        model_context.refs = {
            name: value for name, value in context.refs.items() if name in dependencies.refs
        }
        model_context.variables = {
            name: value
            for name, value in context.variables.items()
            if name in dependencies.variables
        }

        return model_context
