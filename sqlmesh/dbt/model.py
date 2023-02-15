from __future__ import annotations

import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, ModelKindName, load_model
from sqlmesh.dbt.column import ColumnConfig, yaml_to_columns
from sqlmesh.dbt.common import Dependencies, GeneralConfig
from sqlmesh.dbt.macros import ref_method, source_method, var_method
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.errors import ConfigError


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
    sql: str = ""
    time_column: t.Optional[str] = None
    table_name: t.Optional[str] = None
    _depends_on: t.Set[str] = set()
    _calls: t.Set[str] = set()
    _sources: t.Set[str] = set()
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
    post_hook: t.List[str] = Field([], alias="post-hook")
    pre_hook: t.List[str] = Field([], alias="pre-hook")
    schema_: t.Optional[str] = Field(None, alias="schema")
    sql_header: t.Optional[str] = None
    unique_key: t.Optional[t.List[str]] = None
    columns: t.Dict[str, ColumnConfig] = {}

    @validator(
        "pre_hook",
        "post_hook",
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

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

    def to_sqlmesh(
        self,
        sources: t.Dict[str, SourceConfig],
        models: t.Dict[str, ModelConfig],
        seeds: t.Dict[str, SeedConfig],
        variables: t.Dict[str, t.Any],
        macros: MacroRegistry,
        macro_dependencies: t.Dict[str, Dependencies],
    ) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        expressions = d.parse_model(
            f"""
            MODEL (
              name {self.model_name},
              kind {self.model_kind},
            );
            """
            + self.sql,
            default_dialect="",
        )

        for jinja in expressions[1:]:
            # find all the refs here and filter the python env?
            if isinstance(jinja, d.Jinja):
                pass

        source_mapping = {config.config_name: config.source_name for config in sources.values()}
        model_mapping = {name: config.model_name for name, config in models.items()}
        model_mapping.update({name: config.seed_name for name, config in seeds.items()})

        dependencies = self._dependencies(
            source_mapping, models, seeds, variables, macros, macro_dependencies
        )

        python_env = {
            "source": source_method(dependencies.sources, source_mapping),
            "ref": ref_method(dependencies.refs, model_mapping),
            "var": var_method(dependencies.variables, variables),
            **{k: v for k, v in macros.items() if k in dependencies.macros},
        }

        depends_on = {
            seeds[ref].seed_name if ref in seeds else models[ref].model_name
            for ref in dependencies.refs
        }

        return load_model(
            expressions,
            path=self.path,
            python_env=python_env,
            depends_on=depends_on,
            start=self.start,
        )

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
    def model_kind(self) -> str:
        """
        Get the sqlmesh ModelKind

        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.materialized
        if materialization == Materialization.TABLE:
            return ModelKindName.FULL.value
        if materialization == Materialization.VIEW:
            return ModelKindName.VIEW.value
        if materialization == Materialization.INCREMENTAL:
            if self.time_column:
                return f"{ModelKindName.INCREMENTAL_BY_TIME_RANGE.value} (TIME_COLUMN {self.time_column})"
            if self.unique_key:
                return f"{ModelKindName.INCREMENTAL_BY_UNIQUE_KEY.value} (UNIQUE_KEY ({','.join(self.unique_key)}))"
            raise ConfigError(
                "SQLMesh ensures idempotent incremental loads and thus does not support append."
                " Add either an unique key (merge) or a time column (insert-overwrite)."
            )
        if materialization == Materialization.EPHEMERAL:
            return ModelKindName.EMBEDDED.value
        raise ConfigError(f"{materialization.value} materialization not supported.")

    def _dependencies(
        self,
        source_mapping: t.Dict[str, str],
        models: t.Dict[str, ModelConfig],
        seeds: t.Dict[str, SeedConfig],
        variables: t.Dict[str, t.Any],
        macros: MacroRegistry,
        macro_dependencies: t.Dict[str, Dependencies],
    ) -> Dependencies:
        """Recursively gather all the dependencies for this model, including any from ephemeral parents and macros"""
        dependencies: Dependencies = self.__class__._macro_dependencies(
            self, source_mapping, models, seeds, variables, macros, macro_dependencies
        )

        for source in self._sources:
            if source not in source_mapping:
                raise ConfigError(f"Source {source} for model {self.table_name} not found.")

            dependencies.sources.add(source)

        for var, has_default_value in self._variables.items():
            if var not in variables and not has_default_value:
                raise ConfigError(f"Variable {var} for model {self.table_name} not found.")

            dependencies.variables[var] = has_default_value

        for dependency in self._depends_on:
            """Add seed/model as a dependency"""
            parent: t.Union[SeedConfig, ModelConfig, None] = seeds.get(dependency)
            if parent:
                dependencies.refs.add(dependency)
                continue

            parent = models.get(dependency)
            if not parent:
                raise ConfigError(f"Ref {dependency} for Model {self.table_name} not found.")

            dependencies.refs.add(dependency)
            if parent.materialized == Materialization.EPHEMERAL:
                parent_dependencies = parent._dependencies(
                    source_mapping, models, seeds, variables, macros, macro_dependencies
                )
                dependencies = dependencies.union(parent_dependencies)

        return dependencies

    @classmethod
    def _macro_dependencies(
        cls,
        model: ModelConfig,
        source_mapping: t.Dict[str, str],
        models: t.Dict[str, ModelConfig],
        seeds: t.Dict[str, SeedConfig],
        variables: t.Dict[str, t.Any],
        macros: MacroRegistry,
        macro_dependencies: t.Dict[str, Dependencies],
    ) -> Dependencies:
        dependencies = Dependencies()

        def add_dependency(macro: str) -> None:
            """Add macro and everything it recursively uses as dependencies"""
            if macro not in macros:
                raise ConfigError(f"Macro {call} for model {model.table_name} not found.")

            dependencies.macros.add(macro)
            if macro not in macro_dependencies:
                return

            for source in macro_dependencies[macro].sources:
                if source not in source_mapping:
                    raise ConfigError(f"Source {source} for macro {macro} not found.")
                dependencies.sources.add(source)

            for ref in macro_dependencies[macro].refs:
                if ref not in models and ref not in seeds:
                    raise ConfigError(f"Source {source} for macro {macro} not found.")
                dependencies.refs.add(ref)

            for var, has_default_value in macro_dependencies[macro].variables.items():
                if var not in variables:
                    raise ConfigError(f"Variable {var} for macro {macro} not found.")
                dependencies.variables[var] = has_default_value

            for macro_call in macro_dependencies[macro].macros:
                if macro_call not in macros:
                    raise ConfigError(f"Macro {macro_call} used by macro {macro} not found.")

                add_dependency(macro_call)

        for call in model._calls:
            add_dependency(call)

        return dependencies
