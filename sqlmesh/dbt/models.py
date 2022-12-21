from __future__ import annotations

import re
import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, validator
from sqlglot import exp, parse_one

from sqlmesh.core import dialect as d
from sqlmesh.core.model import Model, ModelKind, TimeColumn
from sqlmesh.dbt.render import render_jinja
from sqlmesh.dbt.update import UpdateStrategy, update_field
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import Executable, ExecutableKind
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import yaml


def ensure_list(val: t.Any) -> t.List[t.Any]:
    return val if isinstance(val, list) else [val]


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHERMAL = "ephemeral"


class ModelConfig(PydanticModel):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        path: The file path of the model.
        alias: Relation identifier for this model instead of the model filename
        cluster_by: Field(s) to use for clustering in data warehouses that support clustering
        database: Database the model is stored in
        docs: Documentation specific configuration
        enabled: When false, the model is ignored
        full_refresh: Forces the model to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this model
        identifier: Table name as stored in the database instead of the model filename
        incremental_strategy: Strategy used to build the incremental model
        meta: Dictionary of metadata for the model
        materialized: How the model will be materialized in the database
        perist_docs: Persist resource descriptions as column and/or relation comments in the database
        post-hook: List of SQL statements to run after the model is built
        pre-hook: List of SQL statements to run before the model is built
        schema: Custom schema name added to the model schema name
        sql_header: SQL statement to inject above create table/view as
        tags: List of tags that can be used for model grouping
        unique_key: List of columns that define row uniqueness for the model
    """

    # sqlmesh fields
    path: Path = Path()
    sql: str = ""
    time_column: t.Optional[TimeColumn] = None

    # DBT configuration fields
    alias: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    database: t.Optional[str] = None
    docs: t.Dict[str, t.Any] = {"show": True}
    enabled: bool = True
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    identifier: t.Optional[str] = None
    incremental_strategy: t.Optional[str] = None
    meta: t.Dict[str, t.Any] = {}
    materialized: Materialization = Materialization.VIEW
    persist_docs: t.Dict[str, t.Any] = {}
    post_hook: t.List[str] = Field([], alias="post-hook")
    pre_hook: t.List[str] = Field([], alias="pre-hook")
    schema_: t.Optional[str] = Field(None, alias="schema")
    sql_header: t.Optional[str] = None
    tags: t.List[str] = []
    unique_key: t.Optional[t.List[str]] = None

    @validator(
        "tags",
        "pre_hook",
        "post_hook",
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("enabled", "full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        if isinstance(v, bool):
            return v

        return bool(cls._try_str_to_bool(v))

    @validator("docs", pre=True)
    def _validate_dict(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for key, value in v.items():
            if isinstance(value, str):
                v[key] = cls._try_str_to_bool(value)

        return v

    @validator("materialized", pre=True)
    def _validate_materialization(cls, v: str) -> Materialization:
        return Materialization(v.lower())

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        for key, value in v.items():
            if isinstance(value, str):
                v[key] = cls._try_str_to_bool(value)

        return v

    @validator("persist_docs", pre=True)
    def _validate_persist_docs(cls, v: t.Dict[str, str]) -> t.Dict[str, bool]:
        return {key: bool(value) for key, value in v.items()}

    @validator("grants", pre=True)
    def _validate_grants(cls, v: t.Dict[str, str]) -> t.Dict[str, t.List[str]]:
        return {key: ensure_list(value) for key, value in v.items()}

    @classmethod
    def _try_str_to_bool(cls, val: str) -> t.Union[bool, str]:
        maybe_bool = val.lower()
        if maybe_bool not in {"true", "false"}:
            return val

        return maybe_bool == "true"

    @validator("time_column", pre=True)
    def _validate_time_column(cls, v: str | TimeColumn) -> TimeColumn:
        if isinstance(v, str):
            expression = parse_one(v)
            assert expression
            return TimeColumn.from_expression(expression)
        return v

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "alias": UpdateStrategy.REPLACE,
        "cluster_by": UpdateStrategy.REPLACE,
        "database": UpdateStrategy.REPLACE,
        "docs": UpdateStrategy.KEY_UPDATE,
        "enabled": UpdateStrategy.REPLACE,
        "full_refresh": UpdateStrategy.REPLACE,
        "grants": UpdateStrategy.KEY_APPEND,
        "identifier": UpdateStrategy.REPLACE,
        "incremental_strategy": UpdateStrategy.REPLACE,
        "meta": UpdateStrategy.KEY_UPDATE,
        "materialized": UpdateStrategy.REPLACE,
        "path": UpdateStrategy.IMMUTABLE,
        "persist_docs": UpdateStrategy.KEY_UPDATE,
        "post-hook": UpdateStrategy.APPEND,
        "pre-hook": UpdateStrategy.APPEND,
        "schema": UpdateStrategy.REPLACE,
        "sql": UpdateStrategy.IMMUTABLE,
        "sql_header": UpdateStrategy.REPLACE,
        "tags": UpdateStrategy.APPEND,
        "time_column": UpdateStrategy.IMMUTABLE,
        "unique_key": UpdateStrategy.REPLACE,
    }

    def update_with(self, config: t.Dict[str, t.Any]) -> ModelConfig:
        """
        Update this ModelConfig's fields with the passed in config fields and return as a new ModeConfig

        Args:
            config: Dict of config fields

        Returns:
            New ModelConfig updated with the passed in config fields
        """
        if not config:
            return self.copy()

        fields = self.dict()
        config = {k: v for k, v in ModelConfig(**config).dict().items() if k in config}
        for key, value in config.items():
            fields[key] = update_field(
                fields.get(key), value, self._FIELD_UPDATE_STRATEGY.get(key)
            )
        return ModelConfig(**fields)

    def replace(self, other: ModelConfig) -> None:
        """
        Replace the contents of this ModelConfig with the passed in ModelConfig.

        Args:
            other: The ModelConfig to apply to this instance
        """
        self.__dict__.update(other.dict())

    def to_sqlmesh(self) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        expressions = [
            d.Model(expressions=[exp.Property(this="name", value=self.model_name)]),
            *d.parse_model(self.sql, default_dialect=""),  # how do we get dialect?
        ]

        for jinja in expressions[1:]:
            # find all the refs here and filter the python env?
            if isinstance(jinja, d.Jinja):
                pass

        python_env = {
            "source": Executable(
                payload="""def source(source_name, table_name):
    return ".".join((source_name, table_name))
""",
            ),
            "ref": Executable(
                payload="""def ref(source_name, table_name):
    return "ref"
""",
            ),
            "sqlmesh": Executable(
                kind=ExecutableKind.VALUE,
                payload=True,
            ),
            "is_incremental": Executable(
                payload="def is_incremental(): return False",
            ),
        }

        return Model.load(
            expressions,
            path=self.path,
            python_env=python_env,
            time_column=self.time_column,
        )

    @property
    def model_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        return ".".join(part for part in (self.schema_, self.identifier) if part)

    @property
    def model_kind(self) -> ModelKind:
        """
        Get the sqlmesh ModelKind

        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.materialized
        if materialization == Materialization.TABLE:
            return ModelKind.FULL
        if materialization == Materialization.VIEW:
            return ModelKind.VIEW
        if materialization == Materialization.INCREMENTAL:
            return ModelKind.INCREMENTAL
        if materialization == Materialization.EPHERMAL:
            return ModelKind.EMBEDDED

        raise ConfigError(f"{materialization.value} materialization not supported")


if t.TYPE_CHECKING:
    Scope = t.Union[t.Tuple[()], t.Tuple[str, ...]]
    ScopedModelConfig = t.Dict[Scope, ModelConfig]


class Models:
    @classmethod
    def load(
        cls,
        project_root: Path,
        project_name: str,
        project_schema: str,
        project_raw_config: t.Dict[str, t.Any],
    ) -> t.Dict[str, ModelConfig]:
        """
        Loads the configuration of all models within the specified DBT project.

        Args:
            project_root: Path to the root directory of the DBT project
            project_name: Name of the DBT project as defined in the project yaml
            project_schema: The target database schema
            project_raw_config: The raw yaml from the project yaml file

        Returns:
            Dictionary of model names to model configuration
        """
        # Start with configs in the project file
        global_config = ModelConfig(schema=project_schema)
        configs = cls._load_project_config(project_raw_config, global_config)

        # Layer on configs in property files
        for filepath in project_root.glob("models/**/*.yml"):
            scope = cls._scope_from_path(filepath, project_root, project_name)
            configs = cls._load_properties_config(filepath, scope, configs)

        # Layer on configs from the model file and create model configs
        model_configs = {}
        for filepath in project_root.glob("models/**/*.sql"):
            scope = cls._scope_from_path(filepath, project_root, project_name)
            model_config = cls._load_model_config(filepath, scope, configs)
            if not model_config.identifier:
                raise ConfigError(f"No identifier for {filepath.name}")
            model_configs[model_config.identifier] = model_config

        return model_configs

    @classmethod
    def _load_project_config(
        cls, config: t.Dict[str, t.Any], global_config: ModelConfig
    ) -> ScopedModelConfig:
        scoped_configs: ScopedModelConfig = {}

        model_data = config.get("models")
        if not model_data:
            scoped_configs[()] = global_config
            return scoped_configs

        def load_config(data, parent, scope):
            nested_config = {}
            fields = {}
            for key, value in data.items():
                if key.startswith("+"):
                    fields[key[1:]] = value
                else:
                    nested_config[key] = value

            config = parent.update_with(fields)
            scoped_configs[scope] = config
            for key, value in nested_config.items():
                nested_scope = (*scope, key)
                load_config(value, config, nested_scope)

        load_config(model_data, global_config, ())
        print(scoped_configs.keys())
        return scoped_configs

    @classmethod
    def _load_properties_config(
        cls, filepath: Path, scope: Scope, configs: ScopedModelConfig
    ) -> ScopedModelConfig:
        with filepath.open(encoding="utf-8") as file:
            contents = yaml.load(file.read())

        model_data = contents.get("models")
        if not model_data:
            return configs

        for value in model_data:
            fields = value.get("config")
            if not fields:
                continue

            model_scope = (*scope, value["name"])
            configs[model_scope] = cls._config_for_scope(scope, configs).update_with(
                fields
            )

        return configs

    @classmethod
    def _load_model_config(
        cls, filepath: Path, scope: Scope, configs: ScopedModelConfig
    ) -> ModelConfig:
        with filepath.open(encoding="utf-8") as file:
            sql = file.read()

        model_config = cls._config_for_scope(scope, configs).copy(
            update={"path": filepath}
        )

        def config(*args, **kwargs):
            if args:
                if isinstance(args[0], dict):
                    model_config.replace(model_config.update_with(args[0]))
            if kwargs:
                model_config.replace(model_config.update_with(kwargs))

        render_jinja(sql, {"config": config})

        if not model_config.identifier and scope:
            model_config.identifier = scope[-1]

        model_config.sql = cls._remove_config_jinja(sql)

        return model_config

    @classmethod
    def _scope_from_path(cls, path: Path, root_path: Path, project_name: str) -> Scope:
        """
        DBT rolls-up configuration based on the project name and the directory structure.
        Scope mimics this structure, building a tuple containing the project name and
        directories from project root to the file, omitting the "models" directory and
        filename if a properties file.
        """
        path_from_root = path.relative_to(root_path)
        scope = (project_name, *path_from_root.parts[1:-1])
        if path.match("*.sql"):
            scope = (*scope, path_from_root.stem)
        return scope

    @classmethod
    def _config_for_scope(cls, scope: Scope, configs: ScopedModelConfig) -> ModelConfig:
        return configs.get(scope) or cls._config_for_scope(scope[0:-1], configs)

    @classmethod
    def _remove_config_jinja(cls, query: str) -> str:
        return re.sub(r"{{\s*config(.|\s)*?}}", "", query).strip()
