from __future__ import annotations

import re
import typing as t
from enum import Enum, auto
from pathlib import Path

from pydantic import Field, validator
from ruamel.yaml import YAML
from sqlglot import exp, parse_one

from sqlmesh.core import dialect as d
from sqlmesh.core.model import Model, ModelKind, TimeColumn
from sqlmesh.dbt.render import render_jinja
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import Executable, ExecutableKind
from sqlmesh.utils.pydantic import PydanticModel

DEFAULT_PROJECT_FILE = "dbt_project.yml"


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHERMAL = "ephemeral"


class UpdateStrategy(Enum):
    """Supported strategies for adding new config to existing config"""

    REPLACE = auto()  # Replace with new value
    APPEND = auto()  # Append list to existing list
    KEY_UPDATE = auto()  # Update dict key value with new dict key value
    KEY_APPEND = auto()  # Append dict key value to existing dict key value
    IMMUTABLE = auto()  # Raise if a key tries to change this value


def update_field(
    old: t.Optional[t.Any],
    new: t.Any,
    update_strategy: t.Optional[UpdateStrategy] = UpdateStrategy.REPLACE,
) -> t.Any:
    """
    Update config field with new config value

    Args:
        old: The existing config value
        new: The new config value
        update_strategy: The strategy to use when updating the field

    Returns:
        The updated field
    """
    if not old:
        return new

    if update_strategy == UpdateStrategy.IMMUTABLE:
        raise ConfigError("Cannot modify property: {old}")

    if update_strategy == UpdateStrategy.REPLACE:
        return new
    if update_strategy == UpdateStrategy.APPEND:
        if not isinstance(old, list) or not isinstance(new, list):
            raise ConfigError("APPEND behavior requires list field")

        return old + new
    if update_strategy == UpdateStrategy.KEY_UPDATE:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        combined = old.copy()
        combined.update(new)
        return combined
    if update_strategy == UpdateStrategy.KEY_APPEND:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_APPEND behavior requires dictionary field")

        combined = old.copy()
        for key, value in new.items():
            if not isinstance(value, list):
                raise ConfigError(
                    "KEY_APPEND behavior requires list values in dictionary"
                )

            old_value = combined.get(key)
            if old_value:
                if not isinstance(old_value, list):
                    raise ConfigError(
                        "KEY_APPEND behavior requires list values in dictionary"
                    )

                combined[key] = old_value + value
            else:
                combined[key] = value

        return combined

    raise ConfigError("Unknown update strategy {update_strategy}")


def ensure_list(val: t.Any) -> t.List[t.Any]:
    return val if isinstance(val, list) else [val]


class ModelConfig(PydanticModel):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        path: The file path of the model.
        alias: Relation identifier for this model instead of the model filename
        cluster_by: Field(s) to use for clustering in databases that support clustering
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


class ModelConfigBuilder:
    """
    The ModelConfigBuilder reads the model and configuration files of a DBT
    project and builds the configuration for each of the models
    """

    def __init__(self):
        self._scoped_configs = {}
        self._project_root = None
        self._project_name = None

    def build_model_configs(self, project_root: Path) -> t.Dict[str, ModelConfig]:
        """
        Build the configuration for each model in the specified DBT project.

        Args:
            project_root: Relative or absolute path to the DBT project

        Returns:
            Dict with model name as the key and a tuple containing ModelConfig
            and relative path to model file from the project root.
        """
        self._scoped_configs = {}
        self._project_root = project_root
        self._project_name = None

        self._build_project_config()
        # TODO parse profile for schema
        for file in self._project_root.glob("models/**/*.yml"):
            self._build_properties_config(file)

        configs = {}
        for file in self._project_root.glob("models/**/*.sql"):
            model_config = self._build_model_config(file)
            if not model_config.identifier:
                raise ConfigError(f"No identifier for {file}")
            configs[model_config.identifier] = model_config

        return configs

    def _build_project_config(self) -> None:
        """
        Builds ModelConfigs for each resource path specified in the project config file and
        stores them in _scoped_configs
        """
        project_config_file = DEFAULT_PROJECT_FILE
        with Path(self._project_root, project_config_file).open(
            encoding="utf-8"
        ) as file:
            contents = YAML().load(file.read())

        self._project_name = contents.get("name")
        if not self._project_name:
            raise ConfigError(f"{project_config_file} must include project name")

        model_data = contents.get("models")
        if not model_data:
            self._scoped_configs[()] = ModelConfig()
            return

        def build_config(data, parent=None):
            parent = parent or ModelConfig()
            fields = {
                key[1:]: value for key, value in data.items() if key.startswith("+")
            }
            return parent.update_with(fields)

        def build_nested_configs(data, scope):
            parent_config = self._scoped_configs[scope]
            for key, value in data.items():
                if key.startswith("+"):
                    continue

                nested_scope = (*scope, key)
                self._scoped_configs[nested_scope] = build_config(value, parent_config)
                build_nested_configs(value, nested_scope)

        scope = ()
        self._scoped_configs[scope] = build_config(model_data)
        build_nested_configs(model_data, scope)

    def _build_properties_config(self, filepath: Path) -> None:
        """
        Builds ModelConfigs for each model defined in the specified
        properties config file and stores them in _scoped_configs

        Args:
            filepath: Path to the properties file
        """
        scope = self._scope_from_path(filepath)

        with filepath.open(encoding="utf-8") as file:
            contents = YAML().load(file.read())

        model_data = contents.get("models")
        if not model_data:
            return

        for value in model_data:
            config = value.get("config")
            if not config:
                continue

            model_scope = (*scope, value["name"])
            self._scoped_configs[model_scope] = self._config_for_scope(
                scope
            ).update_with(config)

    def _build_model_config(self, filepath: Path) -> ModelConfig:
        """
        Builds ModelConfig for the specified model file and returns it

        Args:
            filepath: Path to the model file

        Returns:
            ModelConfig for the specified model file
        """
        with filepath.open(encoding="utf-8") as file:
            sql = file.read()

        scope = self._scope_from_path(filepath)
        model_config = self._config_for_scope(scope).copy(update={"path": filepath})

        def config(*args, **kwargs):
            if args:
                if isinstance(args[0], dict):
                    model_config.replace(model_config.update_with(args[0]))
            if kwargs:
                model_config.replace(model_config.update_with(kwargs))

        render_jinja(sql, {"config": config})

        if not model_config.identifier:
            model_config.identifier = scope[-1]

        model_config.sql = _remove_config_jinja(sql)

        return model_config

    def _scope_from_path(self, path: Path) -> t.Tuple[str, ...]:
        """
        Extract resource scope from path

        Args:
            path: The path

        Returns:
            tuple containing the scope of the specified path
        """
        path_from_root = path.relative_to(self._project_root)
        # models/ and file are not included in the scope
        scope = (self._project_name, *path_from_root.parts[1:-1])
        if path.match("*.sql"):
            scope = (*scope, path_from_root.stem)
        return scope

    def _config_for_scope(self, scope: t.Tuple[str, ...]) -> ModelConfig:
        """
        Gets the current config for the specified scope

        Args:
            scope: The scope

        Returns:
            The current config for the specified scope
        """
        return self._scoped_configs.get(scope) or self._config_for_scope(scope[0:-1])


class Config:
    """
    Class to obtain DBT project config
    """

    def __init__(self, project_root: t.Optional[Path] = None):
        """
        Args:
            project_root: Relative or absolute path to the DBT project
        """
        project_root = project_root or Path()
        builder = ModelConfigBuilder()
        self.models = builder.build_model_configs(project_root)

    def get_model_config(self) -> t.Dict[str, ModelConfig]:
        """
        Get the config for each model within the DBT project

        Returns:
            Dict with model name as the key and a tuple containing ModelConfig
            and relative path to model file from the project root.
        """
        return self.models


def _remove_config_jinja(query: str) -> str:
    """
    Removes jinja for config method calls from a query

    args:
        query: The query

    Returns:
        The query without the config method calls
    """
    return re.sub(r"{{\s*config(.|\s)*?}}", "", query).strip()
