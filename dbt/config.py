from __future__ import annotations

from enum import Enum, auto
import glob
from pydantic import Field, validator
from ruamel.yaml import YAML
import typing as t

from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.errors import ConfigError
from dbt.render import render_jinja

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

    if update_strategy == UpdateStrategy.REPLACE:
        return new
    elif update_strategy == UpdateStrategy.APPEND:
        if not isinstance(old, list) or not isinstance(new, list):
            raise ConfigError("APPEND behavior requires list field")

        return old + new
    elif update_strategy == UpdateStrategy.KEY_UPDATE:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        return old.update(new)
    elif update_strategy == UpdateStrategy.KEY_APPEND:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        for key, value in new:
            if old.get(key):
                return old[key] + value
            else:
                return value


class ModelConfig(PydanticModel):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
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
        if not isinstance(v, list):
            v = [v]
        return v

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
        return {key: list(set(value)) for key, value in v.items()}

    @classmethod
    def _try_str_to_bool(cls, val: str) -> t.Union[bool, str]:
        maybe_bool = val.lower()
        if maybe_bool not in {"true", "false"}:
            return val

        return maybe_bool == "true"

    _FIELD_update_strategy: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "alias": UpdateStrategy.REPLACE,
        "cluster_by": UpdateStrategy.REPLACE,
        "database": UpdateStrategy.REPLACE,
        "docs": UpdateStrategy.KEY_UPDATE,
        "enabled": UpdateStrategy.REPLACE,
        "filepath": UpdateStrategy.REPLACE,
        "full_refresh": UpdateStrategy.REPLACE,
        "grants": UpdateStrategy.KEY_APPEND,
        "identifier": UpdateStrategy.REPLACE,
        "incremental_strategy": UpdateStrategy.REPLACE,
        "meta": UpdateStrategy.KEY_UPDATE,
        "materialized": UpdateStrategy.REPLACE,
        "persist_docs": UpdateStrategy.KEY_UPDATE,
        "post-hook": UpdateStrategy.APPEND,
        "pre-hook": UpdateStrategy.APPEND,
        "schema": UpdateStrategy.REPLACE,
        "sql_header": UpdateStrategy.REPLACE,
        "tags": UpdateStrategy.APPEND,
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
        fields = self.dict()
        if not config:
            return ModelConfig(**fields)

        config = {k: v for k, v in ModelConfig(**config).dict().items() if k in config}
        for key, value in config.items():
            fields[key] = update_field(
                fields.get(key), value, self._FIELD_update_strategy.get(key)
            )
        return ModelConfig(**fields)

    def replace(self, other: ModelConfig) -> None:
        """
        Replace the contents of this ModelConfig with the passed in ModelConfig.

        Args:
            other: The ModelConfig to apply to this instance
        """
        self.__dict__.update(other.dict())


class ModelConfigBuilder:
    """
    The ModelConfigBuilder reads the model and configuration files of a DBT
    project and builds the configuration for each of the models
    """

    def __init__(self):
        self._scoped_configs = {}
        self._project_path = None
        self._project_name = None

    def build_model_configs(
        self, project_path: str
    ) -> t.Dict[str, t.Tuple[ModelConfig, str]]:
        """
        Build the configuration for each model in the specified DBT project.

        Args:
            project_path: Relative or absolute path to the DBT project

        Returns:
            Dict with model name as the key and a tuple containing ModelConfig
            and relative path to model file from the project root.
        """
        self._scoped_configs = {}
        self._project_path = project_path
        self._project_name = None

        self._build_project_config()
        # TODO parse profile for schema
        for file in glob.glob(self._full_path("models/*/*.yml")):
            self._build_properties_config(file)

        configs = {}
        for file in glob.glob(self._full_path("models/*/*.sql")):
            model_config = self._build_model_config(file)
            if not model_config.identifier:
                raise ConfigError(f"No identifier for {file}")
            configs[model_config.identifier] = (
                model_config,
                self._project_root_path(file),
            )

        return configs

    def _build_project_config(self) -> None:
        """
        Builds ModelConfigs for each resource path specified in the project config file and
        stores them in _scoped_configs
        """
        filename = DEFAULT_PROJECT_FILE
        with open(self._full_path(filename)) as file:
            contents = YAML().load(file.read())

        self._project_name = contents.get("name")
        if not self._project_name:
            raise ConfigError(f"{filename} must include project name")

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

    def _build_properties_config(self, filepath: str) -> None:
        """
        Builds ModelConfigs for each model defined in the specified
        properties config file and stores them in _scoped_configs

        Args:
            filepath: Path to the properties file
        """
        scope = self._scope_from_path(filepath)

        with open(filepath) as file:
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

    def _build_model_config(self, filepath: str) -> ModelConfig:
        """
        Builds ModelConfig for the specified model file and returns it

        Args:
            filepath: Path to the model file

        Returns:
            ModelConfig for the specified model file
        """
        with open(filepath) as file:
            sql = file.read()

        scope = self._scope_from_path(filepath)
        model_config = self._config_for_scope(scope).copy()

        def config(*args, **kwargs):
            if len(args) > 0:
                if isinstance(args[0], dict):
                    model_config.replace(model_config.update_with(args[0]))
            if kwargs:
                model_config.replace(model_config.update_with(kwargs))

        render_jinja(sql, {"config": config})

        if not model_config.identifier:
            model_config.identifier = scope[-1]

        return model_config

    def _full_path(self, path: str) -> str:
        """
        Get full path from relative path from project root

        Args:
            path: relative path from project root

        Returns:
            Full path for the specified relative path
        """
        if not self._project_path:
            return path

        return f"{self._project_path}/{path}"

    def _project_root_path(self, path: str) -> str:
        """
        Get relative path from project root from specified path

        Args:
            path: The path

        Returns:
            Relative path from project root
        """
        if not self._project_path or not path.startswith(self._project_path):
            return path

        return path[len(self._project_path) + 1 :]

    def _scope_from_path(self, path: str) -> t.Tuple[str, ...]:
        """
        Extract resource scope from path

        Args:
            path: The path

        Returns:
            tuple containing the scope of the specified path
        """
        root_path = self._project_root_path(path)
        # models/ and file are not included in the scope
        scope = (self._project_name, *root_path.split("/")[1:-1])
        if path.endswith(".sql"):
            scope = (*scope, (root_path.split("/")[-1])[:-4])

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

    def __init__(self, project_path: str = ""):
        """
        Args:
            project_path: Relative or absolute path to the DBT project
        """
        self.models: t.Dict[str, t.Tuple[ModelConfig, str]] = {}
        self.project_path = project_path

    def get_model_config(self) -> t.Dict[str, t.Tuple[ModelConfig, str]]:
        """
        Get the config for each model within the DBT project

        Returns:
            Dict with model name as the key and a tuple containing ModelConfig
            and relative path to model file from the project root.
        """
        if self.models:
            return self.models

        builder = ModelConfigBuilder()
        self.models = builder.build_model_configs(self.project_path)
        return self.models
