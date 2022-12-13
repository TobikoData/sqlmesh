from __future__ import annotations
import typing as t
from sqlmesh.utils.pydantic import PydanticModel
from pydantic import Field, validator
from ruamel.yaml import YAML
from enum import Enum, auto
from ruamel.yaml import YAML
from sqlmesh.utils.errors import ConfigError
import glob
from dbt.render import render_jinja

DEFAULT_PROJECT_FILE = "dbt_project.yml"


class Materialization(str, Enum):
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHERMAL = "ephemeral"


class UpdateBehavior(Enum):
    REPLACE = auto()  # Replace with new value
    APPEND = auto()  # Append list to existing list
    KEY_UPDATE = auto()  # Update dict key value with new dict key value
    KEY_APPEND = auto()  # Append dict key value to existing dict key value


def update_field(
    old: t.Optional[t.Any],
    new: t.Any,
    update_behavior: t.Optional[UpdateBehavior] = UpdateBehavior.REPLACE,
) -> t.Any:
    if not old:
        return new

    if update_behavior == UpdateBehavior.REPLACE:
        return new
    elif update_behavior == UpdateBehavior.APPEND:
        if not isinstance(old, list) or not isinstance(new, list):
            raise ConfigError("APPEND behavior requires list field")

        return old + new
    elif update_behavior == UpdateBehavior.KEY_UPDATE:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        return old.update(new)
    elif update_behavior == UpdateBehavior.KEY_APPEND:
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise ConfigError("KEY_UPDATE behavior requires dictionary field")

        for key, value in new:
            if old.get(key):
                return old[key] + value
            else:
                return value


class ModelConfig(PydanticModel):
    alias: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    database: t.Optional[str] = None
    docs: t.Dict[str, t.Any] = {"show": True}
    enabled: bool = True
    filepath: t.Optional[str]
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    identifier: t.Optional[str] = None
    incremental_strategy: t.Optional[str] = None
    meta: t.Dict[str, t.Any] = {}
    materialized: Materialization = Materialization.VIEW
    packages: t.List[str] = []
    persist_docs: t.Dict[str, t.Any] = {}
    post_hook: t.List[str] = Field([], alias="post-hook")
    pre_hook: t.List[str] = Field([], alias="pre-hook")
    schema_: t.Optional[str] = Field(None, alias="schema")
    sql_header: t.Optional[str] = None
    tags: t.List[str] = []
    unique_key: t.Optional[t.List[str]] = None

    @validator(
        "tags",
        "packages",
        "pre_hook",
        "post_hook",
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_unique_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return list(set(v))

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

    _FIELD_UPDATE_BEHAVIOR: t.ClassVar[t.Dict[str, UpdateBehavior]] = {
        "alias": UpdateBehavior.REPLACE,
        "cluster_by": UpdateBehavior.REPLACE,
        "database": UpdateBehavior.REPLACE,
        "docs": UpdateBehavior.KEY_UPDATE,
        "enabled": UpdateBehavior.REPLACE,
        "filepath": UpdateBehavior.REPLACE,
        "full_refresh": UpdateBehavior.REPLACE,
        "grants": UpdateBehavior.KEY_APPEND,
        "identifier": UpdateBehavior.REPLACE,
        "incremental_strategy": UpdateBehavior.REPLACE,
        "meta": UpdateBehavior.KEY_UPDATE,
        "materialized": UpdateBehavior.REPLACE,
        "packages": UpdateBehavior.APPEND,
        "persist_docs": UpdateBehavior.KEY_UPDATE,
        "post-hook": UpdateBehavior.APPEND,
        "pre-hook": UpdateBehavior.APPEND,
        "schema": UpdateBehavior.REPLACE,
        "sql_header": UpdateBehavior.REPLACE,
        "tags": UpdateBehavior.APPEND,
        "unique_key": UpdateBehavior.REPLACE,
    }

    def update_with(self, data: t.Dict[str, t.Any]) -> ModelConfig:
        fields = self.dict()
        new_fields = {k: v for k, v in ModelConfig(**data).dict().items() if k in data}
        for key, value in new_fields.items():
            fields[key] = update_field(
                fields.get(key), value, self._FIELD_UPDATE_BEHAVIOR.get(key)
            )
        return ModelConfig(**fields)

    def replace(self, other: ModelConfig) -> None:
        self.__dict__.update(other.dict())


class ModelConfigBuilder:
    def __init__(self):
        self._model_configs = {}
        self._project_path = None
        self._project_name = None

    def get_model_configs(self, project_path: str) -> t.List[ModelConfig]:
        self._model_configs = {}
        self._project_path = project_path
        self._project_name = None

        # TODO parse profile for schema

        self._build_project_config()
        for file in glob.glob(self._full_path("models/*/*.yml")):
            self._build_properties_config(file)

        for file in glob.glob(self._full_path("models/*/*.sql")):
            self._build_model_config(file)

        return [config for config in self._model_configs.values() if config.filepath]

    def _build_project_config(self, filename: t.Optional[str] = None) -> None:
        if not filename:
            filename = DEFAULT_PROJECT_FILE

        with open(self._full_path(filename)) as file:
            contents = YAML().load(file.read())

        self._project_name = contents.get("name")
        if not self._project_name:
            raise ConfigError(f"{filename} must include project name")
    
        model_data = contents.get("models")
        if not model_data:
            self._model_configs[()] = ModelConfig()
            return

        def build_config(data, parent=None):
            parent = parent or ModelConfig()
            fields = {
                key[1:]: value for key, value in data.items() if key.startswith("+")
            }
            return parent.update_with(fields)

        def build_nested_configs(data, scope):
            parent_config = self._model_configs[scope]
            for key, value in data.items():
                if key.startswith("+"):
                    continue

                nested_scope = (*scope, key)
                self._model_configs[nested_scope] = build_config(value, parent_config)
                build_nested_configs(value, nested_scope)

        scope = ()
        self._model_configs[scope] = build_config(model_data)
        build_nested_configs(model_data, scope)

    def _build_properties_config(self, filepath: str) -> None:
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
            self._model_configs[model_scope] = self._config_for_scope(
                scope
            ).update_with(config)

    def _build_model_config(self, filepath: str) -> None:
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
        model_config.filepath = filepath
        self._model_configs[scope] = model_config

    def _full_path(self, path: str) -> str:
        if not self._project_path:
            return path

        return f"{self._project_path}/{path}"
    
    def _project_root_path(self, path: str) -> str:
        if not self._project_path:
            return path
        
        return path[len(self._project_path) + 1:]


    def _scope_from_path(self, path: str) -> t.Tuple[str, ...]:
        root_path = self._project_root_path(path)
        # models/ and file are not included in the scope
        scope = (self._project_name, *root_path.split("/")[1:-1])
        if path.endswith(".sql"):
            scope = (*scope, (root_path.split("/")[-1])[:-4])

        return scope


    def _config_for_scope(self, scope: t.Tuple[str, ...]) -> ModelConfig:
        return self._model_configs.get(scope) or self._config_for_scope(scope[0:-1])


class Config:
    def __init__(self, project_path: str = ""):
        self.models: t.List[ModelConfig] = []
        self.project_path = project_path

    def get_model_config(self) -> t.List[ModelConfig]:
        if self.models:
            return self.models

        builder = ModelConfigBuilder()
        self.models = builder.get_model_configs(self.project_path)
        return self.models
