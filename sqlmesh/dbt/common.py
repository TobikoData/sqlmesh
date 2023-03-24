from __future__ import annotations

import typing as t
from dataclasses import dataclass, field, replace
from pathlib import Path

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaGlobalAttribute, JinjaMacroRegistry
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load

if t.TYPE_CHECKING:
    from jinja2 import Environment

    from sqlmesh.dbt.model import ModelConfig
    from sqlmesh.dbt.seed import SeedConfig
    from sqlmesh.dbt.source import SourceConfig

T = t.TypeVar("T", bound="GeneralConfig")


PROJECT_FILENAME = "dbt_project.yml"

JINJA_ONLY = {
    "adapter",
    "api",
    "exceptions",
    "flags",
    "load_result",
    "modules",
    "run_query",
    "statement",
    "store_result",
    "target",
}


def load_yaml(source: str | Path) -> t.OrderedDict:
    return load(source, render_jinja=False)


@dataclass
class DbtContext:
    """Context for DBT environment"""

    project_root: Path = Path()
    target_name: t.Optional[str] = None
    project_name: t.Optional[str] = None
    profile_name: t.Optional[str] = None
    project_schema: t.Optional[str] = None
    jinja_macros: JinjaMacroRegistry = field(
        default_factory=lambda: JinjaMacroRegistry(create_builtins_module="sqlmesh.dbt")
    )

    engine_adapter: t.Optional[EngineAdapter] = None

    _variables: t.Dict[str, t.Any] = field(default_factory=dict)
    _models: t.Dict[str, ModelConfig] = field(default_factory=dict)
    _seeds: t.Dict[str, SeedConfig] = field(default_factory=dict)
    _sources: t.Dict[str, SourceConfig] = field(default_factory=dict)
    _refs: t.Dict[str, str] = field(default_factory=dict)

    _target: t.Optional[TargetConfig] = None

    _jinja_environment: t.Optional[Environment] = None

    @property
    def dialect(self) -> str:
        return self.engine_adapter.dialect if self.engine_adapter is not None else ""

    @property
    def variables(self) -> t.Dict[str, t.Any]:
        return self._variables

    @variables.setter
    def variables(self, variables: t.Dict[str, t.Any]) -> None:
        self._variables = {}
        self.add_variables(variables)

    def add_variables(self, variables: t.Dict[str, t.Any]) -> None:
        self._variables.update(variables)
        self._jinja_environment = None

    @property
    def models(self) -> t.Dict[str, ModelConfig]:
        return self._models

    @models.setter
    def models(self, models: t.Dict[str, ModelConfig]) -> None:
        self._models = {}
        self._refs = {}
        self.add_models(models)

    def add_models(self, models: t.Dict[str, ModelConfig]) -> None:
        self._refs = {}
        self._models.update(models)
        self._jinja_environment = None

    @property
    def seeds(self) -> t.Dict[str, SeedConfig]:
        return self._seeds

    @seeds.setter
    def seeds(self, seeds: t.Dict[str, SeedConfig]) -> None:
        self._seeds = {}
        self._refs = {}
        self.add_seeds(seeds)

    def add_seeds(self, seeds: t.Dict[str, SeedConfig]) -> None:
        self._refs = {}
        self._seeds.update(seeds)
        self._jinja_environment = None

    @property
    def sources(self) -> t.Dict[str, SourceConfig]:
        return self._sources

    @sources.setter
    def sources(self, sources: t.Dict[str, SourceConfig]) -> None:
        self._sources = {}
        self.add_sources(sources)

    def add_sources(self, sources: t.Dict[str, SourceConfig]) -> None:
        self._sources.update(sources)
        self._jinja_environment = None

    @property
    def refs(self) -> t.Dict[str, str]:
        if not self._refs:
            self._refs = {k: v.model_name for k, v in {**self._seeds, **self._models}.items()}  # type: ignore
        return self._refs

    @property
    def target(self) -> TargetConfig:
        if not self._target:
            raise ConfigError(f"Target not set for {self.project_name}")
        return self._target

    @target.setter
    def target(self, value: TargetConfig) -> None:
        if not self.project_name:
            raise ConfigError("Project name must be set in the context in order to use a target.")

        self._target = value
        self.engine_adapter = self._target.to_sqlmesh().create_engine_adapter()
        self._jinja_environment = None

    def render(self, source: str, **kwargs: t.Any) -> str:
        return self.jinja_environment.from_string(source).render(**kwargs)

    def copy(self) -> DbtContext:
        return replace(self)

    @property
    def jinja_environment(self) -> Environment:
        if self._jinja_environment is None:
            self._jinja_environment = self.jinja_macros.build_environment(
                **self.jinja_globals, engine_adapter=self.engine_adapter
            )
        return self._jinja_environment

    @property
    def jinja_globals(self) -> t.Dict[str, JinjaGlobalAttribute]:
        refs: t.Dict[str, t.Union[ModelConfig, SeedConfig]] = {**self.models, **self.seeds}
        output: t.Dict[str, JinjaGlobalAttribute] = {
            "vars": AttributeDict(self.variables),
            "refs": AttributeDict({k: v.relation_info for k, v in refs.items()}),
            "sources": AttributeDict({k: v.relation_info for k, v in self.sources.items()}),
        }
        if self.project_name is not None:
            output["project_name"] = self.project_name
        if self._target is not None and self.project_name is not None:
            output["target"] = self._target.target_jinja(self.project_name)
        return output


class SqlStr(str):
    pass


class DbtConfig(PydanticModel):
    class Config:
        extra = "allow"
        allow_mutation = True


class GeneralConfig(DbtConfig, BaseConfig):
    """
    General DBT configuration properties for models, sources, seeds, columns, etc.

    Args:
        description: Description of element
        tests: Tests for the element
        enabled: When false, the element is ignored
        docs: Documentation specific configuration
        perist_docs: Persist resource descriptions as column and/or relation comments in the database
        tags: List of tags that can be used for element grouping
        meta: Dictionary of metadata for the element
    """

    start: t.Optional[str] = None
    description: t.Optional[str] = None
    # TODO add test support
    tests: t.Dict[str, t.Any] = {}
    enabled: bool = True
    docs: t.Dict[str, t.Any] = {"show": True}
    persist_docs: t.Dict[str, t.Any] = {}
    tags: t.List[str] = []
    meta: t.Dict[str, t.Any] = {}

    @validator("enabled", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("docs", pre=True)
    def _validate_dict(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for key, value in v.items():
            if isinstance(value, str):
                v[key] = try_str_to_bool(value)

        return v

    @validator("persist_docs", pre=True)
    def _validate_persist_docs(cls, v: t.Dict[str, str]) -> t.Dict[str, bool]:
        return {key: bool(value) for key, value in v.items()}

    @validator("tags", pre=True)
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        return parse_meta(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **BaseConfig._FIELD_UPDATE_STRATEGY,
        **{
            "tests": UpdateStrategy.KEY_UPDATE,
            "docs": UpdateStrategy.KEY_UPDATE,
            "persist_docs": UpdateStrategy.KEY_UPDATE,
            "tags": UpdateStrategy.EXTEND,
            "meta": UpdateStrategy.KEY_UPDATE,
        },
    }

    _SQL_FIELDS: t.ClassVar[t.List[str]] = []

    def replace(self, other: T) -> None:
        """
        Replace the contents of this instance with the passed in instance.

        Args:
            other: The instance to apply to this instance
        """
        for field in other.__fields_set__:
            setattr(self, field, getattr(other, field))

    def render_config(self: T, context: DbtContext) -> T:
        def render_value(val: t.Any) -> t.Any:
            if type(val) is not SqlStr and type(val) is str:
                val = context.render(val)
            elif isinstance(val, GeneralConfig):
                for name in val.__fields__:
                    setattr(val, name, render_value(getattr(val, name)))
            elif isinstance(val, list):
                for i in range(len(val)):
                    val[i] = render_value(val[i])
            elif isinstance(val, set):
                for set_val in val:
                    val.remove(set_val)
                    val.add(render_value(set_val))
            elif isinstance(val, dict):
                for k in val:
                    val[k] = render_value(val[k])

            return val

        rendered = self.copy(deep=True)
        for name in rendered.__fields__:
            setattr(rendered, name, render_value(getattr(rendered, name)))

        return rendered


def parse_meta(v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    for key, value in v.items():
        if isinstance(value, str):
            v[key] = try_str_to_bool(value)

    return v
