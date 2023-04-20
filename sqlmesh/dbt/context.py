from __future__ import annotations

import typing as t
from dataclasses import dataclass, field, replace
from pathlib import Path

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaGlobalAttribute, JinjaMacroRegistry

if t.TYPE_CHECKING:
    from jinja2 import Environment

    from sqlmesh.dbt.model import ModelConfig
    from sqlmesh.dbt.seed import SeedConfig
    from sqlmesh.dbt.source import SourceConfig


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
    def target(self) -> t.Optional[TargetConfig]:
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
        if self._target is not None:
            output["target"] = self._target.attribute_dict()
        return output
