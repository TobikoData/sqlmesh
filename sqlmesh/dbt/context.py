from __future__ import annotations

import typing as t
from dataclasses import dataclass, field, replace
from pathlib import Path

from sqlmesh.core.config import Config as SQLMeshConfig
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.jinja import JinjaGlobalAttribute, JinjaMacroRegistry

if t.TYPE_CHECKING:
    from jinja2 import Environment

    from sqlmesh.dbt.basemodel import Dependencies
    from sqlmesh.dbt.model import ModelConfig
    from sqlmesh.dbt.seed import SeedConfig
    from sqlmesh.dbt.source import SourceConfig


@dataclass
class DbtContext:
    """Context for DBT environment"""

    project_root: Path = Path()
    target_name: t.Optional[str] = None
    profile_name: t.Optional[str] = None
    project_schema: t.Optional[str] = None
    jinja_macros: JinjaMacroRegistry = field(
        default_factory=lambda: JinjaMacroRegistry(
            create_builtins_module=SQLMESH_DBT_PACKAGE, top_level_packages=["dbt"]
        )
    )

    sqlmesh_config: SQLMeshConfig = field(default_factory=SQLMeshConfig)

    _project_name: t.Optional[str] = None
    _variables: t.Dict[str, t.Any] = field(default_factory=dict)
    _models: t.Dict[str, ModelConfig] = field(default_factory=dict)
    _seeds: t.Dict[str, SeedConfig] = field(default_factory=dict)
    _sources: t.Dict[str, SourceConfig] = field(default_factory=dict)
    _refs: t.Dict[str, t.Union[ModelConfig, SeedConfig]] = field(default_factory=dict)

    _target: t.Optional[TargetConfig] = None

    _jinja_environment: t.Optional[Environment] = None

    _manifest: t.Optional[ManifestHelper] = None

    @property
    def dialect(self) -> str:
        if not self.target:
            raise SQLMeshError("Target must be configured before calling the dialect property.")
        return self.target.type

    @property
    def project_name(self) -> t.Optional[str]:
        return self._project_name

    @project_name.setter
    def project_name(self, project_name: str) -> None:
        self._project_name = project_name
        self.jinja_macros.root_package_name = project_name

    @property
    def manifest(self) -> ManifestHelper:
        if self._manifest is None:
            raise SQLMeshError("Manifest is not set in the context.")
        return self._manifest

    @manifest.setter
    def manifest(self, mainfest: ManifestHelper) -> None:
        self._manifest = mainfest

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

    def set_and_render_variables(self, variables: t.Dict[str, t.Any], package: str) -> None:
        self.variables = variables

        jinja_registry = JinjaMacroRegistry(
            create_builtins_module=SQLMESH_DBT_PACKAGE, top_level_packages=["dbt", package]
        )
        jinja_environment = jinja_registry.build_environment(**self.jinja_globals)

        def _render_var(value: t.Any) -> t.Any:
            if isinstance(value, str):
                return jinja_environment.from_string(value).render()
            if isinstance(value, list):
                return [_render_var(v) for v in value]
            if isinstance(value, dict):
                return {k: _render_var(v) for k, v in value.items()}
            return value

        def _var(name: str, default: t.Optional[t.Any] = None) -> t.Any:
            return _render_var(variables.get(name, default))

        jinja_environment.globals["var"] = _var

        rendered_variables = {}
        for k, v in variables.items():
            try:
                rendered_variables[k] = _render_var(v)
            except Exception as ex:
                raise ConfigError(f"Failed to render variable '{k}', value '{v}': {ex}") from ex

        self.variables = rendered_variables

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
    def refs(self) -> t.Dict[str, t.Union[ModelConfig, SeedConfig]]:
        from sqlmesh.dbt.model import ModelConfig
        from sqlmesh.dbt.seed import SeedConfig

        if not self._refs:
            # Refs can be called with or without package name.
            for model in t.cast(
                t.Dict[str, t.Union[ModelConfig, SeedConfig]], {**self._seeds, **self._models}
            ).values():
                self._refs[model.name] = model
                self._refs[model.config_name] = model
        return self._refs

    @property
    def target(self) -> t.Optional[TargetConfig]:
        return self._target

    @target.setter
    def target(self, value: TargetConfig) -> None:
        if not self.project_name:
            raise ConfigError("Project name must be set in the context in order to use a target.")

        self._target = value
        self._jinja_environment = None

    def render(self, source: str, **kwargs: t.Any) -> str:
        return self.jinja_environment.from_string(source).render(**kwargs)

    def copy(self) -> DbtContext:
        return replace(self)

    @property
    def jinja_environment(self) -> Environment:
        if self._jinja_environment is None:
            self._jinja_environment = self.jinja_macros.build_environment(**self.jinja_globals)
        return self._jinja_environment

    @property
    def jinja_globals(self) -> t.Dict[str, JinjaGlobalAttribute]:
        output: t.Dict[str, JinjaGlobalAttribute] = {
            "vars": AttributeDict(self.variables),
            "refs": AttributeDict({k: v.relation_info for k, v in self.refs.items()}),
            "sources": AttributeDict({k: v.relation_info for k, v in self.sources.items()}),
        }
        if self.project_name is not None:
            output["project_name"] = self.project_name
        if self._target is not None:
            output["target"] = self._target.attribute_dict()
        return output

    def context_for_dependencies(self, dependencies: Dependencies) -> DbtContext:
        from sqlmesh.dbt.model import ModelConfig
        from sqlmesh.dbt.seed import SeedConfig

        dependency_context = self.copy()

        models = {}
        seeds = {}
        sources = {}

        for ref in dependencies.refs:
            model = self.refs.get(ref)
            if model:
                if isinstance(model, SeedConfig):
                    seeds[ref] = t.cast(SeedConfig, model)
                else:
                    models[ref] = t.cast(ModelConfig, model)
            else:
                raise ConfigError(f"Model '{ref}' was not found.")

        for source in dependencies.sources:
            if source in self.sources:
                sources[source] = self.sources[source]
            else:
                raise ConfigError(f"Source '{source}' was not found.")

        dependency_context.sources = sources
        dependency_context.seeds = seeds
        dependency_context.models = models
        dependency_context._refs = {**dependency_context._seeds, **dependency_context._models}  # type: ignore

        return dependency_context


SQLMESH_DBT_PACKAGE = "sqlmesh.dbt"
