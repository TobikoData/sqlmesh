from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config
from sqlmesh.core.context import Context
from sqlmesh.core.hooks import HookRegistry
from sqlmesh.core.loader import LoadedProject, Loader
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.model import Model
from sqlmesh.dbt.common import Dependencies
from sqlmesh.dbt.macros import MacroConfig, builtin_methods
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.package import Package
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import MacroInfo


def sqlmesh_config(project_root: t.Optional[Path] = None) -> Config:
    project_root = project_root or Path()
    profile = Profile.load(project_root)

    return Config(
        default_connection=profile.default_target,
        connections=profile.to_sqlmesh(),
        loader=DbtLoader,
    )


class DbtLoader(Loader):
    def load(self, context: Context) -> LoadedProject:
        self._macro_dependencies: t.Dict[str, Dependencies] = {}
        return super().load(context)

    def _load_scripts(self) -> t.Tuple[MacroRegistry, HookRegistry]:
        macro_files = list(Path(self._context.path, "macros").glob("**/*.sql"))
        for file in macro_files:
            self._track_file(file)

        registry = macro.get_registry()
        registry.update(builtin_methods())

        return (
            registry,
            UniqueKeyDict("hooks"),
        )

    def _load_models(
        self,
        macros: MacroRegistry,
        hooks: HookRegistry,
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict = UniqueKeyDict("models")

        config = Project.load(self._context.path, self._context.connection)
        for path in config.project_files:
            self._track_file(path)

        all_sources: UniqueKeyDict[str, SourceConfig] = UniqueKeyDict("sources")
        all_seeds: UniqueKeyDict[str, SeedConfig] = UniqueKeyDict("seeds")
        all_models: UniqueKeyDict[str, ModelConfig] = UniqueKeyDict("models")
        for package in config.packages.values():
            all_sources.update(package.sources)
            all_seeds.update(package.seeds)
            all_models.update(package.models)

        for name, package in config.packages.items():
            all_macros = self._macros_for_package(name, config.packages)
            for name, macro in macros.items():
                all_macros[name] = MacroConfig(macro=macro, dependencies=Dependencies())

            for model in package.models.values():
                models[model.model_name] = model.to_sqlmesh(
                    all_sources, all_models, all_seeds, all_variables, all_macros
                )

            for seed in package.seeds.values():
                models[seed.seed_name] = seed.to_sqlmesh()

        return models

    def _macros_for_package(
        self, name: str, packages: t.Dict[str, Package]
    ) -> UniqueKeyDict[str, MacroConfig]:
        """
        Macros are namespaced by the package they reside in. Do not include
        the namespace for the macros in the package being processed
        """
        macros: UniqueKeyDict[str, MacroConfig] = UniqueKeyDict("macros")
        for package_name, package in packages.items():
            if package_name == name:
                macros.update(package.macros)
                continue

            for macro_name, macro in package.macros.items():
                macros[f"{package_name}.{macro_name}"] = macro

        return macros

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        return UniqueKeyDict("audits")

    def _on_jinja_macro_added(self, name: str, macro: MacroInfo) -> None:
        if not macro.calls:
            return

        dependencies = Dependencies()

        for method, args, kwargs in macro.calls:
            if method == "ref":
                dep = ".".join(args + tuple(kwargs.values()))
                if dep:
                    dependencies.refs.add(dep)
            elif method == "source":
                source = ".".join(args + tuple(kwargs.values()))
                if source:
                    dependencies.sources.add(dep)
            elif method == "var":
                if args:
                    # We map the var key to True if and only if it includes a default value
                    dependencies.variables[args[0]] = len(args) > 1
            else:
                dependencies.macros.add(method)

        self._macro_dependencies[name] = dependencies
