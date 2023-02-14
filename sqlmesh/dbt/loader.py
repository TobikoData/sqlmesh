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
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import Project
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

        models.update({seed.seed_name: seed.to_sqlmesh() for seed in config.seeds.values()})

        macro_configs = config.macros
        for name, macro in macros.items():
            macro_configs[name] = MacroConfig(macro=macro, dependencies=Dependencies())

        models.update(
            {
                model.model_name: model.to_sqlmesh(
                    config.sources,
                    config.models,
                    config.seeds,
                    config.variables,
                    macro_configs,
                )
                for model in config.models.values()
            }
        )

        return models

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
