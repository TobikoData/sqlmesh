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
from sqlmesh.dbt.common import Dependencies, ignore_macro
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import ProjectConfig
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.jinja import capture_jinja


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

        return (
            self._add_jinja_macros(
                macro.get_registry(),
                Path(self._context.path, "macros").glob("**/*.sql"),
            ),
            UniqueKeyDict("hooks"),
        )

    def _load_models(
        self,
        macros: MacroRegistry,
        hooks: HookRegistry,
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict = UniqueKeyDict("models")

        config = ProjectConfig.load(self._context.path, self._context.connection)
        for path in config.project_files:
            self._track_file(path)

        models.update(
            {seed.seed_name: seed.to_sqlmesh() for seed in config.seeds.values()}
        )

        models.update(
            {
                model.model_name: model.to_sqlmesh(
                    config.sources,
                    config.models,
                    config.seeds,
                    macros,
                    self._macro_dependencies,
                )
                for model in config.models.values()
            }
        )

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        return UniqueKeyDict("audits")

    def _added_jinja_macro(self, name: str, macro: str) -> None:
        dependencies = Dependencies()

        for method, args, kwargs in capture_jinja(macro).calls:
            if method == "ref":
                dep = ".".join(args + tuple(kwargs.values()))
                if dep:
                    dependencies.refs.add(dep)
            elif method == "source":
                source = ".".join(args + tuple(kwargs.values()))
                if source:
                    dependencies.sources.add(dep)
            elif not ignore_macro(name):
                dependencies.macros.add(method)

        self._macro_dependencies[name] = dependencies
