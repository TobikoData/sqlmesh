from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config
from sqlmesh.core.hooks import HookRegistry
from sqlmesh.core.loader import Loader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model
from sqlmesh.dbt.basemodel import BaseModelConfig
from sqlmesh.dbt.common import DbtContext
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import Project
from sqlmesh.utils import UniqueKeyDict


def sqlmesh_config(project_root: t.Optional[Path] = None, **kwargs: t.Any) -> Config:
    project_root = project_root or Path()
    context = DbtContext(project_root=project_root)
    profile = Profile.load(context)

    return Config(
        default_connection=profile.default_target,
        connections=profile.to_sqlmesh(),
        loader=DbtLoader,
        **kwargs,
    )


class DbtLoader(Loader):
    def _load_scripts(self) -> t.Tuple[MacroRegistry, HookRegistry]:
        macro_files = list(Path(self._context.path, "macros").glob("**/*.sql"))
        for file in macro_files:
            self._track_file(file)

        return (
            UniqueKeyDict("macros"),
            UniqueKeyDict("hooks"),
        )

    def _load_models(
        self,
        macros: MacroRegistry,
        hooks: HookRegistry,
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict = UniqueKeyDict("models")

        project = Project.load(
            DbtContext(project_root=self._context.path, target_name=self._context.connection)
        )
        for path in project.project_files:
            self._track_file(path)

        context = project.context.copy()

        for package_name, package in project.packages.items():
            context.add_variables(package.variables)
            context.add_models(package.models)
            context.add_seeds(package.seeds)
            context.add_sources(package.sources)
            context.jinja_macros.add_macros(
                package.macros,
                package=package_name if package_name != context.project_name else None,
            )

        for package in project.packages.values():
            package_models: t.Dict[str, BaseModelConfig] = {**package.models, **package.seeds}
            for model in package_models.values():
                rendered_model = model.render_config(context)
                models[rendered_model.model_name] = rendered_model.to_sqlmesh(context)

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        return UniqueKeyDict("audits")
