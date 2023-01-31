from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config
from sqlmesh.core.hooks import hook
from sqlmesh.core.loader import Loader
from sqlmesh.core.macros import macro
from sqlmesh.core.model import Model
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import ProjectConfig
from sqlmesh.utils import UniqueKeyDict


def sqlmesh_config(project_root: t.Optional[Path] = None) -> Config:
    project_root = project_root or Path()
    profile = Profile.load(project_root)

    return Config(
        default_connection=profile.default_target,
        connections=profile.to_sqlmesh(),
        loader=DbtLoader,
    )


class DbtLoader(Loader):
    def _load_scripts(
        self,
    ) -> t.Tuple[UniqueKeyDict[str, hook], UniqueKeyDict[str, macro]]:
        return (UniqueKeyDict("macros"), UniqueKeyDict("hooks"))

    def _load_models(
        self, macros: UniqueKeyDict[str, macro], hooks: UniqueKeyDict[str, hook]
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict = UniqueKeyDict("models")

        config = ProjectConfig.load(self._context.path, self._context.connection)
        self._path_mtimes = {
            path: path.stat().st_mtime for path in config.project_files
        }

        models.update(
            {seed.seed_name: seed.to_sqlmesh() for seed in config.seeds.values()}
        )

        models.update(
            {
                model.model_name: model.to_sqlmesh(
                    config.sources, config.models, config.seeds
                )
                for model in config.models.values()
            }
        )

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        return UniqueKeyDict("audits")
