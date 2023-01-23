from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.config import Config
from sqlmesh.core.loader import Loader
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import ProjectConfig
from sqlmesh.utils import UniqueKeyDict


def sqlmesh_config(project_root: t.Optional[Path] = None) -> Config:
    project_root = project_root or Path()

    return Config(
        connections=Profile.load(project_root).to_sqlmesh(),
        loader=DbtLoader(),
    )


class DbtLoader(Loader):
    def _load_macros(self) -> UniqueKeyDict:
        return UniqueKeyDict("macros")

    def _load_models(self, macros: UniqueKeyDict) -> UniqueKeyDict:
        models: UniqueKeyDict = UniqueKeyDict("models")

        config = ProjectConfig.load(self._context.path, self._context.connection)
        self._path_mtimes = {
            path: path.stat().st_mtime for path in config.project_files
        }

        models.update(
            {seed.seed_name: seed.to_sqlmesh() for seed in config.seeds.values()}
        )

        source_mapping = config.source_mapping
        model_mapping = config.model_mapping
        models.update(
            {
                model.model_name: model.to_sqlmesh(source_mapping, model_mapping)
                for model in config.models.values()
            }
        )

        return models
