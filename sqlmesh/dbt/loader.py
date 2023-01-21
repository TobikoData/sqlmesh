from __future__ import annotations

import typing as t

from sqlmesh.core.loader import Loader
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.dbt.project import ProjectConfig

class DbtLoader(Loader):
    def _load_macros(self) -> UniqueKeyDict:
        return UniqueKeyDict("macros")
    
    def _load_models(self, macros: UniqueKeyDict) -> UniqueKeyDict:
        models: UniqueKeyDict = UniqueKeyDict("models")

        config = ProjectConfig.load(self._context.path, self.connection)
        self._path_mtimes = {path: path.stat().st_mtime for path in config.project_files}

        models.extend(seed.to_sqlmesh() for seed in config.seeds.values())

        source_mapping = config.source_mapping
        model_mapping = config.model_mapping
        models.extend(model.to_sqlmesh(source_mapping, model_mapping) for model in config.models.values())

        return models
