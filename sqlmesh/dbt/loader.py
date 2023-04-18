from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core import constants as c
from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config
from sqlmesh.core.hooks import HookRegistry
from sqlmesh.core.loader import Loader
from sqlmesh.core.macros import MacroRegistry
from sqlmesh.core.model import Model, ModelCache
from sqlmesh.dbt.basemodel import BaseModelConfig
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.cache import FileCache


def sqlmesh_config(project_root: t.Optional[Path] = None, **kwargs: t.Any) -> Config:
    project_root = project_root or Path()
    context = DbtContext(project_root=project_root)
    profile = Profile.load(context)

    return Config(
        default_connection=profile.target_name,
        connections={profile.target_name: profile.target.to_sqlmesh()},
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

        macros_max_mtime: t.Optional[float] = None

        for package_name, package in project.packages.items():
            context.jinja_macros.add_macros(
                package.macro_infos,
                package=package_name if package_name != context.project_name else None,
            )
            macro_files_mtime = [
                self._path_mtimes[m.path]
                for m in package.macros.values()
                if m.path in self._path_mtimes
            ]
            if macros_max_mtime is not None:
                macros_max_mtime = max(macro_files_mtime + [macros_max_mtime])
            else:
                macros_max_mtime = max(macro_files_mtime)

        cache_path = self._context.path / c.CACHE / context.target.name
        model_cache = ModelCache(cache_path)
        model_config_cache = FileCache(cache_path, ModelConfig, prefix="model_config")
        seed_config_cache = FileCache(cache_path, SeedConfig, prefix="seed_config")

        yaml_max_mtimes = self._compute_yaml_max_mtime_per_subfolder(self._context.path)

        def _cache_entry_id(target_path: Path) -> str:
            max_mtime = self._max_mtime_for_path(
                target_path, project, yaml_max_mtimes, macros_max_mtime
            )
            return str(int(max_mtime)) if max_mtime is not None else "na"

        # First render all the config and discover dependencies
        for package in project.packages.values():
            context.variables = package.variables

            package.sources = {k: v.render_config(context) for k, v in package.sources.items()}
            package.seeds = {
                k: seed_config_cache.get_or_load(
                    k, _cache_entry_id(v.path), lambda: v.render_config(context)
                )
                for k, v in package.seeds.items()
            }
            package.models = {
                k: model_config_cache.get_or_load(
                    k, _cache_entry_id(v.path), lambda: v.render_config(context)
                )
                for k, v in package.models.items()
            }

            context.add_sources(package.sources)
            context.add_seeds(package.seeds)
            context.add_models(package.models)

        # Now that config is rendered, create the sqlmesh models
        for package in project.packages.values():
            context.variables = package.variables
            package_models: t.Dict[str, BaseModelConfig] = {**package.models, **package.seeds}

            models.update(
                {
                    model.model_name: model_cache.get_or_load(
                        model.table_name,
                        _cache_entry_id(model.path),
                        lambda: model.to_sqlmesh(context),
                    )
                    for model in package_models.values()
                }
            )

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        return UniqueKeyDict("audits")

    def _max_mtime_for_path(
        self,
        target_path: Path,
        project: Project,
        yaml_max_mtimes: t.Dict[Path, float],
        macros_max_mtime: t.Optional[float],
    ) -> t.Optional[float]:
        project_root = project.context.project_root

        if not target_path.is_relative_to(project_root):
            return None

        mtimes = [
            self._path_mtimes.get(target_path),
            self._path_mtimes.get(project.profile.path),
            # FIXME: take into account which macros are actually referenced in the target model.
            macros_max_mtime,
        ]

        cursor = target_path
        while cursor != project_root:
            cursor = cursor.parent
            mtimes.append(yaml_max_mtimes.get(cursor))

        non_null_mtimes = [t for t in mtimes if t is not None]
        return max(non_null_mtimes) if non_null_mtimes else None

    def _compute_yaml_max_mtime_per_subfolder(self, root: Path) -> t.Dict[Path, float]:
        if not root.is_dir():
            return {}

        result = {}
        max_mtime: t.Optional[float] = None

        for nested in root.iterdir():
            if nested.is_dir():
                result.update(self._compute_yaml_max_mtime_per_subfolder(nested))
            elif nested.suffix.lower() in (".yaml", ".yml"):
                yaml_mtime = self._path_mtimes.get(nested)
                if yaml_mtime:
                    max_mtime = max(max_mtime, yaml_mtime) if max_mtime is not None else yaml_mtime

        if max_mtime is not None:
            result[root] = max_mtime

        return result
