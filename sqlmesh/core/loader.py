from __future__ import annotations

import abc
import importlib
import itertools
import linecache
import os
import sys
import types
import typing as t
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from sqlglot.errors import SqlglotError
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core.audit import Audit
from sqlmesh.core.dialect import parse
from sqlmesh.core.hooks import HookRegistry, hook
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.model import Model, ModelCache, SeedModel, load_model
from sqlmesh.core.model import model as model_registry
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import ConfigError

if t.TYPE_CHECKING:
    from sqlmesh.core.config import Config
    from sqlmesh.core.context import Context


def update_model_schemas(dag: DAG[str], models: UniqueKeyDict[str, Model]) -> None:
    schema = MappingSchema()
    for name in dag.sorted():
        model = models.get(name)

        # External models don't exist in the context, so we need to skip them
        if not model:
            continue

        if model.contains_star_query and any(dep not in models for dep in model.depends_on):
            raise ConfigError(
                f"Can't expand SELECT * expression for model '{name}'. Projections for models that use external sources must be specified explicitly at '{model._path}'"
            )

        model.update_schema(schema)
        schema.add_table(name, model.columns_to_types)


@dataclass
class LoadedProject:
    macros: MacroRegistry
    hooks: HookRegistry
    models: UniqueKeyDict[str, Model]
    audits: UniqueKeyDict[str, Audit]
    dag: DAG[str]


class Loader(abc.ABC):
    """Abstract base class to load macros and models for a context"""

    def __init__(self) -> None:
        self._path_mtimes: t.Dict[Path, float] = {}
        self._dag: DAG[str] = DAG()

    def load(self, context: Context) -> LoadedProject:
        """
        Loads all hooks, macros, and models in the context's path

        Args:
            context: The context to load macros and models for
        """
        # python files are cached by the system
        # need to manually clear here so we can reload macros
        linecache.clearcache()

        self._context = context
        self._path_mtimes.clear()
        self._dag = DAG()

        config_mtimes: t.Dict[Path, t.List[float]] = defaultdict(list)
        for context_path, config in self._context.configs.items():
            for config_file in context_path.glob("config.*"):
                self._track_file(config_file)
                config_mtimes[context_path].append(self._path_mtimes[config_file])

        for config_file in context.sqlmesh_path.glob("config.*"):
            self._track_file(config_file)
            config_mtimes[context.sqlmesh_path].append(self._path_mtimes[config_file])

        self._config_mtimes = {path: max(mtimes) for path, mtimes in config_mtimes.items()}

        macros, hooks = self._load_scripts()
        models = self._load_models(macros, hooks)
        for model in models.values():
            self._add_model_to_dag(model)
        update_model_schemas(self._dag, models)

        audits = self._load_audits()

        project = LoadedProject(
            macros=macros, hooks=hooks, models=models, audits=audits, dag=self._dag
        )
        return project

    def reload_needed(self) -> bool:
        """
        Checks for any modifications to the files the macros and models depend on
        since the last load.

        Returns:
            True if a modification is found; False otherwise
        """
        return any(
            path.stat().st_mtime > initial_mtime
            for path, initial_mtime in self._path_mtimes.items()
        )

    @abc.abstractmethod
    def _load_scripts(self) -> t.Tuple[MacroRegistry, HookRegistry]:
        """Loads all user defined hooks and macros."""

    @abc.abstractmethod
    def _load_models(self, macros: MacroRegistry, hooks: HookRegistry) -> UniqueKeyDict[str, Model]:
        """Loads all models."""

    @abc.abstractmethod
    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        """Loads all audits."""

    def _add_model_to_dag(self, model: Model) -> None:
        self._dag.graph[model.name] = set()
        self._dag.add(model.name, model.depends_on)

    def _track_file(self, path: Path) -> None:
        """Project file to track for modifications"""
        self._path_mtimes[path] = path.stat().st_mtime


class SqlMeshLoader(Loader):
    """Loads macros and models for a context using the SQLMesh file formats"""

    def _load_scripts(self) -> t.Tuple[MacroRegistry, HookRegistry]:
        """Loads all user defined hooks and macros."""
        # Store a copy of the macro registry
        standard_hooks = hook.get_registry()
        standard_macros = macro.get_registry()

        macros_max_mtime: t.Optional[float] = None

        for context_path, config in self._context.configs.items():
            for path in itertools.chain(
                self._glob_paths(context_path / c.MACROS, config=config, extension=".py"),
                self._glob_paths(context_path / c.HOOKS, config=config, extension=".py"),
            ):
                if self._import_python_file(path, context_path):
                    self._track_file(path)
                    macro_file_mtime = self._path_mtimes[path]
                    macros_max_mtime = (
                        max(macros_max_mtime, macro_file_mtime)
                        if macros_max_mtime
                        else macro_file_mtime
                    )

        self._macros_max_mtime = macros_max_mtime

        hooks = hook.get_registry()
        macros = macro.get_registry()

        hook.set_registry(standard_hooks)
        macro.set_registry(standard_macros)

        return macros, hooks

    def _load_models(self, macros: MacroRegistry, hooks: HookRegistry) -> UniqueKeyDict[str, Model]:
        """
        Loads all of the models within the model directory with their associated
        audits into a Dict and creates the dag
        """
        models = self._load_sql_models(macros, hooks)
        models.update(self._load_python_models())

        return models

    def _load_sql_models(
        self, macros: MacroRegistry, hooks: HookRegistry
    ) -> UniqueKeyDict[str, Model]:
        """Loads the sql models into a Dict"""
        models: UniqueKeyDict = UniqueKeyDict("models")
        for context_path, config in self._context.configs.items():
            cache = SqlMeshLoader._Cache(self, context_path)

            for path in self._glob_paths(context_path / c.MODELS, config=config, extension=".sql"):
                if not os.path.getsize(path):
                    continue

                self._track_file(path)

                def _load() -> Model:
                    with open(path, "r", encoding="utf-8") as file:
                        try:
                            expressions = parse(
                                file.read(), default_dialect=config.model_defaults.dialect
                            )
                        except SqlglotError as ex:
                            raise ConfigError(
                                f"Failed to parse a model definition at '{path}': {ex}."
                            )
                    return load_model(
                        expressions,
                        defaults=config.model_defaults.dict(),
                        macros=macros,
                        hooks=hooks,
                        path=Path(path).absolute(),
                        module_path=context_path,
                        dialect=config.model_defaults.dialect,
                        time_column_format=config.time_column_format,
                    )

                model = cache.get_or_load_model(path, _load)
                models[model.name] = model

                if isinstance(model, SeedModel):
                    seed_path = model.seed_path
                    self._track_file(seed_path)

        return models

    def _load_python_models(self) -> UniqueKeyDict[str, Model]:
        """Loads the python models into a Dict"""
        models: UniqueKeyDict = UniqueKeyDict("models")
        registry = model_registry.registry()
        registry.clear()
        registered: t.Set[str] = set()

        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(context_path / c.MODELS, config=config, extension=".py"):
                if not os.path.getsize(path):
                    continue

                self._track_file(path)
                self._import_python_file(path, context_path)
                new = registry.keys() - registered
                registered |= new
                for name in new:
                    model = registry[name].model(
                        path=path,
                        module_path=context_path,
                        defaults=config.model_defaults.dict(),
                        time_column_format=config.time_column_format,
                    )
                    models[model.name] = model

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        """Loads all the model audits."""
        audits_by_name: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(context_path / c.AUDITS, config=config, extension=".sql"):
                self._track_file(path)
                with open(path, "r", encoding="utf-8") as file:
                    expressions = parse(file.read(), default_dialect=config.model_defaults.dialect)
                    audits = Audit.load_multiple(
                        expressions=expressions,
                        path=path,
                        dialect=config.model_defaults.dialect,
                    )
                    for audit in audits:
                        audits_by_name[audit.name] = audit
        return audits_by_name

    def _import_python_file(self, file: Path, context_path: Path) -> types.ModuleType:
        relative_path = file.relative_to(context_path)
        module_name = str(relative_path.with_suffix("")).replace(os.path.sep, ".")
        # remove the entire module hierarchy in case they were already loaded
        parts = module_name.split(".")
        for i in range(len(parts)):
            sys.modules.pop(".".join(parts[0 : i + 1]), None)

        return importlib.import_module(module_name)

    def _glob_paths(
        self, path: Path, config: Config, extension: str
    ) -> t.Generator[Path, None, None]:
        """
        Globs the provided path for the file extension but also removes any filepaths that match an ignore
        pattern either set in constants or provided in config

        Args:
            path: The filepath to glob
            extension: The extension to check for in that path (checks recursively in zero or more subdirectories)

        Returns:
            Matched paths that are not ignored
        """
        for filepath in path.glob(f"**/*{extension}"):
            for ignore_pattern in config.ignore_patterns:
                if filepath.match(ignore_pattern):
                    break
            else:
                yield filepath

    class _Cache:
        def __init__(self, loader: SqlMeshLoader, context_path: Path):
            self._loader = loader
            self._context_path = context_path

            self._model_cache = ModelCache(loader._context.path / c.CACHE)

        def get_or_load_model(self, target_path: Path, loader: t.Callable[[], Model]) -> Model:
            model = self._model_cache.get_or_load(
                self._cache_entry_name(target_path), self._model_cache_entry_id(target_path), loader
            )
            model._path = target_path
            return model

        def _cache_entry_name(self, target_path: Path) -> str:
            return "__".join(target_path.relative_to(self._context_path).parts).replace(
                target_path.suffix, ""
            )

        def _model_cache_entry_id(self, model_path: Path) -> str:
            mtimes = [
                self._loader._path_mtimes[model_path],
                self._loader._macros_max_mtime,
                self._loader._config_mtimes.get(self._context_path),
                self._loader._config_mtimes.get(self._loader._context.sqlmesh_path),
            ]
            return str(int(max(m for m in mtimes if m is not None)))
