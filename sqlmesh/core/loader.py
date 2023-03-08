from __future__ import annotations

import abc
import importlib
import linecache
import os
import sys
import types
import typing as t
from dataclasses import dataclass
from pathlib import Path

from sqlglot.errors import SqlglotError
from sqlglot.schema import MappingSchema

from sqlmesh.core.audit import Audit
from sqlmesh.core.dialect import parse
from sqlmesh.core.hooks import HookRegistry, hook
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.model import Model, SeedModel, load_model
from sqlmesh.core.model import model as model_registry
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import ConfigError

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context


def update_model_schemas(dialect: str, dag: DAG[str], models: UniqueKeyDict[str, Model]) -> None:
    schema = MappingSchema(dialect=dialect)
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

        macros, hooks = self._load_scripts()
        models = self._load_models(macros, hooks)
        for model in models.values():
            self._add_model_to_dag(model)
        update_model_schemas(self._context.dialect, self._dag, models)

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

        for path in tuple(self._glob_path(self._context.macro_directory_path, ".py")) + tuple(
            self._glob_path(self._context.hook_directory_path, ".py")
        ):
            if self._import_python_file(path.relative_to(self._context.path)):
                self._track_file(path)

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
        for path in self._glob_path(self._context.models_directory_path, ".sql"):
            self._track_file(path)
            with open(path, "r", encoding="utf-8") as file:
                try:
                    expressions = parse(file.read(), default_dialect=self._context.dialect)
                except SqlglotError as ex:
                    raise ConfigError(f"Failed to parse a model definition at '{path}': {ex}")
                model = load_model(
                    expressions,
                    defaults=self._context.config.model_defaults.dict(),
                    macros=macros,
                    hooks=hooks,
                    path=Path(path).absolute(),
                    module_path=self._context.path,
                    dialect=self._context.dialect,
                    time_column_format=self._context.config.time_column_format,
                )
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

        for path in self._glob_path(self._context.models_directory_path, ".py"):
            self._track_file(path)
            self._import_python_file(path.relative_to(self._context.path))
            new = registry.keys() - registered
            registered |= new
            for name in new:
                model = registry[name].model(
                    path=path,
                    module_path=self._context.path,
                    defaults=self._context.config.model_defaults.dict(),
                    time_column_format=self._context.config.time_column_format,
                )
                models[model.name] = model

        return models

    def _load_audits(self) -> UniqueKeyDict[str, Audit]:
        """Loads all the model audits."""
        audits_by_name: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        for path in self._glob_path(self._context.audits_directory_path, ".sql"):
            self._track_file(path)
            with open(path, "r", encoding="utf-8") as file:
                expressions = parse(file.read(), default_dialect=self._context.dialect)
                audits = Audit.load_multiple(
                    expressions=expressions,
                    path=path,
                    dialect=self._context.dialect,
                )
                for audit in audits:
                    audits_by_name[audit.name] = audit
        return audits_by_name

    def _import_python_file(self, relative_path: Path) -> types.ModuleType:
        module_name = str(relative_path.with_suffix("")).replace(os.path.sep, ".")
        # remove the entire module hierarchy in case they were already loaded
        parts = module_name.split(".")
        for i in range(len(parts)):
            sys.modules.pop(".".join(parts[0 : i + 1]), None)

        return importlib.import_module(module_name)

    def _glob_path(self, path: Path, file_extension: str) -> t.Generator[Path, None, None]:
        """
        Globs the provided path for the file extension but also removes any filepaths that match an ignore
        pattern either set in constants or provided in config

        Args:
            path: The filepath to glob
            file_extension: The extension to check for in that path (checks recursively in zero or more subdirectories)

        Returns:
            Matched paths that are not ignored
        """
        for filepath in path.glob(f"**/*{file_extension}"):
            for ignore_pattern in self._context.ignore_patterns:
                if filepath.match(ignore_pattern):
                    break
            else:
                yield filepath
