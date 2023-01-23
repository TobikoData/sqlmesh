from __future__ import annotations

import abc
import importlib
import os
import sys
import types
import typing as t
from pathlib import Path

from sqlglot.errors import SqlglotError
from sqlglot.schema import MappingSchema

from sqlmesh.core.audit import Audit
from sqlmesh.core.dialect import parse_model
from sqlmesh.core.hooks import hook
from sqlmesh.core.macros import macro
from sqlmesh.core.model import Model, SeedModel, load_model
from sqlmesh.core.model import model as model_registry
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import ConfigError, SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context


def update_model_schemas(dialect: str, dag: DAG, models: UniqueKeyDict) -> None:
    schema = MappingSchema(dialect=dialect)
    for name in dag.sorted():
        model = models.get(name)

        # External models don't exist in the context, so we need to skip them
        if not model:
            continue

        if model.contains_star_query and any(
            dep not in models for dep in model.depends_on
        ):
            raise SQLMeshError(
                f"Can't expand SELECT * expression for model {name}. Projections for models that use external sources must be specified explicitly"
            )

        model.update_schema(schema)
        schema.add_table(name, model.columns_to_types)


class Loader(abc.ABC):
    """Abstract base class to load macros and models for a context"""

    def __init__(self) -> None:
        self._path_mtimes: t.Dict[Path, float] = {}
        self._dag: DAG[str] = DAG()

    @abc.abstractmethod
    def load(self, context: Context) -> None:
        """
        Loads all hooks, macros, and models in the context's path

        Args:
            context: The context to load macros and models for
        """
        self._context = context
        self._path_mtimes.clear()
        self._dag = DAG()

        macros = self._load_macros()
        models = self._load_models(macros)
        for model in models.values():
            self._add_model_to_dag(model)
        update_model_schemas(self._context.dialect, self._dag, models)

        return (macros, models, self._dag)

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
    def _load_macros(self) -> UniqueKeyDict:
        """"""

    @abc.abstractmethod
    def _load_models(self, macros: UniqueKeyDict) -> UniqueKeyDict:
        """"""

    def _add_model_to_dag(self, model: Model) -> None:
        self._dag.graph[model.name] = set()
        self._dag.add(model.name, model.depends_on)


class SqlMeshLoader(Loader):
    """Loads macros and models for a context using the SQLMesh file formats"""

    def load(self, context: c.Context) -> None:
        self._path_mtimes.clear()
        self._dag = DAG()
        hooks, macros = self._load_scripts()
        context._hooks = hooks
        context._macros = macros
        context._models = self._load_models(macros, hooks)
        context.dag = self._dag

    def _load_scripts(
        self, context: c.Context
    ) -> t.Tuple[UniqueKeyDict, UniqueKeyDict]:
        """Loads all user defined hooks and macros."""
        # Store a copy of the macro registry
        standard_hooks = hook.get_registry()
        standard_macros = macro.get_registry()

        for path in tuple(
            context.glob_path(context.macro_directory_path, ".py")
        ) + tuple(context.glob_path(context.hook_directory_path, ".py")):
            if self._import_python_file(path.relative_to(context.path)):
                self._path_mtimes[path] = path.stat().st_mtime

        hooks = hook.get_registry()
        macros = macro.get_registry()

        hook.set_registry(standard_hooks)
        macro.set_registry(standard_macros)

        return hooks, macros

    def _load_models(
        self, macros: UniqueKeyDict, hooks: UniqueKeyDict
    ) -> UniqueKeyDict:
        """
        Loads all of the models within the model directory with their associated
        audits into a Dict and creates the dag
        """
        models = self._load_sql_models(macros, hooks)
        models.update(self._load_python_models())
        self._load_model_audits(models)

        return models

    def _load_sql_models(self, macros: UniqueKeyDict, hooks: UniqueKeyDict) -> UniqueKeyDict:
        """Loads the sql models into a Dict"""
        models: UniqueKeyDict = UniqueKeyDict("models")
        for path in self._context.glob_path(
            self._context.models_directory_path, ".sql"
        ):
            self._path_mtimes[path] = path.stat().st_mtime
            with open(path, "r", encoding="utf-8") as file:
                try:
                    expressions = parse_model(
                        file.read(), default_dialect=self._context.dialect
                    )
                except SqlglotError as ex:
                    raise ConfigError(
                        f"Failed to parse a model definition at '{path}': {ex}"
                    )
                model = load_model(
                    expressions,
                    defaults=context.config.model_defaults.dict(),
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
                    self._path_mtimes[seed_path] = seed_path.stat().st_mtime

        return models

    def _load_python_models(self) -> UniqueKeyDict:
        """Loads the python models into a Dict"""
        models: UniqueKeyDict = UniqueKeyDict("models")
        registry = model_registry.registry()
        registry.clear()
        registered: t.Set[str] = set()

        for path in self._context.glob_path(self._context.models_directory_path, ".py"):
            self._path_mtimes[path] = path.stat().st_mtime
            if self._import_python_file(path.relative_to(self._context.path)):
                self._path_mtimes[path] = path.stat().st_mtime
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

    def _load_model_audits(self, models: UniqueKeyDict) -> None:
        """Loads all the model audits and adds them to the associated model"""
        for path in self._context.glob_path(
            self._context.audits_directory_path, ".sql"
        ):
            self._path_mtimes[path] = path.stat().st_mtime
            with open(path, "r", encoding="utf-8") as file:
                expressions = parse_model(
                    file.read(), default_dialect=self._context.dialect
                )
                for audit in Audit.load_multiple(
                    expressions=expressions,
                    path=path,
                    dialect=self._context.dialect,
                ):
                    if not audit.skip:
                        if audit.model not in models:
                            raise ConfigError(
                                f"Model '{audit.model}' referenced in the audit '{audit.name}' ({path}) was not found"
                            )
                        models[audit.model].audits[audit.name] = audit

    def _import_python_file(self, relative_path: Path) -> types.ModuleType:
        module_name = str(relative_path.with_suffix("")).replace(os.path.sep, ".")
        # remove the entire module hierarchy in case they were already loaded
        parts = module_name.split(".")
        for i in range(len(parts)):
            sys.modules.pop(".".join(parts[0 : i + 1]), None)

        return importlib.import_module(module_name)
