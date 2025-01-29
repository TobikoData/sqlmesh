from __future__ import annotations

import abc
import glob
import linecache
import logging
import os
import typing as t
from collections import defaultdict
from concurrent.futures import as_completed
from dataclasses import dataclass
from pathlib import Path

from sqlglot.errors import SchemaError, SqlglotError
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core.audit import Audit, ModelAudit, StandaloneAudit, load_multiple_audits
from sqlmesh.core.dialect import parse
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.metric import Metric, MetricMeta, expand_metrics, load_metric_ddl
from sqlmesh.core.model import (
    Model,
    ExternalModel,
    ModelCache,
    OptimizedQueryCache,
    SeedModel,
    create_external_model,
    load_sql_based_model,
)
from sqlmesh.core.model.cache import load_optimized_query_and_mapping, optimized_query_cache_pool
from sqlmesh.core.model import model as model_registry
from sqlmesh.core.signal import signal
from sqlmesh.utils import UniqueKeyDict, sys_path
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroExtractor
from sqlmesh.utils.metaprogramming import import_python_file
from sqlmesh.utils.yaml import YAML

if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext


logger = logging.getLogger(__name__)


@dataclass
class LoadedProject:
    macros: MacroRegistry
    jinja_macros: JinjaMacroRegistry
    models: UniqueKeyDict[str, Model]
    standalone_audits: UniqueKeyDict[str, StandaloneAudit]
    audits: UniqueKeyDict[str, ModelAudit]
    metrics: UniqueKeyDict[str, Metric]
    requirements: t.Dict[str, str]
    excluded_requirements: t.Set[str]


class Loader(abc.ABC):
    """Abstract base class to load macros and models for a context"""

    def __init__(self, context: GenericContext, path: Path) -> None:
        self._path_mtimes: t.Dict[Path, float] = {}
        self.context = context
        self.config_path = path
        self.config = self.context.configs[self.config_path]

        gateway_name = self.context.selected_gateway
        try:
            gateway = self.config.get_gateway(gateway_name)
        except ConfigError:
            logger.warning(
                "Gateway '%s' not found in project '%s'", gateway_name, self.config.project
            )
            gateway = None
        self._variables = {
            **self.config.variables,
            **(gateway.variables if gateway else {}),
            c.GATEWAY: gateway_name,
        }

    def load(self) -> LoadedProject:
        """
        Loads all macros and models in the context's path.

        Returns:
            A loaded project object.
        """
        with sys_path(self.config_path):
            # python files are cached by the system
            # need to manually clear here so we can reload macros
            linecache.clearcache()
            self._path_mtimes.clear()

            self.load_materializations()
            signals = self._load_signals()

            config_mtimes: t.Dict[Path, t.List[float]] = defaultdict(list)

            for config_file in self.config_path.glob("config.*"):
                self._track_file(config_file)
                config_mtimes[self.config_path].append(self._path_mtimes[config_file])

            for config_file in c.SQLMESH_PATH.glob("config.*"):
                self._track_file(config_file)
                config_mtimes[c.SQLMESH_PATH].append(self._path_mtimes[config_file])

            self._config_mtimes = {path: max(mtimes) for path, mtimes in config_mtimes.items()}

            macros, jinja_macros = self._load_scripts()
            audits: UniqueKeyDict[str, ModelAudit] = UniqueKeyDict("audits")
            standalone_audits: UniqueKeyDict[str, StandaloneAudit] = UniqueKeyDict(
                "standalone_audits"
            )

            for name, audit in self._load_audits(macros=macros, jinja_macros=jinja_macros).items():
                if isinstance(audit, ModelAudit):
                    audits[name] = audit
                else:
                    standalone_audits[name] = audit

            models = self._load_models(
                macros,
                jinja_macros,
                self.context.selected_gateway,
                audits,
                signals,
            )

            metrics = self._load_metrics()

            requirements, excluded_requirements = self._load_requirements()

            project = LoadedProject(
                macros=macros,
                jinja_macros=jinja_macros,
                models=models,
                audits=audits,
                standalone_audits=standalone_audits,
                metrics=expand_metrics(metrics),
                requirements=requirements,
                excluded_requirements=excluded_requirements,
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
            not path.exists() or path.stat().st_mtime > initial_mtime
            for path, initial_mtime in self._path_mtimes.copy().items()
        )

    @abc.abstractmethod
    def _load_scripts(self) -> t.Tuple[MacroRegistry, JinjaMacroRegistry]:
        """Loads all user defined macros."""

    @abc.abstractmethod
    def _load_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        gateway: t.Optional[str],
        audits: UniqueKeyDict[str, ModelAudit],
        signals: UniqueKeyDict[str, signal],
    ) -> UniqueKeyDict[str, Model]:
        """Loads all models."""

    @abc.abstractmethod
    def _load_audits(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Audit]:
        """Loads all audits."""

    def load_materializations(self) -> None:
        """Loads custom materializations."""

    def _load_signals(self) -> UniqueKeyDict[str, signal]:
        return UniqueKeyDict("signals")

    def _load_metrics(self) -> UniqueKeyDict[str, MetricMeta]:
        return UniqueKeyDict("metrics")

    def _load_external_models(
        self,
        audits: UniqueKeyDict[str, ModelAudit],
        gateway: t.Optional[str] = None,
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        external_models_yaml = Path(self.config_path / c.EXTERNAL_MODELS_YAML)
        deprecated_yaml = Path(self.config_path / c.EXTERNAL_MODELS_DEPRECATED_YAML)
        external_models_path = self.config_path / c.EXTERNAL_MODELS

        paths_to_load = []
        if external_models_yaml.exists():
            paths_to_load.append(external_models_yaml)
        elif deprecated_yaml.exists():
            paths_to_load.append(deprecated_yaml)

        if external_models_path.exists() and external_models_path.is_dir():
            paths_to_load.extend(self._glob_paths(external_models_path, extension=".yaml"))

        for path in paths_to_load:
            self._track_file(path)

            with open(path, "r", encoding="utf-8") as file:
                external_models: t.List[ExternalModel] = []
                for row in YAML().load(file.read()):
                    model = create_external_model(
                        defaults=self.config.model_defaults.dict(),
                        path=path,
                        project=self.config.project,
                        audit_definitions=audits,
                        **{
                            "dialect": self.config.model_defaults.dialect,
                            "default_catalog": self.context.default_catalog,
                            **row,
                        },
                    )
                    external_models.append(model)

                # external models with no explicit gateway defined form the base set
                for model in (e for e in external_models if e.gateway is None):
                    models[model.fqn] = model

                # however, if there is a gateway defined, gateway-specific models take precedence
                if gateway:
                    for model in (e for e in external_models if e.gateway == gateway):
                        models.update({model.fqn: model})

        return models

    def _load_requirements(self) -> t.Tuple[t.Dict[str, str], t.Set[str]]:
        """Loads Python dependencies from the lock file.

        Returns:
            A tuple of requirements and excluded requirements.
        """
        requirements: t.Dict[str, str] = {}
        excluded_requirements: t.Set[str] = set()

        requirements_path = self.config_path / c.REQUIREMENTS
        if requirements_path.is_file():
            with open(requirements_path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if line.startswith("^"):
                        excluded_requirements.add(line[1:])
                        continue

                    args = [k.strip() for k in line.split("==")]
                    if len(args) != 2:
                        raise ConfigError(
                            f"Invalid lock file entry '{line.strip()}'. Only 'dep==ver' is supported"
                        )
                    dep, ver = args
                    other_ver = requirements.get(dep, ver)
                    if ver != other_ver:
                        raise ConfigError(
                            f"Conflicting requirement {dep}: {ver} != {other_ver}. Fix your {c.REQUIREMENTS} file."
                        )
                    requirements[dep] = ver

        return requirements, excluded_requirements

    def _glob_paths(
        self,
        path: Path,
        ignore_patterns: t.Optional[t.List[str]] = None,
        extension: t.Optional[str] = None,
    ) -> t.Generator[Path, None, None]:
        """
        Globs the provided path for the file extension but also removes any filepaths that match an ignore
        pattern either set in constants or provided in config

        Args:
            path: The filepath to glob
            ignore_patterns: A list of patterns for glob to ignore
            extension: The extension to check for in that path (checks recursively in zero or more subdirectories)

        Returns:
            Matched paths that are not ignored
        """
        ignore_patterns = ignore_patterns or []
        extension = extension or ""

        # We try to match both ignore_pattern itself and every file returned by glob,
        # so that we will always ignore file names that do not appear in the latter.
        ignored_filepaths = set(ignore_patterns) | {
            ignored_path
            for ignore_pattern in ignore_patterns
            for ignored_path in glob.glob(str(self.config_path / ignore_pattern), recursive=True)
        }
        for filepath in path.glob(f"**/*{extension}"):
            if any(filepath.match(ignored_filepath) for ignored_filepath in ignored_filepaths):
                continue

            yield filepath

    def _track_file(self, path: Path) -> None:
        """Project file to track for modifications"""
        self._path_mtimes[path] = path.stat().st_mtime


class SqlMeshLoader(Loader):
    """Loads macros and models for a context using the SQLMesh file formats"""

    def _load_scripts(self) -> t.Tuple[MacroRegistry, JinjaMacroRegistry]:
        """Loads all user defined macros."""
        # Store a copy of the macro registry
        standard_macros = macro.get_registry()
        jinja_macros = JinjaMacroRegistry()
        extractor = MacroExtractor()

        macros_max_mtime: t.Optional[float] = None

        for path in self._glob_paths(
            self.config_path / c.MACROS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".py",
        ):
            if import_python_file(path, self.config_path):
                self._track_file(path)
                macro_file_mtime = self._path_mtimes[path]
                macros_max_mtime = (
                    max(macros_max_mtime, macro_file_mtime)
                    if macros_max_mtime
                    else macro_file_mtime
                )

        for path in self._glob_paths(
            self.config_path / c.MACROS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".sql",
        ):
            self._track_file(path)
            macro_file_mtime = self._path_mtimes[path]
            macros_max_mtime = (
                max(macros_max_mtime, macro_file_mtime) if macros_max_mtime else macro_file_mtime
            )
            with open(path, "r", encoding="utf-8") as file:
                jinja_macros.add_macros(
                    extractor.extract(file.read(), dialect=self.config.model_defaults.dialect)
                )

        self._macros_max_mtime = macros_max_mtime

        macros = macro.get_registry()
        macro.set_registry(standard_macros)

        return macros, jinja_macros

    def _load_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        gateway: t.Optional[str],
        audits: UniqueKeyDict[str, ModelAudit],
        signals: UniqueKeyDict[str, signal],
    ) -> UniqueKeyDict[str, Model]:
        """
        Loads all of the models within the model directory with their associated
        audits into a Dict and creates the dag
        """
        models = self._load_sql_models(macros, jinja_macros, audits, signals)
        models.update(self._load_external_models(audits, gateway))
        models.update(self._load_python_models(macros, jinja_macros, audits, signals))

        return models

    def _load_sql_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        audits: UniqueKeyDict[str, ModelAudit],
        signals: UniqueKeyDict[str, signal],
    ) -> UniqueKeyDict[str, Model]:
        """Loads the sql models into a Dict"""
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        cache = SqlMeshLoader._Cache(self, self.config_path)

        for path in self._glob_paths(
            self.config_path / c.MODELS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".sql",
        ):
            if not os.path.getsize(path):
                continue

            self._track_file(path)

            def _load() -> Model:
                with open(path, "r", encoding="utf-8") as file:
                    try:
                        expressions = parse(
                            file.read(), default_dialect=self.config.model_defaults.dialect
                        )
                    except SqlglotError as ex:
                        raise ConfigError(f"Failed to parse a model definition at '{path}': {ex}.")

                return load_sql_based_model(
                    expressions,
                    defaults=self.config.model_defaults.dict(),
                    macros=macros,
                    jinja_macros=jinja_macros,
                    audit_definitions=audits,
                    default_audits=self.config.model_defaults.audits,
                    path=Path(path).absolute(),
                    module_path=self.config_path,
                    dialect=self.config.model_defaults.dialect,
                    time_column_format=self.config.time_column_format,
                    physical_schema_mapping=self.config.physical_schema_mapping,
                    project=self.config.project,
                    default_catalog=self.context.default_catalog,
                    variables=self._variables,
                    infer_names=self.config.model_naming.infer_names,
                    signal_definitions=signals,
                )

            model = cache.get_or_load_model(path, _load)
            if model.enabled:
                models[model.fqn] = model

            if isinstance(model, SeedModel):
                seed_path = model.seed_path
                self._track_file(seed_path)

        return models

    def _load_python_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        audits: UniqueKeyDict[str, ModelAudit],
        signals: UniqueKeyDict[str, signal],
    ) -> UniqueKeyDict[str, Model]:
        """Loads the python models into a Dict"""
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        registry = model_registry.registry()
        registry.clear()
        registered: t.Set[str] = set()

        model_registry._dialect = self.config.model_defaults.dialect
        try:
            for path in self._glob_paths(
                self.config_path / c.MODELS,
                ignore_patterns=self.config.ignore_patterns,
                extension=".py",
            ):
                if not os.path.getsize(path):
                    continue

                self._track_file(path)
                import_python_file(path, self.config_path)
                new = registry.keys() - registered
                registered |= new
                for name in new:
                    model = registry[name].model(
                        path=path,
                        module_path=self.config_path,
                        defaults=self.config.model_defaults.dict(),
                        macros=macros,
                        jinja_macros=jinja_macros,
                        dialect=self.config.model_defaults.dialect,
                        time_column_format=self.config.time_column_format,
                        physical_schema_mapping=self.config.physical_schema_mapping,
                        project=self.config.project,
                        default_catalog=self.context.default_catalog,
                        variables=self._variables,
                        infer_names=self.config.model_naming.infer_names,
                        audit_definitions=audits,
                    )
                    if model.enabled:
                        models[model.fqn] = model
        finally:
            model_registry._dialect = None

        return models

    def load_materializations(self) -> None:
        """Loads custom materializations."""
        for path in self._glob_paths(
            self.config_path / c.MATERIALIZATIONS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".py",
        ):
            if os.path.getsize(path):
                import_python_file(path, self.config_path)

    def _load_signals(self) -> UniqueKeyDict[str, signal]:
        """Loads signals for the built-in scheduler."""

        signals_max_mtime: t.Optional[float] = None

        for path in self._glob_paths(
            self.config_path / c.SIGNALS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".py",
        ):
            if os.path.getsize(path):
                self._track_file(path)
                signal_file_mtime = self._path_mtimes[path]
                signals_max_mtime = (
                    max(signals_max_mtime, signal_file_mtime)
                    if signals_max_mtime
                    else signal_file_mtime
                )
                import_python_file(path, self.config_path)

        self._signals_max_mtime = signals_max_mtime

        return signal.get_registry()

    def _load_audits(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Audit]:
        """Loads all the model audits."""
        audits_by_name: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        audits_max_mtime: t.Optional[float] = None

        for path in self._glob_paths(
            self.config_path / c.AUDITS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".sql",
        ):
            self._track_file(path)
            with open(path, "r", encoding="utf-8") as file:
                audits_file_mtime = self._path_mtimes[path]
                audits_max_mtime = (
                    max(audits_max_mtime, audits_file_mtime)
                    if audits_max_mtime
                    else audits_file_mtime
                )
                expressions = parse(file.read(), default_dialect=self.config.model_defaults.dialect)
                audits = load_multiple_audits(
                    expressions=expressions,
                    path=path,
                    module_path=self.config_path,
                    macros=macros,
                    jinja_macros=jinja_macros,
                    dialect=self.config.model_defaults.dialect,
                    default_catalog=self.context.default_catalog,
                    variables=self._variables,
                )
                for audit in audits:
                    audits_by_name[audit.name] = audit

        self._audits_max_mtime = audits_max_mtime

        return audits_by_name

    def _load_metrics(self) -> UniqueKeyDict[str, MetricMeta]:
        """Loads all metrics."""
        metrics: UniqueKeyDict[str, MetricMeta] = UniqueKeyDict("metrics")

        for path in self._glob_paths(
            self.config_path / c.METRICS,
            ignore_patterns=self.config.ignore_patterns,
            extension=".sql",
        ):
            if not os.path.getsize(path):
                continue
            self._track_file(path)

            with open(path, "r", encoding="utf-8") as file:
                dialect = self.config.model_defaults.dialect
                try:
                    for expression in parse(file.read(), default_dialect=dialect):
                        metric = load_metric_ddl(expression, path=path, dialect=dialect)
                        metrics[metric.name] = metric
                except SqlglotError as ex:
                    raise ConfigError(f"Failed to parse metric definitions at '{path}': {ex}.")

        return metrics

    class _Cache:
        def __init__(self, loader: SqlMeshLoader, config_path: Path):
            self._loader = loader
            self.config_path = config_path
            self._model_cache = ModelCache(self.config_path / c.CACHE)

        def get_or_load_model(self, target_path: Path, loader: t.Callable[[], Model]) -> Model:
            model = self._model_cache.get_or_load(
                self._cache_entry_name(target_path),
                self._model_cache_entry_id(target_path),
                loader=loader,
            )
            model._path = target_path
            return model

        def _cache_entry_name(self, target_path: Path) -> str:
            return "__".join(target_path.relative_to(self.config_path).parts).replace(
                target_path.suffix, ""
            )

        def _model_cache_entry_id(self, model_path: Path) -> str:
            mtimes = [
                self._loader._path_mtimes[model_path],
                self._loader._macros_max_mtime,
                self._loader._signals_max_mtime,
                self._loader._audits_max_mtime,
                self._loader._config_mtimes.get(self.config_path),
                self._loader._config_mtimes.get(c.SQLMESH_PATH),
            ]
            return "__".join(
                [
                    str(max(m for m in mtimes if m is not None)),
                    self._loader.config.fingerprint,
                    # default catalog can change outside sqlmesh (e.g., DB user's
                    # default catalog), and it is retained in cached model's fully
                    # qualified name
                    self._loader.context.default_catalog or "",
                    # gateway is configurable, and it is retained in a cached
                    # model's python environment if the @gateway macro variable is
                    # used in the model
                    self._loader.context.gateway or self._loader.config.default_gateway_name,
                ]
            )


# TODO: consider moving this to context
def update_model_schemas(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    config_path: Path,
) -> None:
    schema = MappingSchema(normalize=False)
    optimized_query_cache: OptimizedQueryCache = OptimizedQueryCache(config_path / c.CACHE)

    if c.MAX_FORK_WORKERS == 1:
        _update_model_schemas_sequential(dag, models, schema, optimized_query_cache)
    else:
        _update_model_schemas_parallel(dag, models, schema, optimized_query_cache)


def _update_schema_with_model(schema: MappingSchema, model: Model) -> None:
    columns_to_types = model.columns_to_types
    if columns_to_types:
        try:
            schema.add_table(model.fqn, columns_to_types, dialect=model.dialect)
        except SchemaError as e:
            if "nesting level:" in str(e):
                logger.error(
                    "SQLMesh requires all model names and references to have the same level of nesting."
                )
            raise


def _update_model_schemas_sequential(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    schema: MappingSchema,
    optimized_query_cache: OptimizedQueryCache,
) -> None:
    for name in dag.sorted:
        model = models.get(name)

        # External models don't exist in the context, so we need to skip them
        if not model:
            continue

        model.update_schema(schema)
        optimized_query_cache.with_optimized_query(model)
        _update_schema_with_model(schema, model)


def _update_model_schemas_parallel(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    schema: MappingSchema,
    optimized_query_cache: OptimizedQueryCache,
) -> None:
    futures = set()
    graph = {
        model: {dep for dep in deps if dep in models}
        for model, deps in dag._dag.items()
        if model in models
    }

    def process_models(completed_model: t.Optional[Model] = None) -> None:
        for name in list(graph):
            deps = graph[name]

            if completed_model:
                deps.discard(completed_model.fqn)

            if not deps:
                del graph[name]
                model = models[name]
                futures.add(
                    executor.submit(
                        load_optimized_query_and_mapping,
                        model,
                        mapping={
                            parent: models[parent].columns_to_types
                            for parent in model.depends_on
                            if parent in models
                        },
                    )
                )

    with optimized_query_cache_pool(optimized_query_cache) as executor:
        process_models()

        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                fqn, entry_name, data_hash, metadata_hash, mapping_schema = future.result()
                model = models[fqn]
                model._data_hash = data_hash
                model._metadata_hash = metadata_hash
                model.set_mapping_schema(mapping_schema)
                optimized_query_cache.with_optimized_query(model, entry_name)
                _update_schema_with_model(schema, model)
                process_models(completed_model=model)
