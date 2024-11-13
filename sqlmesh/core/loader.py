from __future__ import annotations

import abc
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
from sqlmesh.core.audit import Audit, ModelAudit, load_multiple_audits
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
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroExtractor
from sqlmesh.utils.metaprogramming import import_python_file
from sqlmesh.utils.yaml import YAML

if t.TYPE_CHECKING:
    from sqlmesh.core.config import Config
    from sqlmesh.core.context import GenericContext


logger = logging.getLogger(__name__)


@dataclass
class LoadedProject:
    macros: MacroRegistry
    jinja_macros: JinjaMacroRegistry
    models: UniqueKeyDict[str, Model]
    audits: UniqueKeyDict[str, Audit]
    metrics: UniqueKeyDict[str, Metric]
    dag: DAG[str]


class Loader(abc.ABC):
    """Abstract base class to load macros and models for a context"""

    def __init__(self) -> None:
        self._path_mtimes: t.Dict[Path, float] = {}
        self._dag: DAG[str] = DAG()

    def load(self, context: GenericContext, update_schemas: bool = True) -> LoadedProject:
        """
        Loads all macros and models in the context's path.

        Args:
            context: The context to load macros and models for.
            update_schemas: Convert star projections to explicit columns.
        """
        # python files are cached by the system
        # need to manually clear here so we can reload macros
        linecache.clearcache()

        self._context = context
        self._path_mtimes.clear()
        self._dag = DAG()

        self._load_materializations()
        self._load_signals()

        config_mtimes: t.Dict[Path, t.List[float]] = defaultdict(list)
        for context_path, config in self._context.configs.items():
            for config_file in context_path.glob("config.*"):
                self._track_file(config_file)
                config_mtimes[context_path].append(self._path_mtimes[config_file])

        for config_file in c.SQLMESH_PATH.glob("config.*"):
            self._track_file(config_file)
            config_mtimes[c.SQLMESH_PATH].append(self._path_mtimes[config_file])

        self._config_mtimes = {path: max(mtimes) for path, mtimes in config_mtimes.items()}

        macros, jinja_macros = self._load_scripts()
        audits = self._load_audits(macros=macros, jinja_macros=jinja_macros)
        models = self._load_models(
            macros, jinja_macros, context.gateway or context.config.default_gateway, audits or None
        )

        for model in models.values():
            self._add_model_to_dag(model)

        # This topologically sorts the DAG & caches the result in-memory for later;
        # we do it here to detect any cycles as early as possible and fail if needed
        self._dag.sorted

        if update_schemas:
            update_model_schemas(
                self._dag,
                models=models,
                audits={k: v for k, v in audits.items() if isinstance(v, ModelAudit)},
                context_path=self._context.path,
            )
            for model in models.values():
                # The model definition can be validated correctly only after the schema is set.
                model.validate_definition()

        metrics = self._load_metrics()

        project = LoadedProject(
            macros=macros,
            jinja_macros=jinja_macros,
            models=models,
            audits=audits,
            metrics=expand_metrics(metrics),
            dag=self._dag,
        )
        return project

    def load_signals(self, context: GenericContext) -> None:
        """Loads signals for the built-in scheduler."""
        self._context = context
        self._load_signals()

    def load_materializations(self, context: GenericContext) -> None:
        """Loads materializations for the built-in scheduler."""
        self._context = context
        self._load_materializations()

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
        audits: t.Optional[t.Dict[str, Audit]],
    ) -> UniqueKeyDict[str, Model]:
        """Loads all models."""

    @abc.abstractmethod
    def _load_audits(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Audit]:
        """Loads all audits."""

    def _load_materializations(self) -> None:
        """Loads custom materializations."""

    def _load_signals(self) -> None:
        """Loads signals for the built-in scheduler."""

    def _load_metrics(self) -> UniqueKeyDict[str, MetricMeta]:
        return UniqueKeyDict("metrics")

    def _load_external_models(self, gateway: t.Optional[str] = None) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        for context_path, config in self._context.configs.items():
            external_models_yaml = Path(context_path / c.EXTERNAL_MODELS_YAML)
            deprecated_yaml = Path(context_path / c.EXTERNAL_MODELS_DEPRECATED_YAML)
            external_models_path = context_path / c.EXTERNAL_MODELS

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
                            defaults=config.model_defaults.dict(),
                            path=path,
                            project=config.project,
                            **{
                                "dialect": config.model_defaults.dialect,
                                "default_catalog": self._context.default_catalog,
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

        for filepath in path.glob(f"**/*{extension}"):
            for ignore_pattern in ignore_patterns:
                if filepath.match(ignore_pattern):
                    break
            else:
                yield filepath

    def _add_model_to_dag(self, model: Model) -> None:
        self._dag.add(model.fqn, model.depends_on)

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

        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(
                context_path / c.MACROS, ignore_patterns=config.ignore_patterns, extension=".py"
            ):
                if import_python_file(path, context_path):
                    self._track_file(path)
                    macro_file_mtime = self._path_mtimes[path]
                    macros_max_mtime = (
                        max(macros_max_mtime, macro_file_mtime)
                        if macros_max_mtime
                        else macro_file_mtime
                    )

            for path in self._glob_paths(
                context_path / c.MACROS, ignore_patterns=config.ignore_patterns, extension=".sql"
            ):
                self._track_file(path)
                macro_file_mtime = self._path_mtimes[path]
                macros_max_mtime = (
                    max(macros_max_mtime, macro_file_mtime)
                    if macros_max_mtime
                    else macro_file_mtime
                )
                with open(path, "r", encoding="utf-8") as file:
                    jinja_macros.add_macros(extractor.extract(file.read()))

        self._macros_max_mtime = macros_max_mtime

        macros = macro.get_registry()
        macro.set_registry(standard_macros)

        return macros, jinja_macros

    def _load_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        gateway: t.Optional[str],
        audits: t.Optional[t.Dict[str, Audit]] = None,
    ) -> UniqueKeyDict[str, Model]:
        """
        Loads all of the models within the model directory with their associated
        audits into a Dict and creates the dag
        """
        models = self._load_sql_models(macros, jinja_macros, audits)
        models.update(self._load_external_models(gateway))
        models.update(self._load_python_models(macros, jinja_macros))

        return models

    def _load_sql_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        audits: t.Optional[t.Dict[str, Audit]] = None,
    ) -> UniqueKeyDict[str, Model]:
        """Loads the sql models into a Dict"""
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        for context_path, config in self._context._loaders[c.NATIVE].configs.items():
            cache = SqlMeshLoader._Cache(self, context_path)
            variables = self._variables(config)

            for path in self._glob_paths(
                context_path / c.MODELS,
                ignore_patterns=config.ignore_patterns,
                extension=".sql",
            ):
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

                    return load_sql_based_model(
                        expressions,
                        defaults=config.model_defaults.dict(),
                        macros=macros,
                        jinja_macros=jinja_macros,
                        audits=audits,
                        default_audits=config.model_defaults.audits,
                        path=Path(path).absolute(),
                        module_path=context_path,
                        dialect=config.model_defaults.dialect,
                        time_column_format=config.time_column_format,
                        physical_schema_mapping=config.physical_schema_mapping,
                        project=config.project,
                        default_catalog=self._context.default_catalog,
                        variables=variables,
                        infer_names=config.model_naming.infer_names,
                    )

                model = cache.get_or_load_model(path, _load)
                if model.enabled:
                    models[model.fqn] = model

                if isinstance(model, SeedModel):
                    seed_path = model.seed_path
                    self._track_file(seed_path)

        return models

    def _load_python_models(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Model]:
        """Loads the python models into a Dict"""
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        registry = model_registry.registry()
        registry.clear()
        registered: t.Set[str] = set()

        for context_path, config in self._context.configs.items():
            variables = self._variables(config)
            model_registry._dialect = config.model_defaults.dialect
            try:
                for path in self._glob_paths(
                    context_path / c.MODELS, ignore_patterns=config.ignore_patterns, extension=".py"
                ):
                    if not os.path.getsize(path):
                        continue

                    self._track_file(path)
                    import_python_file(path, context_path)
                    new = registry.keys() - registered
                    registered |= new
                    for name in new:
                        model = registry[name].model(
                            path=path,
                            module_path=context_path,
                            defaults=config.model_defaults.dict(),
                            macros=macros,
                            jinja_macros=jinja_macros,
                            dialect=config.model_defaults.dialect,
                            time_column_format=config.time_column_format,
                            physical_schema_mapping=config.physical_schema_mapping,
                            project=config.project,
                            default_catalog=self._context.default_catalog,
                            variables=variables,
                            infer_names=config.model_naming.infer_names,
                        )
                        if model.enabled:
                            models[model.fqn] = model
            finally:
                model_registry._dialect = None

        return models

    def _load_materializations(self) -> None:
        """Loads custom materializations."""
        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(
                context_path / c.MATERIALIZATIONS,
                ignore_patterns=config.ignore_patterns,
                extension=".py",
            ):
                if os.path.getsize(path):
                    import_python_file(path, context_path)

    def _load_signals(self) -> None:
        """Loads signals for the built-in scheduler."""
        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(
                context_path / c.SIGNALS,
                ignore_patterns=config.ignore_patterns,
                extension=".py",
            ):
                if os.path.getsize(path):
                    import_python_file(path, context_path)

    def _load_audits(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Audit]:
        """Loads all the model audits."""
        audits_by_name: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        for context_path, config in self._context.configs.items():
            variables = self._variables(config)
            for path in self._glob_paths(
                context_path / c.AUDITS, ignore_patterns=config.ignore_patterns, extension=".sql"
            ):
                self._track_file(path)
                with open(path, "r", encoding="utf-8") as file:
                    expressions = parse(file.read(), default_dialect=config.model_defaults.dialect)
                    audits = load_multiple_audits(
                        expressions=expressions,
                        path=path,
                        module_path=context_path,
                        macros=macros,
                        jinja_macros=jinja_macros,
                        dialect=config.model_defaults.dialect,
                        default_catalog=self._context.default_catalog,
                        variables=variables,
                    )
                    for audit in audits:
                        audits_by_name[audit.name] = audit
        return audits_by_name

    def _load_metrics(self) -> UniqueKeyDict[str, MetricMeta]:
        """Loads all metrics."""
        metrics: UniqueKeyDict[str, MetricMeta] = UniqueKeyDict("metrics")

        for context_path, config in self._context.configs.items():
            for path in self._glob_paths(
                context_path / c.METRICS, ignore_patterns=config.ignore_patterns, extension=".sql"
            ):
                if not os.path.getsize(path):
                    continue
                self._track_file(path)

                with open(path, "r", encoding="utf-8") as file:
                    dialect = config.model_defaults.dialect
                    try:
                        for expression in parse(file.read(), default_dialect=dialect):
                            metric = load_metric_ddl(expression, path=path, dialect=dialect)
                            metrics[metric.name] = metric
                    except SqlglotError as ex:
                        raise ConfigError(f"Failed to parse metric definitions at '{path}': {ex}.")

        return metrics

    def _variables(self, config: Config) -> t.Dict[str, t.Any]:
        gateway_name = self._context.gateway or self._context.config.default_gateway_name
        try:
            gateway = config.get_gateway(gateway_name)
        except ConfigError:
            logger.warning("Gateway '%s' not found in project '%s'", gateway_name, config.project)
            gateway = None
        return {
            **config.variables,
            **(gateway.variables if gateway else {}),
            c.GATEWAY: gateway_name,
        }

    class _Cache:
        def __init__(self, loader: SqlMeshLoader, context_path: Path):
            self._loader = loader
            self._context_path = context_path
            self._model_cache = ModelCache(self._context_path / c.CACHE)

        def get_or_load_model(self, target_path: Path, loader: t.Callable[[], Model]) -> Model:
            model = self._model_cache.get_or_load(
                self._cache_entry_name(target_path),
                self._model_cache_entry_id(target_path),
                loader=loader,
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
                self._loader._config_mtimes.get(c.SQLMESH_PATH),
            ]
            return "__".join(
                [
                    str(max(m for m in mtimes if m is not None)),
                    self._loader._context.config.fingerprint,
                    # default catalog can change outside sqlmesh (e.g., DB user's
                    # default catalog), and it is retained in cached model's fully
                    # qualified name
                    self._loader._context.default_catalog or "",
                    # gateway is configurable, and it is retained in a cached
                    # model's python environment if the @gateway macro variable is
                    # used in the model
                    self._loader._context.gateway
                    or self._loader._context.config.default_gateway_name,
                ]
            )


# TODO: consider moving this to context
def update_model_schemas(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    audits: t.Dict[str, ModelAudit],
    context_path: Path,
) -> None:
    schema = MappingSchema(normalize=False)
    optimized_query_cache: OptimizedQueryCache = OptimizedQueryCache(context_path / c.CACHE)

    if c.MAX_FORK_WORKERS == 1:
        _update_model_schemas_sequential(dag, models, schema, optimized_query_cache)
    else:
        _update_model_schemas_parallel(dag, models, audits, schema, optimized_query_cache)


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
    audits: t.Dict[str, ModelAudit],
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

    with optimized_query_cache_pool(optimized_query_cache, audits) as executor:
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
