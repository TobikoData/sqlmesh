"""
# Context

A SQLMesh context encapsulates a SQLMesh environment. When you create a new context, it will discover and
load your project's models, macros, and audits. Afterwards, you can use the context to create and apply
plans, visualize your model's lineage, run your audits and model tests, and perform various other tasks.
For more information regarding what a context can do, see `sqlmesh.core.context.Context`.

# Examples:

Creating and applying a plan against the staging environment.
```python
from sqlmesh.core.context import Context
context = Context(paths="example", config="local_config")
plan = context.plan("staging")
context.apply(plan)
```

Running audits on your data.
```python
from sqlmesh.core.context import Context
context = Context(paths="example", config="local_config")
context.audit("yesterday", "now")
```

Running tests on your models.
```python
from sqlmesh.core.context import Context
context = Context(paths="example")
context.test()
```
"""

from __future__ import annotations

import abc
import collections
import logging
import time
import traceback
import typing as t
import unittest.result
from datetime import date, timedelta
from functools import cached_property
from io import StringIO
from pathlib import Path
from shutil import rmtree
from types import MappingProxyType, SimpleNamespace

import pandas as pd
from sqlglot import Dialect, exp
from sqlglot.lineage import GraphHTML

from sqlmesh.core import analytics
from sqlmesh.core import constants as c
from sqlmesh.core.analytics import python_api_analytics
from sqlmesh.core.audit import Audit, ModelAudit, StandaloneAudit
from sqlmesh.core.config import CategorizerConfig, Config, load_configs
from sqlmesh.core.config.loader import C
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.dialect import (
    format_model_expressions,
    is_meta_expression,
    normalize_model_name,
    pandas_to_sql,
    parse,
    parse_one,
)
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.loader import Loader, update_model_schemas
from sqlmesh.core.macros import ExecutableOrMacro, macro
from sqlmesh.core.metric import Metric, rewrite
from sqlmesh.core.model import Model
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTarget,
    NotificationTargetManager,
)
from sqlmesh.core.plan import Plan, PlanBuilder
from sqlmesh.core.reference import ReferenceGraph
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.schema_loader import create_external_models_file
from sqlmesh.core.selector import Selector
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotEvaluator,
    SnapshotFingerprint,
    to_table_mapping,
)
from sqlmesh.core.state_sync import (
    CachingStateSync,
    StateReader,
    StateSync,
    cleanup_expired_views,
)
from sqlmesh.core.table_diff import TableDiff
from sqlmesh.core.test import (
    ModelTextTestResult,
    generate_test,
    get_all_model_tests,
    run_model_tests,
    run_tests,
)
from sqlmesh.core.user import User
from sqlmesh.utils import UniqueKeyDict, sys_path
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now_ds, to_date
from sqlmesh.utils.errors import (
    CircuitBreakerError,
    ConfigError,
    PlanError,
    SQLMeshError,
    UncategorizedPlanError,
)
from sqlmesh.utils.jinja import JinjaMacroRegistry

if t.TYPE_CHECKING:
    from typing_extensions import Literal

    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        SnowparkSession,
    )
    from sqlmesh.core.snapshot import Node

    ModelOrSnapshot = t.Union[str, Model, Snapshot]
    NodeOrSnapshot = t.Union[str, Model, StandaloneAudit, Snapshot]

logger = logging.getLogger(__name__)


class BaseContext(abc.ABC):
    """The base context which defines methods to execute a model."""

    @property
    @abc.abstractmethod
    def default_dialect(self) -> t.Optional[str]:
        """Returns the default dialect."""

    @property
    @abc.abstractmethod
    def _model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""

    @property
    @abc.abstractmethod
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""

    @property
    def spark(self) -> t.Optional[PySparkSession]:
        """Returns the spark session if it exists."""
        return self.engine_adapter.spark

    @property
    def snowpark(self) -> t.Optional[SnowparkSession]:
        """Returns the snowpark session if it exists."""
        return self.engine_adapter.snowpark

    @property
    def default_catalog(self) -> t.Optional[str]:
        raise NotImplementedError

    def table(self, model_name: str) -> str:
        """Gets the physical table name for a given model.

        Args:
            model_name: The model name.

        Returns:
            The physical table name.
        """
        model_name = normalize_model_name(model_name, self.default_catalog, self.default_dialect)

        # We generate SQL for the default dialect because the table name may be used in a
        # fetchdf call and so the quotes need to be correct (eg. backticks for bigquery)
        return parse_one(self._model_tables[model_name]).sql(
            dialect=self.default_dialect, identify=True
        )

    def fetchdf(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a dataframe given a sql string or sqlglot expression.

        Args:
            query: SQL string or sqlglot expression.
            quote_identifiers: Whether to quote all identifiers in the query.

        Returns:
            The default dataframe is Pandas, but for Spark a PySpark dataframe is returned.
        """
        return self.engine_adapter.fetchdf(query, quote_identifiers=quote_identifiers)

    def fetch_pyspark_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> PySparkDataFrame:
        """Fetches a PySpark dataframe given a sql string or sqlglot expression.

        Args:
            query: SQL string or sqlglot expression.
            quote_identifiers: Whether to quote all identifiers in the query.

        Returns:
            A PySpark dataframe.
        """
        return self.engine_adapter.fetch_pyspark_df(query, quote_identifiers=quote_identifiers)


class ExecutionContext(BaseContext):
    """The minimal context needed to execute a model.

    Args:
        engine_adapter: The engine adapter to execute queries against.
        snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
        deployability_index: Determines snapshots that are deployable in the context of this evaluation.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        snapshots: t.Dict[str, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
        default_dialect: t.Optional[str] = None,
        default_catalog: t.Optional[str] = None,
        variables: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.snapshots = snapshots
        self.deployability_index = deployability_index
        self._engine_adapter = engine_adapter
        self._default_catalog = default_catalog
        self._default_dialect = default_dialect
        self._variables = variables or {}

    @property
    def default_dialect(self) -> t.Optional[str]:
        return self._default_dialect

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    @cached_property
    def _model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""
        return to_table_mapping(self.snapshots.values(), self.deployability_index)

    @property
    def default_catalog(self) -> t.Optional[str]:
        return self._default_catalog

    @property
    def gateway(self) -> t.Optional[str]:
        """Returns the gateway name."""
        return self.var(c.GATEWAY)

    def var(self, var_name: str, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
        """Returns a variable value."""
        return self._variables.get(var_name.lower(), default)

    def with_variables(self, variables: t.Dict[str, t.Any]) -> ExecutionContext:
        """Returns a new ExecutionContext with additional variables."""
        return ExecutionContext(
            self._engine_adapter,
            self.snapshots,
            self.deployability_index,
            self._default_dialect,
            self._default_catalog,
            variables=variables,
        )


class GenericContext(BaseContext, t.Generic[C]):
    """Encapsulates a SQLMesh environment supplying convenient functions to perform various tasks.

    Args:
        engine_adapter: The default engine adapter to use.
        notification_targets: The notification target to use. Defaults to what is defined in config.
        paths: The directories containing SQLMesh files.
        config: A Config object or the name of a Config object in config.py.
        connection: The name of the connection. If not specified the first connection as it appears
            in configuration will be used.
        test_connection: The name of the connection to use for tests. If not specified the first
            connection as it appears in configuration will be used.
        concurrent_tasks: The maximum number of tasks that can use the connection concurrently.
        load: Whether or not to automatically load all models and macros (default True).
        console: The rich instance used for printing out CLI command results.
        users: A list of users to make known to SQLMesh.
    """

    CONFIG_TYPE: t.Type[C]
    """The type of config object to use (default: Config)."""

    PLAN_BUILDER_TYPE = PlanBuilder
    """The type of plan builder object to use (default: PlanBuilder)."""

    def __init__(
        self,
        engine_adapter: t.Optional[EngineAdapter] = None,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        state_sync: t.Optional[StateSync] = None,
        paths: t.Union[str | Path, t.Iterable[str | Path]] = "",
        config: t.Optional[t.Union[C, str, t.Dict[Path, C]]] = None,
        gateway: t.Optional[str] = None,
        concurrent_tasks: t.Optional[int] = None,
        loader: t.Optional[t.Type[Loader]] = None,
        load: bool = True,
        console: t.Optional[Console] = None,
        users: t.Optional[t.List[User]] = None,
    ):
        self.configs = (
            config if isinstance(config, dict) else load_configs(config, self.CONFIG_TYPE, paths)
        )
        self._loaders: UniqueKeyDict[str, SimpleNamespace] = UniqueKeyDict("loaders")
        self.dag: DAG[str] = DAG()
        self._models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        self._audits: UniqueKeyDict[str, ModelAudit] = UniqueKeyDict("audits")
        self._standalone_audits: UniqueKeyDict[str, StandaloneAudit] = UniqueKeyDict(
            "standaloneaudits"
        )
        self._macros: UniqueKeyDict[str, ExecutableOrMacro] = UniqueKeyDict("macros")
        self._metrics: UniqueKeyDict[str, Metric] = UniqueKeyDict("metrics")
        self._jinja_macros = JinjaMacroRegistry()
        self._default_catalog: t.Optional[str] = None
        self._loaded: bool = False

        self.path, self.config = t.cast(t.Tuple[Path, C], next(iter(self.configs.items())))
        for path, config in self.configs.items():
            project_type = c.DBT if config.loader.__name__.lower().startswith(c.DBT) else c.NATIVE
            if project_type not in self._loaders:
                self._loaders[project_type] = SimpleNamespace(
                    loader=(loader or config.loader)(**config.loader_kwargs), configs={}
                )
            self._loaders[project_type].configs[path] = config

        self.project_type = c.HYBRID if len(self._loaders) > 1 else project_type
        self._all_dialects: t.Set[str] = {self.config.dialect or ""}

        # This allows overriding the default dialect's normalization strategy, so for example
        # one can do `dialect="duckdb,normalization_strategy=lowercase"` and this will be
        # applied to the DuckDB dialect globally
        if "normalization_strategy" in str(self.config.dialect):
            dialect = Dialect.get_or_raise(self.config.dialect)
            type(dialect).NORMALIZATION_STRATEGY = dialect.normalization_strategy

        if self.config.disable_anonymized_analytics:
            analytics.disable_analytics()

        self.gateway = gateway
        self._scheduler = self.config.get_scheduler(self.gateway)
        self.environment_ttl = self.config.environment_ttl
        self.pinned_environments = Environment.sanitize_names(self.config.pinned_environments)
        self.auto_categorize_changes = self.config.plan.auto_categorize_changes

        self._connection_config = self.config.get_connection(self.gateway)
        self.concurrent_tasks = concurrent_tasks or self._connection_config.concurrent_tasks
        self._engine_adapter = engine_adapter or self._connection_config.create_engine_adapter()

        self.console = console or get_console(dialect=self._engine_adapter.dialect)

        self._test_connection_config = self.config.get_test_connection(
            self.gateway, self.default_catalog, default_catalog_dialect=self.engine_adapter.DIALECT
        )

        self._snapshot_evaluator: t.Optional[SnapshotEvaluator] = None

        self._provided_state_sync: t.Optional[StateSync] = state_sync
        self._state_sync: t.Optional[StateSync] = None

        # Should we dedupe notification_targets? If so how?
        self.notification_targets = (notification_targets or []) + self.config.notification_targets
        self.users = (users or []) + self.config.users
        self.users = list({user.username: user for user in self.users}.values())
        self._register_notification_targets()

        if (
            self.config.environment_catalog_mapping
            and not self.engine_adapter.CATALOG_SUPPORT.is_multi_catalog_supported
        ):
            raise SQLMeshError(
                "Environment catalog mapping is only supported for engine adapters that support multiple catalogs"
            )

        if load:
            self.load()

    @property
    def default_dialect(self) -> t.Optional[str]:
        return self.config.dialect

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    @property
    def snapshot_evaluator(self) -> SnapshotEvaluator:
        if not self._snapshot_evaluator:
            self._snapshot_evaluator = SnapshotEvaluator(
                self.engine_adapter.with_log_level(logging.INFO),
                ddl_concurrent_tasks=self.concurrent_tasks,
            )
        return self._snapshot_evaluator

    def execution_context(
        self, deployability_index: t.Optional[DeployabilityIndex] = None
    ) -> ExecutionContext:
        """Returns an execution context."""
        return ExecutionContext(
            engine_adapter=self._engine_adapter,
            snapshots=self.snapshots,
            deployability_index=deployability_index,
            default_dialect=self.default_dialect,
            default_catalog=self.default_catalog,
        )

    @python_api_analytics
    def upsert_model(self, model: t.Union[str, Model], **kwargs: t.Any) -> Model:
        """Update or insert a model.

        The context's models dictionary will be updated to include these changes.

        Args:
            model: Model name or instance to update.
            kwargs: The kwargs to update the model with.

        Returns:
            A new instance of the updated or inserted model.
        """
        model = self.get_model(model, raise_if_missing=True)
        if not model.enabled:
            raise SQLMeshError(f"The disabled model '{model.name}' cannot be upserted")
        path = model._path

        model = model.copy(update=kwargs)
        model._path = path

        self.dag.add(model.fqn, model.depends_on)

        self._models.update(
            {
                model.fqn: model,
                # bust the fingerprint cache for all downstream models
                **{fqn: self._models[fqn].copy() for fqn in self.dag.downstream(model.fqn)},
            }
        )

        update_model_schemas(
            self.dag,
            models=self._models,
            audits=self._audits,
            context_path=self.path,
        )

        if model.dialect:
            self._all_dialects.add(model.dialect)

        model.validate_definition()

        return model

    def scheduler(self, environment: t.Optional[str] = None) -> Scheduler:
        """Returns the built-in scheduler.

        Args:
            environment: The target environment to source model snapshots from, or None
                if snapshots should be sourced from the currently loaded local state.

        Returns:
            The built-in scheduler instance.
        """
        snapshots: t.Iterable[Snapshot]
        if environment is not None:
            stored_environment = self.state_sync.get_environment(environment)
            if stored_environment is None:
                raise ConfigError(f"Environment '{environment}' was not found.")
            snapshots = self.state_sync.get_snapshots(stored_environment.snapshots).values()
        else:
            snapshots = self.snapshots.values()

        if not snapshots:
            raise ConfigError("No models were found")

        return self.create_scheduler(snapshots)

    def create_scheduler(self, snapshots: t.Iterable[Snapshot]) -> Scheduler:
        """Creates the built-in scheduler.

        Args:
            snapshots: The snapshots to schedule.

        Returns:
            The built-in scheduler instance.
        """
        return Scheduler(
            snapshots,
            self.snapshot_evaluator,
            self.state_sync,
            default_catalog=self.default_catalog,
            max_workers=self.concurrent_tasks,
            console=self.console,
            notification_target_manager=self.notification_target_manager,
        )

    @property
    def state_sync(self) -> StateSync:
        if not self._state_sync:
            self._state_sync = self._new_state_sync()

            if self._state_sync.get_versions(validate=False).schema_version == 0:
                self._state_sync.migrate(default_catalog=self.default_catalog)
            self._state_sync.get_versions()
            self._state_sync = CachingStateSync(self._state_sync)  # type: ignore
        return self._state_sync

    @property
    def state_reader(self) -> StateReader:
        return self.state_sync

    def refresh(self) -> None:
        """Refresh all models that have been updated."""
        if any(context_loader.loader.reload_needed() for context_loader in self._loaders.values()):
            self.load()

    def load(self, update_schemas: bool = True) -> GenericContext[C]:
        """Load all files in the context's path."""
        load_start_ts = time.perf_counter()

        projects = []
        for context_loader in self._loaders.values():
            with sys_path(*context_loader.configs):
                projects.append(context_loader.loader.load(self, update_schemas))

        self._standalone_audits.clear()
        self._audits.clear()
        self._macros.clear()
        self._models.clear()
        self._metrics.clear()
        for project in projects:
            self._jinja_macros = self._jinja_macros.merge(project.jinja_macros)
            self._macros.update(project.macros)
            self._models.update(project.models)
            self._metrics.update(project.metrics)

            for name, audit in project.audits.items():
                if isinstance(audit, StandaloneAudit):
                    self._standalone_audits[name] = audit
                else:
                    self._audits[name] = audit

        self.dag = DAG({k: v for project in projects for k, v in project.dag.graph.items()})

        duplicates = set(self._models) & set(self._standalone_audits)
        if duplicates:
            raise ConfigError(
                f"Models and Standalone audits cannot have the same name: {duplicates}"
            )

        self._all_dialects = {m.dialect for m in self._models.values() if m.dialect} | {
            self.default_dialect or ""
        }

        analytics.collector.on_project_loaded(
            project_type=self.project_type,
            models_count=len(self._models),
            audits_count=len(self._audits),
            standalone_audits_count=len(self._standalone_audits),
            macros_count=len(self._macros),
            jinja_macros_count=len(self._jinja_macros.root_macros),
            load_time_sec=time.perf_counter() - load_start_ts,
            state_sync_fingerprint=self._scheduler.state_sync_fingerprint(self),
            project_name=self.config.project,
        )

        self._loaded = True
        return self

    @python_api_analytics
    def run(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        skip_janitor: bool = False,
        ignore_cron: bool = False,
    ) -> bool:
        """Run the entire dag through the scheduler.

        Args:
            environment: The target environment to source model snapshots from and virtually update. Default: prod.
            start: The start of the interval to render.
            end: The end of the interval to render.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            skip_janitor: Whether to skip the janitor task.
            ignore_cron: Whether to ignore the model's cron schedule and run all available missing intervals.

        Returns:
            True if the run was successful, False otherwise.
        """
        environment = environment or self.config.default_target_environment

        self.notification_target_manager.notify(
            NotificationEvent.RUN_START, environment=environment
        )
        analytics_run_id = analytics.collector.on_run_start(
            engine_type=self.snapshot_evaluator.adapter.dialect,
            state_sync_type=self.state_sync.state_type(),
        )

        if not self._loaded:
            # Signals and materializations should be loaded to run correctly.
            for context_loader in self._loaders.values():
                with sys_path(*context_loader.configs):
                    context_loader.loader.load_signals(self)
                    context_loader.loader.load_materializations(self)

        success = False
        try:
            success = self._run(
                environment=environment,
                start=start,
                end=end,
                execution_time=execution_time,
                skip_janitor=skip_janitor,
                ignore_cron=ignore_cron,
            )
        except Exception as e:
            self.notification_target_manager.notify(
                NotificationEvent.RUN_FAILURE, traceback.format_exc()
            )
            logger.error(f"Run Failure: {traceback.format_exc()}")
            analytics.collector.on_run_end(run_id=analytics_run_id, succeeded=False, error=e)
            raise e

        if success:
            self.notification_target_manager.notify(
                NotificationEvent.RUN_END, environment=environment
            )
            self.console.log_success(f"Run finished for environment '{environment}'")
        else:
            self.notification_target_manager.notify(
                NotificationEvent.RUN_FAILURE, "See console logs for details."
            )

        analytics.collector.on_run_end(run_id=analytics_run_id, succeeded=success)

        return success

    @python_api_analytics
    def run_janitor(self, ignore_ttl: bool) -> bool:
        success = False

        if self.console.start_cleanup(ignore_ttl):
            try:
                self._run_janitor(ignore_ttl)
                success = True
            finally:
                self.console.stop_cleanup(success=success)

        return success

    @t.overload
    def get_model(
        self, model_or_snapshot: ModelOrSnapshot, raise_if_missing: Literal[True] = True
    ) -> Model: ...

    @t.overload
    def get_model(
        self,
        model_or_snapshot: ModelOrSnapshot,
        raise_if_missing: Literal[False] = False,
    ) -> t.Optional[Model]: ...

    def get_model(
        self, model_or_snapshot: ModelOrSnapshot, raise_if_missing: bool = False
    ) -> t.Optional[Model]:
        """Returns a model with the given name or None if a model with such name doesn't exist.

        Args:
            model_or_snapshot: A model name, model, or snapshot.
            raise_if_missing: Raises an error if a model is not found.

        Returns:
            The expected model.
        """
        if isinstance(model_or_snapshot, str):
            # We should try all dialects referenced in the project for cases when models use mixed dialects.
            for dialect in self._all_dialects:
                normalized_name = normalize_model_name(
                    model_or_snapshot,
                    dialect=dialect,
                    default_catalog=self.default_catalog,
                )
                if normalized_name in self._models:
                    return self._models[normalized_name]
        elif isinstance(model_or_snapshot, Snapshot):
            return model_or_snapshot.model
        else:
            return model_or_snapshot

        if raise_if_missing:
            raise SQLMeshError(f"Cannot find model for '{model_or_snapshot}'")

        return None

    @t.overload
    def get_snapshot(self, node_or_snapshot: NodeOrSnapshot) -> t.Optional[Snapshot]: ...

    @t.overload
    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: Literal[True]
    ) -> Snapshot: ...

    @t.overload
    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: Literal[False]
    ) -> t.Optional[Snapshot]: ...

    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: bool = False
    ) -> t.Optional[Snapshot]:
        """Returns a snapshot with the given name or None if a snapshot with such name doesn't exist.

        Args:
            node_or_snapshot: A node name, node, or snapshot.
            raise_if_missing: Raises an error if a snapshot is not found.

        Returns:
            The expected snapshot.
        """
        if isinstance(node_or_snapshot, Snapshot):
            return node_or_snapshot
        if isinstance(node_or_snapshot, str) and not self.standalone_audits.get(node_or_snapshot):
            node_or_snapshot = normalize_model_name(
                node_or_snapshot,
                dialect=self.default_dialect,
                default_catalog=self.default_catalog,
            )
        fqn = node_or_snapshot if isinstance(node_or_snapshot, str) else node_or_snapshot.fqn
        snapshot = self.snapshots.get(fqn)

        if raise_if_missing and not snapshot:
            raise SQLMeshError(f"Cannot find snapshot for '{fqn}'")

        return snapshot

    def config_for_path(self, path: Path) -> Config:
        for config_path, config in self.configs.items():
            try:
                path.relative_to(config_path)
                return config
            except ValueError:
                pass
        return self.config

    def config_for_node(self, node: str | Model | Audit) -> Config:
        if isinstance(node, str):
            return self.config_for_path(self.get_snapshot(node, raise_if_missing=True).node._path)  # type: ignore
        return self.config_for_path(node._path)  # type: ignore

    @property
    def models(self) -> MappingProxyType[str, Model]:
        """Returns all registered models in this context."""
        return MappingProxyType(self._models)

    @property
    def metrics(self) -> MappingProxyType[str, Metric]:
        """Returns all registered metrics in this context."""
        return MappingProxyType(self._metrics)

    @property
    def standalone_audits(self) -> MappingProxyType[str, StandaloneAudit]:
        """Returns all registered standalone audits in this context."""
        return MappingProxyType(self._standalone_audits)

    @property
    def snapshots(self) -> t.Dict[str, Snapshot]:
        """Generates and returns snapshots based on models registered in this context.

        If one of the snapshots has been previously stored in the persisted state, the stored
        instance will be returned.
        """
        return self._snapshots()

    @property
    def default_catalog(self) -> t.Optional[str]:
        if self._default_catalog is None:
            self._default_catalog = self._scheduler.get_default_catalog(self)
        return self._default_catalog

    @python_api_analytics
    def render(
        self,
        model_or_snapshot: ModelOrSnapshot,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        expand: t.Union[bool, t.Iterable[str]] = False,
        **kwargs: t.Any,
    ) -> exp.Expression:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to render.
            end: The end of the interval to render.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            expand: Whether or not to use expand materialized models, defaults to False.
                If True, all referenced models are expanded as raw queries.
                If a list, only referenced models are expanded as raw queries.

        Returns:
            The rendered expression.
        """
        execution_time = execution_time or now_ds()

        model = self.get_model(model_or_snapshot, raise_if_missing=True)

        if expand and not isinstance(expand, bool):
            expand = {
                normalize_model_name(
                    x, default_catalog=self.default_catalog, dialect=self.default_dialect
                )
                for x in expand
            }

        expand = self.dag.upstream(model.fqn) if expand is True else expand or []

        if model.is_seed:
            df = next(
                model.render(
                    context=self.execution_context(),
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    **kwargs,
                )
            )
            return next(pandas_to_sql(t.cast(pd.DataFrame, df), model.columns_to_types))

        snapshots = self.snapshots
        deployability_index = DeployabilityIndex.create(snapshots.values())

        return model.render_query_or_raise(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            expand=expand,
            deployability_index=deployability_index,
            engine_adapter=self.engine_adapter,
            **kwargs,
        )

    @python_api_analytics
    def evaluate(
        self,
        model_or_snapshot: ModelOrSnapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        limit: t.Optional[int] = None,
        **kwargs: t.Any,
    ) -> DF:
        """Evaluate a model or snapshot (running its query against a DB/Engine).

        This method is used to test or iterate on models without side effects.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to evaluate.
            end: The end of the interval to evaluate.
            execution_time: The date/time time reference to use for execution time.
            limit: A limit applied to the model.
        """
        snapshot = self.get_snapshot(model_or_snapshot, raise_if_missing=True)

        df = self.snapshot_evaluator.evaluate_and_fetch(
            snapshot,
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=self.snapshots,
            limit=limit or c.DEFAULT_MAX_LIMIT,
        )

        if df is None:
            raise RuntimeError(f"Error evaluating {snapshot.name}")

        return df

    @python_api_analytics
    def format(
        self,
        transpile: t.Optional[str] = None,
        rewrite_casts: t.Optional[bool] = None,
        append_newline: t.Optional[bool] = None,
        *,
        check: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> bool:
        """Format all SQL models and audits."""
        format_targets = {**self._models, **self._audits}
        for target in format_targets.values():
            if target._path is None or target._path.suffix != ".sql":
                continue

            with open(target._path, "r+", encoding="utf-8") as file:
                before = file.read()
                expressions = parse(before, default_dialect=self.config_for_node(target).dialect)
                if transpile and is_meta_expression(expressions[0]):
                    for prop in expressions[0].expressions:
                        if prop.name.lower() == "dialect":
                            prop.replace(
                                exp.Property(
                                    this="dialect",
                                    value=exp.Literal.string(transpile or target.dialect),
                                )
                            )

                format_config = self.config_for_node(target).format
                after = format_model_expressions(
                    expressions,
                    transpile or target.dialect,
                    rewrite_casts=(
                        rewrite_casts
                        if rewrite_casts is not None
                        else not format_config.no_rewrite_casts
                    ),
                    **{**format_config.generator_options, **kwargs},
                )

                if append_newline is None:
                    append_newline = format_config.append_newline
                if append_newline:
                    after += "\n"

                if not check:
                    file.seek(0)
                    file.write(after)
                    file.truncate()
                elif before != after:
                    return False
        return True

    @python_api_analytics
    def plan(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        create_from: t.Optional[str] = None,
        skip_tests: bool = False,
        restate_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        forward_only: t.Optional[bool] = None,
        allow_destructive_models: t.Optional[t.Collection[str]] = None,
        no_prompts: t.Optional[bool] = None,
        auto_apply: t.Optional[bool] = None,
        no_auto_categorization: t.Optional[bool] = None,
        effective_from: t.Optional[TimeLike] = None,
        include_unmodified: t.Optional[bool] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        backfill_models: t.Optional[t.Collection[str]] = None,
        categorizer_config: t.Optional[CategorizerConfig] = None,
        enable_preview: t.Optional[bool] = None,
        no_diff: t.Optional[bool] = None,
        run: bool = False,
    ) -> Plan:
        """Interactively creates a plan.

        This method compares the current context with the target environment. It then presents
        the differences and asks whether to backfill each modified model.

        Args:
            environment: The environment to diff and plan against.
            start: The start date of the backfill if there is one.
            end: The end date of the backfill if there is one.
            execution_time: The date/time reference to use for execution time. Defaults to now.
            create_from: The environment to create the target environment from if it
                doesn't exist. If not specified, the "prod" environment will be used.
            skip_tests: Unit tests are run by default so this will skip them if enabled
            restate_models: A list of either internal or external models, or tags, that need to be restated
                for the given plan interval. If the target environment is a production environment,
                ALL snapshots that depended on these upstream tables will have their intervals deleted
                (even ones not in this current environment). Only the snapshots in this environment will
                be backfilled whereas others need to be recovered on a future plan application. For development
                environments only snapshots that are part of this plan will be affected.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.
            skip_backfill: Whether to skip the backfill step. Default: False.
            forward_only: Whether the purpose of the plan is to make forward only changes.
            allow_destructive_models: Models whose forward-only changes are allowed to be destructive.
            no_prompts: Whether to disable interactive prompts for the backfill time range. Please note that
                if this flag is set to true and there are uncategorized changes the plan creation will
                fail. Default: False.
            auto_apply: Whether to automatically apply the new plan after creation. Default: False.
            no_auto_categorization: Indicates whether to disable automatic categorization of model
                changes (breaking / non-breaking). If not provided, then the corresponding configuration
                option determines the behavior.
            categorizer_config: The configuration for the categorizer. Uses the categorizer configuration defined in the
                project config by default.
            effective_from: The effective date from which to apply forward-only changes on production.
            include_unmodified: Indicates whether to include unmodified models in the target development environment.
            select_models: A list of model selection strings to filter the models that should be included into this plan.
            backfill_models: A list of model selection strings to filter the models for which the data should be backfilled.
            enable_preview: Indicates whether to enable preview for forward-only models in development environments.
            no_diff: Hide text differences for changed models.
            run: Whether to run latest intervals as part of the plan application.

        Returns:
            The populated Plan object.
        """
        plan_builder = self.plan_builder(
            environment,
            start=start,
            end=end,
            execution_time=execution_time,
            create_from=create_from,
            skip_tests=skip_tests,
            restate_models=restate_models,
            no_gaps=no_gaps,
            skip_backfill=skip_backfill,
            forward_only=forward_only,
            allow_destructive_models=allow_destructive_models,
            no_auto_categorization=no_auto_categorization,
            effective_from=effective_from,
            include_unmodified=include_unmodified,
            select_models=select_models,
            backfill_models=backfill_models,
            categorizer_config=categorizer_config,
            enable_preview=enable_preview,
            run=run,
        )

        self.console.plan(
            plan_builder,
            auto_apply if auto_apply is not None else self.config.plan.auto_apply,
            self.default_catalog,
            no_diff=no_diff if no_diff is not None else self.config.plan.no_diff,
            no_prompts=no_prompts if no_prompts is not None else self.config.plan.no_prompts,
        )

        return plan_builder.build()

    @python_api_analytics
    def plan_builder(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        create_from: t.Optional[str] = None,
        skip_tests: bool = False,
        restate_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        forward_only: t.Optional[bool] = None,
        allow_destructive_models: t.Optional[t.Collection[str]] = None,
        no_auto_categorization: t.Optional[bool] = None,
        effective_from: t.Optional[TimeLike] = None,
        include_unmodified: t.Optional[bool] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        backfill_models: t.Optional[t.Collection[str]] = None,
        categorizer_config: t.Optional[CategorizerConfig] = None,
        enable_preview: t.Optional[bool] = None,
        run: bool = False,
    ) -> PlanBuilder:
        """Creates a plan builder.

        Args:
            environment: The environment to diff and plan against.
            start: The start date of the backfill if there is one.
            end: The end date of the backfill if there is one.
            execution_time: The date/time reference to use for execution time. Defaults to now.
            create_from: The environment to create the target environment from if it
                doesn't exist. If not specified, the "prod" environment will be used.
            skip_tests: Unit tests are run by default so this will skip them if enabled
            restate_models: A list of either internal or external models, or tags, that need to be restated
                for the given plan interval. If the target environment is a production environment,
                ALL snapshots that depended on these upstream tables will have their intervals deleted
                (even ones not in this current environment). Only the snapshots in this environment will
                be backfilled whereas others need to be recovered on a future plan application. For development
                environments only snapshots that are part of this plan will be affected.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.
            skip_backfill: Whether to skip the backfill step. Default: False.
            forward_only: Whether the purpose of the plan is to make forward only changes.
            allow_destructive_models: Models whose forward-only changes are allowed to be destructive.
            no_auto_categorization: Indicates whether to disable automatic categorization of model
                changes (breaking / non-breaking). If not provided, then the corresponding configuration
                option determines the behavior.
            categorizer_config: The configuration for the categorizer. Uses the categorizer configuration defined in the
                project config by default.
            effective_from: The effective date from which to apply forward-only changes on production.
            include_unmodified: Indicates whether to include unmodified models in the target development environment.
            select_models: A list of model selection strings to filter the models that should be included into this plan.
            backfill_models: A list of model selection strings to filter the models for which the data should be backfilled.
            enable_preview: Indicates whether to enable preview for forward-only models in development environments.
            run: Whether to run latest intervals as part of the plan application.

        Returns:
            The plan builder.
        """
        environment = environment or self.config.default_target_environment
        environment = Environment.sanitize_name(environment)
        is_dev = environment != c.PROD

        if include_unmodified is None:
            include_unmodified = self.config.plan.include_unmodified

        if skip_backfill and not no_gaps and not is_dev:
            raise ConfigError(
                "When targeting the production environment either the backfill should not be skipped or the lack of data gaps should be enforced (--no-gaps flag)."
            )

        if run and is_dev:
            raise ConfigError("The '--run' flag is only supported for the production environment.")

        self._run_plan_tests(skip_tests=skip_tests)

        environment_ttl = (
            self.environment_ttl if environment not in self.pinned_environments else None
        )

        model_selector = self._new_selector()

        if allow_destructive_models:
            expanded_destructive_models = model_selector.expand_model_selections(
                allow_destructive_models
            )
        else:
            expanded_destructive_models = None

        if backfill_models:
            backfill_models = model_selector.expand_model_selections(backfill_models)
        else:
            backfill_models = None

        models_override: t.Optional[UniqueKeyDict[str, Model]] = None
        if select_models:
            models_override = model_selector.select_models(
                select_models,
                environment,
                fallback_env_name=create_from or c.PROD,
                ensure_finalized_snapshots=self.config.plan.use_finalized_state,
            )
            if not backfill_models:
                # Only backfill selected models unless explicitly specified.
                backfill_models = model_selector.expand_model_selections(select_models)

        expanded_restate_models = None
        if restate_models is not None:
            expanded_restate_models = model_selector.expand_model_selections(restate_models)

        snapshots = self._snapshots(models_override)
        context_diff = self._context_diff(
            environment or c.PROD,
            snapshots=snapshots,
            create_from=create_from,
            force_no_diff=restate_models is not None
            or (backfill_models is not None and not backfill_models),
            ensure_finalized_snapshots=self.config.plan.use_finalized_state,
        )

        if (
            is_dev
            and not include_unmodified
            and backfill_models is None
            and expanded_restate_models is None
        ):
            # Only backfill modified and added models.
            # This ensures that no models outside the impacted sub-DAG(s) will be backfilled unexpectedly.
            backfill_models = {
                *context_diff.modified_snapshots,
                *[s.name for s in context_diff.added],
            }

        # If no end date is specified, use the max interval end from prod
        # to prevent unintended evaluation of the entire DAG.
        default_end: t.Optional[int] = None
        default_start: t.Optional[date] = None
        max_interval_end_per_model: t.Optional[t.Dict[str, int]] = None
        if not run and not end:
            models_for_interval_end: t.Optional[t.Set[str]] = None
            if backfill_models is not None:
                models_for_interval_end = set()
                models_stack = list(backfill_models)
                while models_stack:
                    next_model = models_stack.pop()
                    if next_model not in snapshots:
                        continue
                    models_for_interval_end.add(next_model)
                    models_stack.extend(
                        s.name
                        for s in snapshots[next_model].parents
                        if s.name not in models_for_interval_end
                    )

            max_interval_end_per_model = self.state_sync.max_interval_end_per_model(
                c.PROD,
                models=models_for_interval_end,
                ensure_finalized_snapshots=self.config.plan.use_finalized_state,
            )
            if max_interval_end_per_model:
                default_end = max(max_interval_end_per_model.values())
                default_start = to_date(min(max_interval_end_per_model.values())) - timedelta(
                    days=1
                )

        return self.PLAN_BUILDER_TYPE(
            context_diff=context_diff,
            start=start,
            end=end,
            execution_time=execution_time,
            apply=self.apply,
            restate_models=expanded_restate_models,
            backfill_models=backfill_models,
            no_gaps=no_gaps,
            skip_backfill=skip_backfill,
            is_dev=is_dev,
            forward_only=(
                forward_only if forward_only is not None else self.config.plan.forward_only
            ),
            allow_destructive_models=expanded_destructive_models,
            environment_ttl=environment_ttl,
            environment_suffix_target=self.config.environment_suffix_target,
            environment_catalog_mapping=self.config.environment_catalog_mapping,
            categorizer_config=categorizer_config or self.auto_categorize_changes,
            auto_categorization_enabled=not no_auto_categorization,
            effective_from=effective_from,
            include_unmodified=include_unmodified,
            default_start=default_start,
            default_end=default_end,
            enable_preview=(
                enable_preview if enable_preview is not None else self.config.plan.enable_preview
            ),
            end_bounded=not run,
            ensure_finalized_snapshots=self.config.plan.use_finalized_state,
            engine_schema_differ=self.engine_adapter.SCHEMA_DIFFER,
            interval_end_per_model=max_interval_end_per_model,
            console=self.console,
        )

    def apply(
        self,
        plan: Plan,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
    ) -> None:
        """Applies a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state sync and then uses the scheduler
        to backfill all models.

        Args:
            plan: The plan to apply.
            circuit_breaker: An optional handler which checks if the apply should be aborted.
        """
        if (
            not plan.context_diff.has_changes
            and not plan.requires_backfill
            and not plan.has_unmodified_unpromoted
        ):
            return
        if plan.uncategorized:
            raise UncategorizedPlanError("Can't apply a plan with uncategorized changes.")
        self.notification_target_manager.notify(
            NotificationEvent.APPLY_START,
            environment=plan.environment_naming_info.name,
            plan_id=plan.plan_id,
        )
        try:
            self._apply(plan, circuit_breaker)
        except Exception as e:
            self.notification_target_manager.notify(
                NotificationEvent.APPLY_FAILURE,
                environment=plan.environment_naming_info.name,
                plan_id=plan.plan_id,
                exc=traceback.format_exc(),
            )
            logger.error(f"Apply Failure: {traceback.format_exc()}")
            raise e
        self.notification_target_manager.notify(
            NotificationEvent.APPLY_END,
            environment=plan.environment_naming_info.name,
            plan_id=plan.plan_id,
        )

    @python_api_analytics
    def invalidate_environment(self, name: str, sync: bool = False) -> None:
        """Invalidates the target environment by setting its expiration timestamp to now.

        Args:
            name: The name of the environment to invalidate.
            sync: If True, the call blocks until the environment is deleted. Otherwise, the environment will
                be deleted asynchronously by the janitor process.
        """
        name = Environment.sanitize_name(name)
        self.state_sync.invalidate_environment(name)
        if sync:
            self._cleanup_environments()
            self.console.log_success(f"Environment '{name}' has been deleted.")
        else:
            self.console.log_success(f"Environment '{name}' has been invalidated.")

    @python_api_analytics
    def diff(self, environment: t.Optional[str] = None, detailed: bool = False) -> bool:
        """Show a diff of the current context with a given environment.

        Args:
            environment: The environment to diff against.
            detailed: Show the actual SQL differences if True.

        Returns:
            True if there are changes, False otherwise.
        """
        environment = environment or self.config.default_target_environment
        environment = Environment.sanitize_name(environment)
        context_diff = self._context_diff(environment)
        self.console.show_model_difference_summary(
            context_diff,
            EnvironmentNamingInfo.from_environment_catalog_mapping(
                self.config.environment_catalog_mapping,
                name=environment,
                suffix_target=self.config.environment_suffix_target,
                normalize_name=context_diff.normalize_environment_name,
            ),
            self.default_catalog,
            no_diff=not detailed,
        )
        return context_diff.has_changes

    @python_api_analytics
    def table_diff(
        self,
        source: str,
        target: str,
        on: t.List[str] | exp.Condition | None = None,
        skip_columns: t.List[str] | None = None,
        model_or_snapshot: t.Optional[ModelOrSnapshot] = None,
        where: t.Optional[str | exp.Condition] = None,
        limit: int = 20,
        show: bool = True,
        show_sample: bool = True,
        decimals: int = 3,
        skip_grain_check: bool = False,
    ) -> TableDiff:
        """Show a diff between two tables.

        Args:
            source: The source environment or table.
            target: The target environment or table.
            on: The join condition, table aliases must be "s" and "t" for source and target.
                If omitted, the table's grain will be used.
            skip_columns: The columns to skip when computing the table diff.
            model_or_snapshot: The model or snapshot to use when environments are passed in.
            where: An optional where statement to filter results.
            limit: The limit of the sample dataframe.
            show: Show the table diff output in the console.
            show_sample: Show the sample dataframe in the console. Requires show=True.
            decimals: The number of decimal places to keep when comparing floating point columns.
            skip_grain_check: Skip check for rows that contain null or duplicate grains.

        Returns:
            The TableDiff object containing schema and summary differences.
        """
        source_alias, target_alias = source, target

        if model_or_snapshot:
            model = self.get_model(model_or_snapshot, raise_if_missing=True)
            source_env = self.state_reader.get_environment(source)
            target_env = self.state_reader.get_environment(target)

            if not source_env:
                raise SQLMeshError(f"Could not find environment '{source}'")
            if not target_env:
                raise SQLMeshError(f"Could not find environment '{target}')")

            source = next(
                snapshot for snapshot in source_env.snapshots if snapshot.name == model.fqn
            ).table_name()
            target = next(
                snapshot for snapshot in target_env.snapshots if snapshot.name == model.fqn
            ).table_name()
            source_alias = source_env.name
            target_alias = target_env.name

            if not on:
                for ref in model.all_references:
                    if ref.unique:
                        expr = ref.expression

                        if isinstance(expr, exp.Tuple):
                            on = [key.this.sql() for key in expr.expressions]
                        else:
                            # Handle a single Column or Paren expression
                            on = [expr.this.sql()]

        if not on:
            raise SQLMeshError(
                "SQLMesh doesn't know how to join the two tables. Specify the `grains` in each model definition or pass join column names in separate `-o` flags."
            )

        table_diff = TableDiff(
            adapter=self._engine_adapter,
            source=source,
            target=target,
            on=on,
            skip_columns=skip_columns,
            where=where,
            source_alias=source_alias,
            target_alias=target_alias,
            model_name=model.name if model_or_snapshot else None,
            model_dialect=model.dialect if model_or_snapshot else None,
            limit=limit,
            decimals=decimals,
        )
        if show:
            self.console.show_schema_diff(table_diff.schema_diff())
            self.console.show_row_diff(
                table_diff.row_diff(skip_grain_check=skip_grain_check),
                show_sample=show_sample,
                skip_grain_check=skip_grain_check,
            )
        return table_diff

    @python_api_analytics
    def get_dag(
        self, select_models: t.Optional[t.Collection[str]] = None, **options: t.Any
    ) -> GraphHTML:
        """Gets an HTML object representation of the DAG.

        Args:
            select_models: A list of model selection strings that should be included in the dag.
        Returns:
            An html object that renders the dag.
        """
        dag = (
            self.dag.prune(*self._new_selector().expand_model_selections(select_models))
            if select_models
            else self.dag
        )

        nodes = {}
        edges: t.List[t.Dict] = []

        for node, deps in dag.graph.items():
            nodes[node] = {
                "id": node,
                "label": node.split(".")[-1],
                "title": f"<span>{node}</span>",
            }
            edges.extend({"from": d, "to": node} for d in deps)

        return GraphHTML(
            nodes,
            edges,
            options={
                "height": "100%",
                "width": "100%",
                "interaction": {},
                "layout": {
                    "hierarchical": {
                        "enabled": True,
                        "nodeSpacing": 200,
                        "sortMethod": "directed",
                    },
                },
                "nodes": {
                    "shape": "box",
                },
                **options,
            },
        )

    @python_api_analytics
    def render_dag(self, path: str, select_models: t.Optional[t.Collection[str]] = None) -> None:
        """Render the dag as HTML and save it to a file.

        Args:
            path: filename to save the dag html to
            select_models: A list of model selection strings that should be included in the dag.
        """
        file_path = Path(path)
        suffix = file_path.suffix
        if suffix != ".html":
            if suffix:
                logger.warning(
                    f"The extension {suffix} does not designate an html file. A file with a `.html` extension will be created instead."
                )
            path = str(file_path.with_suffix(".html"))

        with open(path, "w", encoding="utf-8") as file:
            file.write(str(self.get_dag(select_models)))

    @python_api_analytics
    def create_test(
        self,
        model: str,
        input_queries: t.Dict[str, str],
        overwrite: bool = False,
        variables: t.Optional[t.Dict[str, str]] = None,
        path: t.Optional[str] = None,
        name: t.Optional[str] = None,
        include_ctes: bool = False,
    ) -> None:
        """Generate a unit test fixture for a given model.

        Args:
            model: The model to test.
            input_queries: Mapping of model names to queries. Each model included in this mapping
                will be populated in the test based on the results of the corresponding query.
            overwrite: Whether to overwrite the existing test in case of a file path collision.
                When set to False, an error will be raised if there is such a collision.
            variables: Key-value pairs that will define variables needed by the model.
            path: The file path corresponding to the fixture, relative to the test directory.
                By default, the fixture will be created under the test directory and the file name
                will be inferred from the test's name.
            name: The name of the test. This is inferred from the model name by default.
            include_ctes: When true, CTE fixtures will also be generated.
        """
        input_queries = {
            # The get_model here has two purposes: return normalized names & check for missing deps
            self.get_model(dep, raise_if_missing=True).fqn: query
            for dep, query in input_queries.items()
        }

        try:
            test_adapter = self._test_connection_config.create_engine_adapter(
                register_comments_override=False
            )
            generate_test(
                model=self.get_model(model, raise_if_missing=True),
                input_queries=input_queries,
                models=self._models,
                engine_adapter=self._engine_adapter,
                test_engine_adapter=test_adapter,
                project_path=self.path,
                overwrite=overwrite,
                variables=variables,
                path=path,
                name=name,
                include_ctes=include_ctes,
            )
        finally:
            test_adapter.close()

    @python_api_analytics
    def test(
        self,
        match_patterns: t.Optional[t.List[str]] = None,
        tests: t.Optional[t.List[str]] = None,
        verbose: bool = False,
        preserve_fixtures: bool = False,
        stream: t.Optional[t.TextIO] = None,
    ) -> ModelTextTestResult:
        """Discover and run model tests"""
        if verbose:
            pd.set_option("display.max_columns", None)
            verbosity = 2
        else:
            verbosity = 1

        if tests:
            result = run_model_tests(
                tests=tests,
                models=self._models,
                config=self.config,
                gateway=self.gateway,
                dialect=self.default_dialect,
                verbosity=verbosity,
                patterns=match_patterns,
                preserve_fixtures=preserve_fixtures,
                stream=stream,
                default_catalog=self.default_catalog,
                default_catalog_dialect=self.engine_adapter.DIALECT,
            )
        else:
            test_meta = []

            for path, config in self.configs.items():
                test_meta.extend(
                    get_all_model_tests(
                        path / c.TESTS,
                        patterns=match_patterns,
                        ignore_patterns=config.ignore_patterns,
                        variables=config.variables,
                    )
                )

            result = run_tests(
                model_test_metadata=test_meta,
                models=self._models,
                config=self.config,
                gateway=self.gateway,
                dialect=self.default_dialect,
                verbosity=verbosity,
                preserve_fixtures=preserve_fixtures,
                stream=stream,
                default_catalog=self.default_catalog,
                default_catalog_dialect=self.engine_adapter.DIALECT,
            )

        return result

    @python_api_analytics
    def audit(
        self,
        start: TimeLike,
        end: TimeLike,
        *,
        models: t.Optional[t.Iterator[str]] = None,
        execution_time: t.Optional[TimeLike] = None,
    ) -> None:
        """Audit models.

        Args:
            start: The start of the interval to audit.
            end: The end of the interval to audit.
            models: The models to audit. All models will be audited if not specified.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
        """

        snapshots = (
            [self.get_snapshot(model, raise_if_missing=True) for model in models]
            if models
            else self.snapshots.values()
        )

        num_audits = sum(len(snapshot.audits_with_args) for snapshot in snapshots)
        self.console.log_status_update(f"Found {num_audits} audit(s).")
        errors = []
        skipped_count = 0
        for snapshot in snapshots:
            for audit_result in self.snapshot_evaluator.audit(
                snapshot=snapshot,
                start=start,
                end=end,
                snapshots=self.snapshots,
                raise_exception=False,
            ):
                audit_id = f"{audit_result.audit.name}"
                if audit_result.model:
                    audit_id += f" on model {audit_result.model.name}"

                if audit_result.skipped:
                    self.console.log_status_update(f"{audit_id} ⏸️ SKIPPED.")
                    skipped_count += 1
                elif audit_result.count:
                    errors.append(audit_result)
                    self.console.log_status_update(
                        f"{audit_id} ❌ [red]FAIL [{audit_result.count}][/red]."
                    )
                else:
                    self.console.log_status_update(f"{audit_id} ✅ [green]PASS[/green].")

        self.console.log_status_update(
            f"\nFinished with {len(errors)} audit error{'' if len(errors) == 1 else 's'} "
            f"and {skipped_count} audit{'' if skipped_count == 1 else 's'} skipped."
        )
        for error in errors:
            self.console.log_status_update(
                f"\nFailure in audit {error.audit.name} ({error.audit._path})."
            )
            self.console.log_status_update(f"Got {error.count} results, expected 0.")
            if error.query:
                self.console.show_sql(
                    f"{error.query.sql(dialect=self.snapshot_evaluator.adapter.dialect)}"
                )

        self.console.log_status_update("Done.")

    @python_api_analytics
    def rewrite(self, sql: str, dialect: str = "") -> exp.Expression:
        """Rewrite a sql expression with semantic references into an executable query.

        https://sqlmesh.readthedocs.io/en/latest/concepts/metrics/overview/

        Args:
            sql: The sql string to rewrite.
            dialect: The dialect of the sql string, defaults to the project dialect.

        Returns:
            A SQLGlot expression with semantic references expanded.
        """
        return rewrite(
            sql,
            graph=ReferenceGraph(self.models.values()),
            metrics=self._metrics,
            dialect=dialect or self.default_dialect,
        )

    @python_api_analytics
    def migrate(self) -> None:
        """Migrates SQLMesh to the current running version.

        Please contact your SQLMesh administrator before doing this.
        """
        self.notification_target_manager.notify(NotificationEvent.MIGRATION_START)
        try:
            self._new_state_sync().migrate(
                default_catalog=self.default_catalog,
                promoted_snapshots_only=self.config.migration.promoted_snapshots_only,
            )
        except Exception as e:
            self.notification_target_manager.notify(
                NotificationEvent.MIGRATION_FAILURE, traceback.format_exc()
            )
            raise e
        self.notification_target_manager.notify(NotificationEvent.MIGRATION_END)

    @python_api_analytics
    def rollback(self) -> None:
        """Rolls back SQLMesh to the previous migration.

        Please contact your SQLMesh administrator before doing this. This action cannot be undone.
        """
        self._new_state_sync().rollback()

    @python_api_analytics
    def create_external_models(self, strict: bool = False) -> None:
        """Create a file to document the schema of external models.

        The external models file contains all columns and types of external models, allowing for more
        robust lineage, validation, and optimizations.

        Args:
            strict: If True, raise an error if the external model is missing in the database.
        """
        if not self._models:
            self.load(update_schemas=False)

        for path, config in self.configs.items():
            deprecated_yaml = path / c.EXTERNAL_MODELS_DEPRECATED_YAML

            external_models_yaml = (
                path / c.EXTERNAL_MODELS_YAML if not deprecated_yaml.exists() else deprecated_yaml
            )
            create_external_models_file(
                path=external_models_yaml,
                models=UniqueKeyDict(
                    "models",
                    {
                        fqn: model
                        for fqn, model in self._models.items()
                        if self.config_for_node(model) is config
                    },
                ),
                adapter=self._engine_adapter,
                state_reader=self.state_reader,
                dialect=config.model_defaults.dialect,
                gateway=self.gateway,
                max_workers=self.concurrent_tasks,
                strict=strict,
            )

    @python_api_analytics
    def print_info(self, skip_connection: bool = False) -> None:
        """Prints information about connections, models, macros, etc. to the console."""
        self.console.log_status_update(f"Models: {len(self.models)}")
        self.console.log_status_update(f"Macros: {len(self._macros) - len(macro.get_registry())}")

        if skip_connection:
            return

        self._try_connection("data warehouse", self._engine_adapter.ping)

        state_connection = self.config.get_state_connection(self.gateway)
        if state_connection:
            self._try_connection("state backend", state_connection.connection_validator())

    def close(self) -> None:
        """Releases all resources allocated by this context."""
        if self._snapshot_evaluator:
            self._snapshot_evaluator.close()
        if self._state_sync:
            self._state_sync.close()

    def _run(
        self,
        environment: str,
        *,
        start: t.Optional[TimeLike],
        end: t.Optional[TimeLike],
        execution_time: t.Optional[TimeLike],
        skip_janitor: bool,
        ignore_cron: bool,
    ) -> bool:
        if not skip_janitor and environment.lower() == c.PROD:
            self._run_janitor()

        env_check_attempts_num = max(
            1,
            self.config.run.environment_check_max_wait
            // self.config.run.environment_check_interval,
        )

        def _block_until_finalized() -> str:
            for _ in range(env_check_attempts_num):
                assert environment is not None  # mypy
                environment_state = self.state_sync.get_environment(environment)
                if not environment_state:
                    raise SQLMeshError(f"Environment '{environment}' was not found.")
                if environment_state.finalized_ts:
                    return environment_state.plan_id
                logger.warning(
                    "Environment '%s' is being updated by plan '%s'. Retrying in %s seconds...",
                    environment,
                    environment_state.plan_id,
                    self.config.run.environment_check_interval,
                )
                time.sleep(self.config.run.environment_check_interval)
            raise SQLMeshError(
                f"Exceeded the maximum wait time for environment '{environment}' to be ready. "
                "This means that the environment either failed to update or the update is taking longer than expected. "
                "See https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#run to adjust the timeout settings."
            )

        done = False
        while not done:
            plan_id_at_start = _block_until_finalized()

            def _has_environment_changed() -> bool:
                assert environment is not None  # mypy
                current_environment_state = self.state_sync.get_environment(environment)
                return (
                    not current_environment_state
                    or current_environment_state.plan_id != plan_id_at_start
                    or not current_environment_state.finalized_ts
                )

            try:
                success = self.scheduler(environment=environment).run(
                    environment,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    ignore_cron=ignore_cron,
                    circuit_breaker=_has_environment_changed,
                )
                done = True
            except CircuitBreakerError:
                logger.warning(
                    "Environment '%s' has been modified while running. Restarting the run...",
                    environment,
                )

        return success

    def _apply(self, plan: Plan, circuit_breaker: t.Optional[t.Callable[[], bool]]) -> None:
        self._scheduler.create_plan_evaluator(self).evaluate(
            plan.to_evaluatable(), circuit_breaker=circuit_breaker
        )

    @python_api_analytics
    def table_name(self, model_name: str, dev: bool) -> str:
        """Returns the name of the pysical table for the given model name.

        Args:
            model_name: The name of the model.
            dev: Whether to use the deployability index for the table name.

        Returns:
            The name of the physical table.
        """
        deployability_index = (
            DeployabilityIndex.create(self.snapshots.values())
            if dev
            else DeployabilityIndex.all_deployable()
        )
        snapshot = self.get_snapshot(model_name)
        if not snapshot:
            raise SQLMeshError(f"Model '{model_name}' was not found.")
        return snapshot.table_name(is_deployable=deployability_index.is_deployable(snapshot))

    def clear_caches(self) -> None:
        for path in self.configs:
            rmtree(path / c.CACHE)

    def _run_tests(self, verbose: bool = False) -> t.Tuple[unittest.result.TestResult, str]:
        test_output_io = StringIO()
        result = self.test(stream=test_output_io, verbose=verbose)
        return result, test_output_io.getvalue()

    def _run_plan_tests(
        self, skip_tests: bool = False
    ) -> t.Tuple[t.Optional[unittest.result.TestResult], t.Optional[str]]:
        if not skip_tests:
            result, test_output = self._run_tests()
            if result.testsRun > 0:
                self.console.log_test_results(
                    result, test_output, self._test_connection_config._engine_adapter.DIALECT
                )
            if not result.wasSuccessful():
                raise PlanError(
                    "Cannot generate plan due to failing test(s). Fix test(s) and run again"
                )
            return result, test_output
        return None, None

    @property
    def _model_tables(self) -> t.Dict[str, str]:
        """Mapping of model name to physical table name.

        If a snapshot has not been versioned yet, its view name will be returned.
        """
        return {
            fqn: (
                snapshot.table_name()
                if snapshot.version
                else snapshot.qualified_view_name.for_environment(
                    EnvironmentNamingInfo.from_environment_catalog_mapping(
                        self.config.environment_catalog_mapping,
                        name=c.PROD,
                        suffix_target=self.config.environment_suffix_target,
                    )
                )
            )
            for fqn, snapshot in self.snapshots.items()
        }

    def _snapshots(
        self, models_override: t.Optional[UniqueKeyDict[str, Model]] = None
    ) -> t.Dict[str, Snapshot]:
        projects = {config.project for config in self.configs.values()}

        if any(projects):
            prod = self.state_reader.get_environment(c.PROD)
            remote_snapshots = (
                {
                    snapshot.name: snapshot
                    for snapshot in self.state_reader.get_snapshots(prod.snapshots).values()
                }
                if prod
                else {}
            )
        else:
            remote_snapshots = {}

        local_nodes = {**(models_override or self._models), **self._standalone_audits}
        nodes = local_nodes.copy()
        audits = self._audits.copy()

        for name, snapshot in remote_snapshots.items():
            if name not in nodes and snapshot.node.project not in projects:
                nodes[name] = snapshot.node
                if snapshot.is_model:
                    for audit in snapshot.audits:
                        if name not in audits:
                            audits[name] = audit

        def _nodes_to_snapshots(nodes: t.Dict[str, Node]) -> t.Dict[str, Snapshot]:
            snapshots: t.Dict[str, Snapshot] = {}
            fingerprint_cache: t.Dict[str, SnapshotFingerprint] = {}

            for node in nodes.values():
                if node.fqn not in local_nodes and node.fqn in remote_snapshots:
                    ttl = remote_snapshots[node.fqn].ttl
                else:
                    config = self.config_for_node(node)
                    ttl = config.snapshot_ttl

                snapshot = Snapshot.from_node(
                    node,
                    nodes=nodes,
                    audits=audits,
                    cache=fingerprint_cache,
                    ttl=ttl,
                    config=self.config_for_node(node),
                )
                snapshots[snapshot.name] = snapshot
            return snapshots

        snapshots = _nodes_to_snapshots(nodes)
        stored_snapshots = self.state_reader.get_snapshots(snapshots.values())

        unrestorable_snapshots = {
            snapshot
            for snapshot in stored_snapshots.values()
            if snapshot.name in local_nodes and snapshot.unrestorable
        }
        if unrestorable_snapshots:
            for snapshot in unrestorable_snapshots:
                logger.info(
                    "Found a unrestorable snapshot %s. Restamping the model...", snapshot.name
                )
                node = local_nodes[snapshot.name]
                nodes[snapshot.name] = node.copy(
                    update={"stamp": f"revert to {snapshot.identifier}"}
                )
            snapshots = _nodes_to_snapshots(nodes)
            stored_snapshots = self.state_reader.get_snapshots(snapshots.values())

        for snapshot in stored_snapshots.values():
            # Keep the original model instance to preserve the query cache.
            snapshot.node = snapshots[snapshot.name].node

        return {name: stored_snapshots.get(s.snapshot_id, s) for name, s in snapshots.items()}

    def _context_diff(
        self,
        environment: str,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        create_from: t.Optional[str] = None,
        force_no_diff: bool = False,
        ensure_finalized_snapshots: bool = False,
    ) -> ContextDiff:
        environment = Environment.sanitize_name(environment)
        if force_no_diff:
            return ContextDiff.create_no_diff(environment, self.state_reader)

        return ContextDiff.create(
            environment,
            snapshots=snapshots or self.snapshots,
            create_from=create_from or c.PROD,
            state_reader=self.state_reader,
            ensure_finalized_snapshots=ensure_finalized_snapshots,
        )

    def _run_janitor(self, ignore_ttl: bool = False) -> None:
        self._cleanup_environments()
        expired_snapshots = self.state_sync.delete_expired_snapshots(ignore_ttl=ignore_ttl)
        self.snapshot_evaluator.cleanup(
            expired_snapshots, on_complete=self.console.update_cleanup_progress
        )

        self.state_sync.compact_intervals()

    def _cleanup_environments(self) -> None:
        expired_environments = self.state_sync.delete_expired_environments()
        cleanup_expired_views(self.engine_adapter, expired_environments, console=self.console)

    def _try_connection(self, connection_name: str, validator: t.Callable[[], None]) -> None:
        connection_name = connection_name.capitalize()
        try:
            validator()
            self.console.log_status_update(f"{connection_name} connection [green]succeeded[/green]")
        except Exception as ex:
            self.console.log_error(f"{connection_name} connection failed. {ex}")

    def _new_state_sync(self) -> StateSync:
        return self._provided_state_sync or self._scheduler.create_state_sync(self)

    def _new_selector(self) -> Selector:
        return Selector(
            self.state_reader,
            models=self._models,
            audits=self._audits,
            context_path=self.path,
            default_catalog=self.default_catalog,
            dialect=self.default_dialect,
        )

    def _register_notification_targets(self) -> None:
        event_notifications = collections.defaultdict(set)
        for target in self.notification_targets:
            if target.is_configured:
                for event in target.notify_on:
                    event_notifications[event].add(target)
        user_notification_targets = {
            user.username: set(
                target for target in user.notification_targets if target.is_configured
            )
            for user in self.users
        }
        self.notification_target_manager = NotificationTargetManager(
            event_notifications, user_notification_targets, username=self.config.username
        )


class Context(GenericContext[Config]):
    CONFIG_TYPE = Config
