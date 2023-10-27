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
context = Context(path="example", config="local_config")
plan = context.plan("staging")
context.apply(plan)
```

Running audits on your data.
```python
from sqlmesh.core.context import Context
context = Context(path="example", config="local_config")
context.audit("yesterday", "now")
```

Running tests on your models.
```python
from sqlmesh.core.context import Context
context = Context(path="example")
context.test()
```
"""
from __future__ import annotations

import abc
import collections
import gc
import logging
import time
import traceback
import typing as t
import unittest.result
from datetime import timedelta
from io import StringIO
from pathlib import Path
from types import MappingProxyType

import pandas as pd
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.audit import Audit, StandaloneAudit
from sqlmesh.core.config import (
    CategorizerConfig,
    Config,
    load_config_from_paths,
    load_config_from_yaml,
)
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.dialect import (
    format_model_expressions,
    normalize_model_name,
    pandas_to_sql,
    parse,
)
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.loader import Loader, update_model_schemas
from sqlmesh.core.macros import ExecutableOrMacro
from sqlmesh.core.metric import Metric, rewrite
from sqlmesh.core.model import Model
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTarget,
    NotificationTargetManager,
)
from sqlmesh.core.plan import Plan
from sqlmesh.core.reference import ReferenceGraph
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.schema_loader import create_schema_file
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
from sqlmesh.core.test import get_all_model_tests, run_model_tests, run_tests
from sqlmesh.core.user import User
from sqlmesh.utils import UniqueKeyDict, env_vars, sys_path
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now_ds, to_date
from sqlmesh.utils.errors import (
    CircuitBreakerError,
    ConfigError,
    MissingDependencyError,
    PlanError,
    SQLMeshError,
    UncategorizedPlanError,
)
from sqlmesh.utils.jinja import JinjaMacroRegistry

if t.TYPE_CHECKING:
    import graphviz
    from typing_extensions import Literal

    from sqlmesh.core.engine_adapter._typing import DF, PySparkDataFrame, PySparkSession
    from sqlmesh.core.snapshot import Node

    ModelOrSnapshot = t.Union[str, Model, Snapshot]
    NodeOrSnapshot = t.Union[str, Model, StandaloneAudit, Snapshot]


logger = logging.getLogger(__name__)


class BaseContext(abc.ABC):
    """The base context which defines methods to execute a model."""

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

    def table(self, model_name: str) -> str:
        """Gets the physical table name for a given model.

        Args:
            model_name: The model name.

        Returns:
            The physical table name.
        """
        return self._model_tables[model_name]

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
        deployability_index: t.Optional[DeployabilityIndex],
    ):
        self.snapshots = snapshots
        self.deployability_index = deployability_index
        self._engine_adapter = engine_adapter
        self.__model_tables = to_table_mapping(snapshots.values(), deployability_index)

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    @property
    def _model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""
        return self.__model_tables


class Context(BaseContext):
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

    def __init__(
        self,
        engine_adapter: t.Optional[EngineAdapter] = None,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        state_sync: t.Optional[StateSync] = None,
        paths: t.Union[str, t.Iterable[str]] = "",
        config: t.Optional[t.Union[Config, str]] = None,
        gateway: t.Optional[str] = None,
        concurrent_tasks: t.Optional[int] = None,
        loader: t.Optional[t.Type[Loader]] = None,
        load: bool = True,
        console: t.Optional[Console] = None,
        users: t.Optional[t.List[User]] = None,
    ):
        self.console = console or get_console()

        self.sqlmesh_path = Path.home() / ".sqlmesh"

        self.configs = self._load_configs(
            config or "config",
            [
                Path(path).absolute()
                for path in ([paths] if isinstance(paths, str) else list(paths))
            ],
        )

        self.dag: DAG[str] = DAG()
        self._models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        self._audits: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        self._standalone_audits: UniqueKeyDict[str, StandaloneAudit] = UniqueKeyDict(
            "standaloneaudits"
        )
        self._macros: UniqueKeyDict[str, ExecutableOrMacro] = UniqueKeyDict("macros")
        self._metrics: UniqueKeyDict[str, Metric] = UniqueKeyDict("metrics")
        self._jinja_macros = JinjaMacroRegistry()

        self.path, self.config = t.cast(t.Tuple[Path, Config], next(iter(self.configs.items())))

        self.gateway = gateway
        self._scheduler = self.config.get_scheduler(self.gateway)
        self.environment_ttl = self.config.environment_ttl
        self.pinned_environments = Environment.normalize_names(self.config.pinned_environments)
        self.auto_categorize_changes = self.config.auto_categorize_changes

        connection_config = self.config.get_connection(self.gateway)
        self.concurrent_tasks = concurrent_tasks or connection_config.concurrent_tasks
        self._engine_adapter = engine_adapter or connection_config.create_engine_adapter()

        test_connection_config = self.config.get_test_connection(self.gateway)
        self._test_engine_adapter = test_connection_config.create_engine_adapter()

        self._snapshot_evaluator: t.Optional[SnapshotEvaluator] = None

        self._provided_state_sync: t.Optional[StateSync] = state_sync
        self._state_sync: t.Optional[StateSync] = None

        self._loader = (loader or self.config.loader)()

        # Should we dedupe notification_targets? If so how?
        self.notification_targets = (notification_targets or []) + self.config.notification_targets
        self.users = (users or []) + self.config.users
        self.users = list({user.username: user for user in self.users}.values())
        self._register_notification_targets()

        if load:
            self.load()

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    @property
    def snapshot_evaluator(self) -> SnapshotEvaluator:
        if not self._snapshot_evaluator:
            self._snapshot_evaluator = SnapshotEvaluator(
                self.engine_adapter,
                ddl_concurrent_tasks=self.concurrent_tasks,
                console=self.console,
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
        )

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
        path = model._path

        # model.copy() can't be used here due to a cached state that can be a part of a model instance.
        model = t.cast(Model, type(model)(**{**t.cast(Model, model).dict(), **kwargs}))
        model._path = path

        self._models.update({model.name: model})
        self.dag.add(model.name, model.depends_on)
        update_model_schemas(
            self.dag,
            self._models,
            self.path,
            {model.name: self.config_for_node(model).model_defaults},
        )

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

        return Scheduler(
            snapshots,
            self.snapshot_evaluator,
            self.state_sync,
            max_workers=self.concurrent_tasks,
            console=self.console,
            notification_target_manager=self.notification_target_manager,
        )

    @property
    def state_sync(self) -> StateSync:
        if not self._state_sync:
            self._state_sync = self._new_state_sync()

            if self._state_sync.get_versions(validate=False).schema_version == 0:
                self._state_sync.migrate()
            self._state_sync.get_versions()
            self._state_sync = CachingStateSync(self._state_sync)  # type: ignore
        return self._state_sync

    @property
    def state_reader(self) -> StateReader:
        return self.state_sync

    def refresh(self) -> None:
        """Refresh all models that have been updated."""
        if self._loader.reload_needed():
            self.load()

    def load(self, update_schemas: bool = True) -> Context:
        """Load all files in the context's path."""
        with sys_path(*self.configs):
            gc.disable()
            project = self._loader.load(self, update_schemas)
            self._macros = project.macros
            self._jinja_macros = project.jinja_macros
            self._models = project.models
            self._metrics = project.metrics
            self._standalone_audits.clear()
            self._audits.clear()
            for name, audit in project.audits.items():
                if isinstance(audit, StandaloneAudit):
                    self._standalone_audits[name] = audit
                else:
                    self._audits[name] = audit
            self.dag = project.dag
            gc.enable()

            duplicates = set(self._models) & set(self._standalone_audits)
            if duplicates:
                raise ConfigError(
                    f"Models and Standalone audits cannot have the same name: {duplicates}"
                )

        return self

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

        if not skip_janitor and environment.lower() == c.PROD:
            self._run_janitor()

        self.notification_target_manager.notify(
            NotificationEvent.RUN_START, environment=environment
        )

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
            except Exception as e:
                self.notification_target_manager.notify(
                    NotificationEvent.RUN_FAILURE, traceback.format_exc()
                )
                raise e

        if success:
            self.notification_target_manager.notify(
                NotificationEvent.RUN_END, environment=environment
            )
        else:
            self.notification_target_manager.notify(
                NotificationEvent.RUN_FAILURE, environment=environment
            )
            return success

        return success

    @t.overload
    def get_model(
        self, model_or_snapshot: ModelOrSnapshot, raise_if_missing: Literal[True] = True
    ) -> Model:
        ...

    @t.overload
    def get_model(
        self, model_or_snapshot: ModelOrSnapshot, raise_if_missing: Literal[False] = False
    ) -> t.Optional[Model]:
        ...

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
            normalized_name = normalize_model_name(model_or_snapshot, dialect=self.config.dialect)
            model = self._models.get(normalized_name)
        elif isinstance(model_or_snapshot, Snapshot):
            model = model_or_snapshot.model
        else:
            model = model_or_snapshot

        if raise_if_missing and not model:
            raise SQLMeshError(f"Cannot find model for '{model_or_snapshot}'")

        return model

    @t.overload
    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: Literal[True]
    ) -> Snapshot:
        ...

    @t.overload
    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: Literal[False]
    ) -> t.Optional[Snapshot]:
        ...

    def get_snapshot(
        self, node_or_snapshot: NodeOrSnapshot, raise_if_missing: bool = False
    ) -> t.Optional[Snapshot]:
        """Returns a snapshot with the given name or None if a snapshot with such name doesn't exist.

        Args:
            model_or_snapshot: A model name, model, or snapshot.
            raise_if_missing: Raises an error if a snapshot is not found.

        Returns:
            The expected snapshot.
        """
        if isinstance(node_or_snapshot, str):
            normalized_name = normalize_model_name(node_or_snapshot, dialect=self.config.dialect)
            snapshot = self.snapshots.get(normalized_name)
        elif isinstance(node_or_snapshot, Snapshot):
            snapshot = node_or_snapshot
        else:
            snapshot = self.snapshots.get(node_or_snapshot.name)

        if raise_if_missing and not snapshot:
            raise SQLMeshError(f"Cannot find snapshot for '{node_or_snapshot}'")

        return snapshot

    def config_for_path(self, path: Path) -> Config:
        for config_path, config in self.configs.items():
            try:
                path.relative_to(config_path)
                return config
            except ValueError:
                pass
        return self.config

    def config_for_node(self, node: str | Model | StandaloneAudit) -> Config:
        return self.config_for_path(
            (
                self._models.get(node, None) or self._standalone_audits.get(node, None)
                if isinstance(node, str)
                else node
            )._path
        )

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

        If one of the snapshots has been previosly stored in the persisted state, the stored
        instance will be returned.
        """
        return self._snapshots()

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

        expand = self.dag.upstream(model.name) if expand is True else expand or []

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

        return model.render_query_or_raise(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=self.snapshots,
            expand=expand,
            engine_adapter=self.engine_adapter,
            **kwargs,
        )

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

        df = self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            execution_time,
            snapshots=self.snapshots,
            limit=limit or c.DEFAULT_MAX_LIMIT,
        )

        if df is None:
            raise RuntimeError(f"Error evaluating {snapshot.name}")

        return df

    def format(self, transpile: t.Optional[str] = None, newline: bool = False) -> None:
        """Format all models in a given directory."""
        for model in self._models.values():
            if not model.is_sql:
                continue
            with open(model._path, "r+", encoding="utf-8") as file:
                expressions = parse(
                    file.read(), default_dialect=self.config_for_node(model).dialect
                )
                if transpile:
                    for prop in expressions[0].expressions:
                        if prop.name.lower() == "dialect":
                            prop.replace(
                                exp.Property(
                                    this="dialect",
                                    value=exp.Literal.string(transpile or model.dialect),
                                )
                            )
                file.seek(0)
                file.write(format_model_expressions(expressions, transpile or model.dialect))
                if newline:
                    file.write("\n")
                file.truncate()

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
        forward_only: bool = False,
        no_prompts: bool = False,
        auto_apply: bool = False,
        no_auto_categorization: t.Optional[bool] = None,
        effective_from: t.Optional[TimeLike] = None,
        include_unmodified: t.Optional[bool] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        categorizer_config: t.Optional[CategorizerConfig] = None,
    ) -> Plan:
        """Interactively create a migration plan.

        This method compares the current context with an environment. It then presents
        the differences and asks whether to backfill each modified model.

        Args:
            environment: The environment to diff and plan against.
            start: The start date of the backfill if there is one.
            end: The end date of the backfill if there is one.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            create_from: The environment to create the target environment from if it
                doesn't exist. If not specified, the "prod" environment will be used.
            skip_tests: Unit tests are run by default so this will skip them if enabled
            restate_models: A list of of either internal or external models that need to be restated
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

        Returns:
            The populated Plan object.
        """
        environment = environment or self.config.default_target_environment
        environment = Environment.normalize_name(environment)
        is_dev = environment != c.PROD

        if skip_backfill and not no_gaps and environment == c.PROD:
            raise ConfigError(
                "When targeting the production enviornment either the backfill should not be skipped or the lack of data gaps should be enforced (--no-gaps flag)."
            )

        self._run_plan_tests(skip_tests=skip_tests)

        environment_ttl = (
            self.environment_ttl if environment not in self.pinned_environments else None
        )

        if include_unmodified is None:
            include_unmodified = self.config.include_unmodified

        model_selector = Selector(
            self.state_reader,
            self._models,
            {
                model.name: self.config_for_node(model).model_defaults
                for model in self.models.values()
            },
            self.path,
        )

        models_override: t.Optional[UniqueKeyDict[str, Model]] = None
        if select_models:
            models_override = model_selector.select_models(
                select_models, environment, fallback_env_name=create_from or c.PROD
            )

        if restate_models is not None:
            restate_models = model_selector.expand_model_selections(restate_models)

        # If no end date is specified, use the max interval end from prod
        # to prevent unintended evaluation of the entire DAG.
        default_end = self.state_sync.max_interval_end_for_environment(c.PROD) if is_dev else None
        default_start = to_date(default_end) - timedelta(days=1) if default_end else None

        plan = Plan(
            context_diff=self._context_diff(
                environment or c.PROD,
                snapshots=self._snapshots(models_override),
                create_from=create_from,
            ),
            start=start,
            end=end,
            execution_time=execution_time,
            apply=self.apply,
            restate_models=restate_models,
            no_gaps=no_gaps,
            skip_backfill=skip_backfill,
            is_dev=is_dev,
            forward_only=forward_only,
            environment_ttl=environment_ttl,
            environment_suffix_target=self.config.environment_suffix_target,
            categorizer_config=categorizer_config or self.auto_categorize_changes,
            auto_categorization_enabled=not no_auto_categorization,
            effective_from=effective_from,
            include_unmodified=include_unmodified,
            default_start=default_start,
            default_end=default_end,
        )

        if not no_prompts:
            self.console.plan(plan, auto_apply)
        elif auto_apply:
            self.apply(plan)

        return plan

    def apply(self, plan: Plan) -> None:
        """Applies a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state sync and then uses the scheduler
        to backfill all models.

        Args:
            plan: The plan to apply.
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
            NotificationEvent.APPLY_START, environment=plan.environment.name
        )
        try:
            self._scheduler.create_plan_evaluator(self).evaluate(plan)
        except Exception as e:
            self.notification_target_manager.notify(
                NotificationEvent.APPLY_FAILURE, traceback.format_exc()
            )
            raise e
        self.notification_target_manager.notify(
            NotificationEvent.APPLY_END, environment=plan.environment.name
        )

    def invalidate_environment(self, name: str) -> None:
        """Invalidates the target environment by setting its expiration timestamp to now.

        Args:
            name: The name of the environment to invalidate.
        """
        self.state_sync.invalidate_environment(name)
        self.console.log_success(f"Environment '{name}' has been invalidated.")

    def diff(self, environment: t.Optional[str] = None, detailed: bool = False) -> None:
        """Show a diff of the current context with a given environment.

        Args:
            environment: The environment to diff against.
            detailed: Show the actual SQL differences if True.
        """
        environment = environment or self.config.default_target_environment
        environment = Environment.normalize_name(environment)
        self.console.show_model_difference_summary(self._context_diff(environment), detailed)

    def table_diff(
        self,
        source: str,
        target: str,
        on: t.List[str] | exp.Condition | None = None,
        model_or_snapshot: t.Optional[ModelOrSnapshot] = None,
        where: t.Optional[str | exp.Condition] = None,
        limit: int = 20,
        show: bool = True,
        show_sample: bool = True,
    ) -> TableDiff:
        """Show a diff between two tables.

        Args:
            source: The source environment or table.
            target: The target environment or table.
            on: The join condition, table aliases must be "s" and "t" for source and target.
                If omitted, the table's grain will be used.
            model_or_snapshot: The model or snapshot to use when environments are passed in.
            where: An optional where statement to filter results.
            limit: The limit of the sample dataframe.
            show: Show the table diff output in the console.
            show_sample: Show the sample dataframe in the console. Requires show=True.

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
                snapshot for snapshot in source_env.snapshots if snapshot.name == model.name
            ).table_name()
            target = next(
                snapshot for snapshot in target_env.snapshots if snapshot.name == model.name
            ).table_name()
            source_alias = source_env.name
            target_alias = target_env.name

            if not on:
                for ref in model.all_references:
                    if ref.unique:
                        on = ref.columns

        if not on:
            raise SQLMeshError("Missing join condition 'on'")

        table_diff = TableDiff(
            adapter=self._engine_adapter,
            source=source,
            target=target,
            on=on,
            where=where,
            source_alias=source_alias,
            target_alias=target_alias,
            model_name=model.name if model_or_snapshot else None,
            limit=limit,
        )
        if show:
            self.console.show_schema_diff(table_diff.schema_diff())
            self.console.show_row_diff(table_diff.row_diff(), show_sample=show_sample)
        return table_diff

    def get_dag(self, format: str = "svg") -> graphviz.Digraph:
        """Gets a graphviz dag.

        This method requires installing the graphviz base library through your package manager
        and the python graphviz library.

        To display within Databricks:
        displayHTML(context.get_dag().pipe(encoding='utf-8'))

        Args:
            format: The desired format to use for representing the graph
        """
        from sqlmesh import runtime_env

        try:
            import graphviz  # type: ignore
        except ModuleNotFoundError as e:
            if runtime_env.is_databricks:
                raise MissingDependencyError(
                    "Rendering a dag requires graphviz. Run `pip install graphviz` and then `sudo apt-get install -y python3-dev graphviz libgraphviz-dev pkg-config`"
                )
            raise MissingDependencyError(
                "Rendering a dag requires a manual install of graphviz. Run `pip install graphviz` and then install graphviz library: https://graphviz.org/download/."
            ) from e

        graph = graphviz.Digraph(node_attr={"shape": "box"}, format=format)

        for name, upstream in self.dag.graph.items():
            graph.node(name)
            for u in upstream:
                graph.edge(u, name)
        return graph

    def render_dag(self, path: str, format: str = "jpeg") -> str:
        """Render the dag using graphviz.

        This method requires installing the graphviz base library through your package manager
        and the python graphviz library.

        Args:
            path: filename to save the dag to
            format: The desired format to use when rending the dag
        """
        graph = self.get_dag(format=format)
        # We know graphviz is installed because the command above would have failed if it was not. This allows
        # us to then catch the specific error that occurs when the system install is missing.
        import graphviz  # type: ignore

        try:
            return graph.render(path, format=format)
        except graphviz.backend.execute.ExecutableNotFound as e:
            raise MissingDependencyError(
                "Graphviz is pip-installed but the system install is missing. Instructions: https://graphviz.org/download/"
            ) from e

    def test(
        self,
        match_patterns: t.Optional[t.List[str]] = None,
        tests: t.Optional[t.List[str]] = None,
        verbose: bool = False,
        stream: t.Optional[t.TextIO] = None,
    ) -> unittest.result.TestResult:
        """Discover and run model tests"""
        verbosity = 2 if verbose else 1

        try:
            if tests:
                result = run_model_tests(
                    tests=tests,
                    models=self._models,
                    engine_adapter=self._test_engine_adapter,
                    dialect=self.config.dialect,
                    verbosity=verbosity,
                    patterns=match_patterns,
                )
            else:
                test_meta = []

                for path, config in self.configs.items():
                    test_meta.extend(
                        get_all_model_tests(
                            path / c.TESTS,
                            patterns=match_patterns,
                            ignore_patterns=config.ignore_patterns,
                        )
                    )

                result = run_tests(
                    test_meta,
                    models=self._models,
                    engine_adapter=self._test_engine_adapter,
                    dialect=self.config.dialect,
                    verbosity=verbosity,
                    stream=stream,
                )
        finally:
            self._test_engine_adapter.close()

        return result

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
            self.console.show_sql(f"{error.query}")

        self.console.log_status_update("Done.")

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
            dialect=dialect or self.config.dialect,
        )

    def migrate(self) -> None:
        """Migrates SQLMesh to the current running version.

        Please contact your SQLMesh administrator before doing this.
        """
        self._new_state_sync().migrate()

    def rollback(self) -> None:
        """Rolls back SQLMesh to the previous migration.

        Please contact your SQLMesh administrator before doing this. This action cannot be undone.
        """
        self._new_state_sync().rollback()

    def create_external_models(self) -> None:
        """Create a schema file with all external models.

        The schema file contains all columns and types of external models, allowing for more robust
        lineage, validation, and optimizations.
        """
        if not self._models:
            self.load(update_schemas=False)

        for path, config in self.configs.items():
            create_schema_file(
                path=path / c.SCHEMA_YAML,
                models={
                    name: model
                    for name, model in self._models.items()
                    if self.config_for_node(model) is config
                },
                adapter=self._engine_adapter,
                state_reader=self.state_reader,
                dialect=config.model_defaults.dialect,
                max_workers=self.concurrent_tasks,
            )

    def print_info(self) -> None:
        """Prints information about connections, models, macros, etc. to the console."""
        self.console.log_status_update(f"Models: {len(self.models)}")
        self.console.log_status_update(f"Macros: {len(self._macros)}")

        self._try_connection("data warehouse", self._engine_adapter)

        state_connection = self.config.get_state_connection(self.gateway)
        if state_connection:
            self._try_connection("state backend", state_connection.create_engine_adapter())

        self._try_connection("test", self._test_engine_adapter)

    def close(self) -> None:
        """Releases all resources allocated by this context."""
        self.snapshot_evaluator.close()
        self.state_sync.close()

    def _run_tests(self) -> t.Tuple[unittest.result.TestResult, str]:
        test_output_io = StringIO()
        result = self.test(stream=test_output_io)
        return result, test_output_io.getvalue()

    def _run_plan_tests(
        self, skip_tests: bool = False
    ) -> t.Tuple[t.Optional[unittest.result.TestResult], t.Optional[str]]:
        if self._test_engine_adapter and not skip_tests:
            result, test_output = self._run_tests()
            self.console.log_test_results(result, test_output, self._test_engine_adapter.dialect)
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
            name: snapshot.table_name()
            if snapshot.version
            else snapshot.qualified_view_name.for_environment(
                EnvironmentNamingInfo(
                    name=c.PROD, suffix_target=self.config.environment_suffix_target
                )
            )
            for name, snapshot in self.snapshots.items()
        }

    def _snapshots(
        self, models_override: t.Optional[UniqueKeyDict[str, Model]] = None
    ) -> t.Dict[str, Snapshot]:
        prod = self.state_reader.get_environment(c.PROD)
        remote_snapshots = (
            {
                snapshot.name: snapshot
                for snapshot in self.state_reader.get_snapshots(prod.snapshots).values()
            }
            if prod
            else {}
        )

        local_nodes = {**(models_override or self._models), **self._standalone_audits}
        nodes = local_nodes.copy()
        audits = self._audits.copy()
        projects = {config.project for config in self.configs.values()}

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
                if node.name not in local_nodes and node.name in remote_snapshots:
                    snapshot = remote_snapshots[node.name]
                    ttl = snapshot.ttl
                else:
                    config = self.config_for_node(node)
                    ttl = config.snapshot_ttl

                snapshot = Snapshot.from_node(
                    node,
                    nodes=nodes,
                    audits=audits,
                    cache=fingerprint_cache,
                    ttl=ttl,
                )
                snapshots[node.name] = snapshot
            return snapshots

        snapshots = _nodes_to_snapshots(nodes)
        stored_snapshots = self.state_reader.get_snapshots(snapshots.values())

        unrestorable_snapshot_names = {
            s.name for s in stored_snapshots.values() if s.name in local_nodes and s.unrestorable
        }
        if unrestorable_snapshot_names:
            for name in unrestorable_snapshot_names:
                snapshot_id = snapshots[name].snapshot_id
                logger.info(
                    "Found a unrestorable snapshot %s. Restamping the model...", snapshot_id
                )
                node = local_nodes[name]
                nodes[name] = node.copy(update={"stamp": f"revert to {snapshot_id.identifier}"})
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
    ) -> ContextDiff:
        environment = Environment.normalize_name(environment)
        return ContextDiff.create(
            environment,
            snapshots=snapshots or self.snapshots,
            create_from=create_from or c.PROD,
            state_reader=self.state_reader,
        )

    def _load_configs(
        self, config: t.Union[str, Config], paths: t.List[Path]
    ) -> t.Dict[Path, Config]:
        if isinstance(config, Config):
            return {path: config for path in paths}

        config_env_vars = None
        personal_paths = [
            self.sqlmesh_path / "config.yml",
            self.sqlmesh_path / "config.yaml",
        ]
        for path in personal_paths:
            if path.exists():
                config_env_vars = load_config_from_yaml(path).get("env_vars")
                if config_env_vars:
                    break

        with env_vars(config_env_vars if config_env_vars else {}):
            return {
                path: load_config_from_paths(
                    project_paths=[path / "config.py", path / "config.yml", path / "config.yaml"],
                    personal_paths=personal_paths,
                    config_name=config,
                )
                for path in paths
            }

    def _run_janitor(self) -> None:
        expired_environments = self.state_sync.delete_expired_environments()
        cleanup_expired_views(self.engine_adapter, expired_environments)
        expired_snapshots = self.state_sync.delete_expired_snapshots()
        self.snapshot_evaluator.cleanup(expired_snapshots)

        self.state_sync.compact_intervals()

    def _try_connection(self, connection_name: str, engine_adapter: EngineAdapter) -> None:
        connection_name = connection_name.capitalize()
        try:
            engine_adapter.fetchall("SELECT 1")
            self.console.log_status_update(f"{connection_name} connection [green]succeeded[/green]")
        except Exception as ex:
            self.console.log_error(f"{connection_name} connection failed. {ex}")

    def _new_state_sync(self) -> StateSync:
        return self._provided_state_sync or self._scheduler.create_state_sync(self)

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
