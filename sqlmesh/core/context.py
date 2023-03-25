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
context.run_tests()
```
"""
from __future__ import annotations

import abc
import contextlib
import typing as t
import unittest.result
from io import StringIO
from pathlib import Path
from types import MappingProxyType

import pandas as pd
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config, load_config_from_paths
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.dialect import format_model_expressions, pandas_to_sql, parse
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.hooks import hook
from sqlmesh.core.loader import Loader, SqlMeshLoader, update_model_schemas
from sqlmesh.core.macros import ExecutableOrMacro
from sqlmesh.core.model import Model
from sqlmesh.core.plan import Plan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotFingerprint,
    to_table_mapping,
)
from sqlmesh.core.state_sync import StateReader, StateSync
from sqlmesh.core.test import run_all_model_tests, run_model_tests
from sqlmesh.core.user import User
from sqlmesh.utils import UniqueKeyDict, sys_path
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, yesterday_ds
from sqlmesh.utils.errors import ConfigError, MissingDependencyError, PlanError

if t.TYPE_CHECKING:
    import graphviz
    import pyspark

    from sqlmesh.core.engine_adapter._typing import DF

    ModelOrSnapshot = t.Union[str, Model, Snapshot]


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
    def spark(self) -> t.Optional[pyspark.sql.SparkSession]:
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

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        """Fetches a dataframe given a sql string or sqlglot expression.

        Args:
            query: SQL string or sqlglot expression.

        Returns:
            The default dataframe is Pandas, but for Spark a PySpark dataframe is returned.
        """
        return self.engine_adapter.fetchdf(query)

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> pyspark.sql.DataFrame:
        """Fetches a PySpark dataframe given a sql string or sqlglot expression.

        Args:
            query: SQL string or sqlglot expression.

        Returns:
            A PySpark dataframe.
        """
        return self.engine_adapter.fetch_pyspark_df(query)


class ExecutionContext(BaseContext):
    """The minimal context needed to execute a model.

    Args:
        engine_adapter: The engine adapter to execute queries against.
        snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
        is_dev: Indicates whether the evaluation happens in the development mode and temporary
            tables / table clones should be used where applicable.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        snapshots: t.Dict[str, Snapshot],
        is_dev: bool,
    ):
        self.snapshots = snapshots
        self.is_dev = is_dev
        self._engine_adapter = engine_adapter
        self.__model_tables = to_table_mapping(snapshots.values(), is_dev)

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
        dialect: Default dialect of the sql in models.
        physical_schema: The schema used to store physical materialized tables.
        snapshot_ttl: Duration before unpromoted snapshots are removed.
        path: The directory containing SQLMesh files.
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
        dialect: str = "",
        physical_schema: str = "",
        snapshot_ttl: str = "",
        path: str = "",
        config: t.Optional[t.Union[Config, str]] = None,
        connection: t.Optional[str] = None,
        test_connection: t.Optional[str] = None,
        concurrent_tasks: t.Optional[int] = None,
        loader: t.Optional[t.Type[Loader]] = None,
        load: bool = True,
        console: t.Optional[Console] = None,
        users: t.Optional[t.List[User]] = None,
    ):
        self.console = console or get_console()
        self.path = Path(path).absolute()
        if not self.path.is_dir():
            raise ConfigError(f"{path} is not a directory")

        self.config = self._load_config(config or "config")

        self.physical_schema = physical_schema or self.config.physical_schema or "sqlmesh"
        self.snapshot_ttl = snapshot_ttl or self.config.snapshot_ttl or c.DEFAULT_SNAPSHOT_TTL
        self.dag: DAG[str] = DAG()

        self._models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        self._audits: UniqueKeyDict[str, Audit] = UniqueKeyDict("audits")
        self._macros: UniqueKeyDict[str, ExecutableOrMacro] = UniqueKeyDict("macros")
        self._hooks: UniqueKeyDict[str, hook] = UniqueKeyDict("hooks")

        self.connection = connection
        connection_config = self.config.get_connection(connection)
        self.concurrent_tasks = concurrent_tasks or connection_config.concurrent_tasks
        self._engine_adapter = engine_adapter or connection_config.create_engine_adapter()

        test_connection_config = (
            self.config.test_connection
            if test_connection is None
            else self.config.get_connection(test_connection)
        )
        self._test_engine_adapter = test_connection_config.create_engine_adapter()

        self.dialect = dialect or self.config.model_defaults.dialect or self._engine_adapter.dialect

        self.snapshot_evaluator = SnapshotEvaluator(
            self.engine_adapter, ddl_concurrent_tasks=self.concurrent_tasks
        )

        self.notification_targets = self.config.notification_targets + (notification_targets or [])

        self._provided_state_sync: t.Optional[StateSync] = state_sync
        self._state_sync: t.Optional[StateSync] = None
        self._state_reader: t.Optional[StateReader] = None

        self.users = self.config.users + (users or [])

        self._loader = (loader or self.config.loader or SqlMeshLoader)()

        if load:
            self.load()

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    def upsert_model(self, model: t.Union[str, Model], **kwargs: t.Any) -> Model:
        """Update or insert a model.

        The context's models dictionary will be updated to include these changes.

        Args:
            model: Model name or instance to update.
            kwargs: The kwargs to update the model with.

        Returns:
            A new instance of the updated or inserted model.
        """
        if isinstance(model, str):
            model = self._models[model]

        path = model._path  # type: ignore
        # model.copy() can't be used here due to a cached state that can be a part of a model instance.
        model = t.cast(Model, type(model)(**{**t.cast(Model, model).dict(), **kwargs}))
        model._path = path
        self._models.update({model.name: model})

        self._add_model_to_dag(model)
        update_model_schemas(self.dialect, self.dag, self._models)

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
        )

    @property
    def state_sync(self) -> StateSync:
        if not self._state_sync:
            self._state_sync = self._provided_state_sync or self.config.scheduler.create_state_sync(
                self
            )
            if not self._state_sync:
                raise ConfigError(
                    "The operation is not supported when using a read-only state sync"
                )
            self._state_sync.init_schema()
        return self._state_sync

    @property
    def state_reader(self) -> StateReader:
        if not self._state_reader:
            try:
                self._state_reader = self.state_sync
            except ConfigError:
                self._state_reader = self.config.scheduler.create_state_reader(self)
            if not self._state_reader:
                raise ConfigError(
                    "Invalid configuration: neither State Sync nor Reader has been configured"
                )
        return self._state_reader

    @property
    def sqlmesh_path(self) -> Path:
        """Path to the SQLMesh home directory."""
        return Path.home() / ".sqlmesh"

    @property
    def models_directory_path(self) -> Path:
        """Path to the directory where the models are defined"""
        return self.path / "models"

    @property
    def macro_directory_path(self) -> Path:
        """Path to the directory where macros are defined"""
        return self.path / "macros"

    @property
    def hook_directory_path(self) -> Path:
        """Path to the directory where hooks are defined"""
        return self.path / "hooks"

    @property
    def test_directory_path(self) -> Path:
        return self.path / "tests"

    @property
    def audits_directory_path(self) -> Path:
        return self.path / "audits"

    @property
    def ignore_patterns(self) -> t.List[str]:
        return c.IGNORE_PATTERNS + self.config.ignore_patterns

    def refresh(self) -> None:
        """Refresh all models that have been updated."""
        if self._loader.reload_needed():
            self.load()

    def load(self) -> Context:
        """Load all files in the context's path."""
        with sys_path(self.path):
            project = self._loader.load(self)
            self._hooks = project.hooks
            self._macros = project.macros
            self._models = project.models
            self._audits = project.audits
            self.dag = project.dag

        return self

    def run(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        skip_janitor: bool = False,
    ) -> None:
        """Run the entire dag through the scheduler.

        Args:
            environment: The target environment to source model snapshots from. Default: prod.
            start: The start of the interval to render.
            end: The end of the interval to render.
            latest: The latest time used for non incremental datasets.
            skip_janitor: Whether to skip the jantitor task.
        """
        self.scheduler(environment=environment or c.PROD).run(start, end, latest)

        if not skip_janitor:
            self._run_janitor()

    def get_model(self, name: str) -> t.Optional[Model]:
        """Returns a model with the given name or None if a model with such name doesn't exist."""
        return self._models.get(name)

    @property
    def models(self) -> MappingProxyType[str, Model]:
        """Returns all registered models in this context."""
        return MappingProxyType(self._models)

    @property
    def macros(self) -> MappingProxyType[str, ExecutableOrMacro]:
        """Returns all registered macros in this context."""
        return MappingProxyType(self._macros)

    @property
    def hooks(self) -> MappingProxyType[str, hook]:
        """Returns all registered hooks in this context."""
        return MappingProxyType(self._hooks)

    @property
    def snapshots(self) -> t.Dict[str, Snapshot]:
        """Generates and returns snapshots based on models registered in this context.

        If one of the snapshots has been previosly stored in the persisted state, the stored
        instance will be returned.
        """
        local_snapshots = self.local_snapshots

        stored_snapshots = self.state_reader.get_snapshots(
            [s.snapshot_id for s in local_snapshots.values()]
        )

        return {name: stored_snapshots.get(s.snapshot_id, s) for name, s in local_snapshots.items()}

    @property
    def local_snapshots(self) -> t.Dict[str, Snapshot]:
        """Generates and returns snapshots based on models registered in this context without reconciling them
        with the persisted state.
        """
        local_snapshots = {}
        fingerprint_cache: t.Dict[str, SnapshotFingerprint] = {}
        for model in self._models.values():
            snapshot = Snapshot.from_model(
                model,
                physical_schema=self.physical_schema,
                models=self._models,
                ttl=self.snapshot_ttl,
                audits=self._audits,
                cache=fingerprint_cache,
            )
            local_snapshots[model.name] = snapshot
        return local_snapshots

    def render(
        self,
        model_or_snapshot: ModelOrSnapshot,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        expand: t.Union[bool, t.Iterable[str]] = False,
        **kwargs: t.Any,
    ) -> exp.Expression:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to render.
            end: The end of the interval to render.
            latest: The latest time used for non incremental datasets.
            expand: Whether or not to use expand materialized models, defaults to False.
                If True, all referenced models are expanded as raw queries.
                If a list, only referenced models are expanded as raw queries.

        Returns:
            The rendered expression.
        """
        latest = latest or yesterday_ds()

        if isinstance(model_or_snapshot, str):
            model = self._models[model_or_snapshot]
        elif isinstance(model_or_snapshot, Snapshot):
            model = model_or_snapshot.model
        else:
            model = model_or_snapshot

        expand = self.dag.upstream(model.name) if expand is True else expand or []

        if model.is_seed:
            df = next(model.render(self, start=start, end=end, latest=latest, **kwargs))
            return next(pandas_to_sql(df, model.columns_to_types))

        return model.render_query(
            start=start,
            end=end,
            latest=latest,
            snapshots=self.snapshots,
            expand=expand,
            **kwargs,
        )

    def evaluate(
        self,
        model_or_snapshot: ModelOrSnapshot,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
        limit: t.Optional[int] = None,
        **kwargs: t.Any,
    ) -> DF:
        """Evaluate a model or snapshot (running its query against a DB/Engine).

        This method is used to test or iterate on models without side effects.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to evaluate.
            end: The end of the interval to evaluate.
            latest: The latest time used for non incremental datasets.
            limit: A limit applied to the model.
        """
        if isinstance(model_or_snapshot, str):
            snapshot = self.snapshots[model_or_snapshot]
        elif isinstance(model_or_snapshot, Snapshot):
            snapshot = model_or_snapshot
        else:
            snapshot = self.snapshots[model_or_snapshot.name]

        df = self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            latest,
            snapshots=self.snapshots,
            limit=limit or c.DEFAULT_MAX_LIMIT,
        )

        if df is None:
            raise RuntimeError(f"Error evaluating {snapshot.model.name}")

        return df

    def format(self) -> None:
        """Format all models in a given directory."""
        for model in self._models.values():
            if not model.is_sql:
                continue
            with open(model._path, "r+", encoding="utf-8") as file:
                expressions = parse(file.read(), default_dialect=self.dialect)
                file.seek(0)
                file.write(format_model_expressions(expressions, model.dialect))
                file.truncate()

    def plan(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        create_from: t.Optional[str] = None,
        skip_tests: bool = False,
        restate_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        forward_only: bool = False,
        no_prompts: bool = False,
        auto_apply: bool = False,
        no_auto_categorization: t.Optional[bool] = None,
    ) -> Plan:
        """Interactively create a migration plan.

        This method compares the current context with an environment. It then presents
        the differences and asks whether to backfill each modified model.

        Args:
            environment: The environment to diff and plan against.
            start: The start date of the backfill if there is one.
            end: The end date of the backfill if there is one.
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

        Returns:
            The populated Plan object.
        """
        environment = environment or c.PROD
        environment = Environment.normalize_name(environment)

        if skip_backfill and not no_gaps and environment == c.PROD:
            raise ConfigError(
                "When targeting the production enviornment either the backfill should not be skipped or the lack of data gaps should be enforced (--no-gaps flag)."
            )

        self._run_plan_tests(skip_tests)

        plan = Plan(
            context_diff=self._context_diff(environment or c.PROD, create_from=create_from),
            dag=self.dag,
            state_reader=self.state_reader,
            start=start,
            end=end,
            apply=self.apply,
            restate_models=restate_models,
            no_gaps=no_gaps,
            skip_backfill=skip_backfill,
            is_dev=environment != c.PROD,
            forward_only=forward_only,
            environment_ttl=self.config.environment_ttl,
            categorizer_config=self.config.auto_categorize_changes,
            auto_categorization_enabled=not no_auto_categorization,
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
        if not plan.context_diff.has_changes and not plan.requires_backfill:
            return
        if plan.uncategorized:
            raise PlanError("Can't apply a plan with uncategorized changes.")
        self.config.scheduler.create_plan_evaluator(self).evaluate(plan)

    def diff(self, environment: t.Optional[str] = None, detailed: bool = False) -> None:
        """Show a diff of the current context with a given environment.

        Args:
            environment: The environment to diff against.
            detailed: Show the actual SQL differences if True.
        """
        environment = environment or c.PROD
        environment = Environment.normalize_name(environment)
        self.console.show_model_difference_summary(
            self._context_diff(environment or c.PROD), detailed
        )

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
    ) -> unittest.result.TestResult:
        """Discover and run model tests"""
        verbosity = 2 if verbose else 1
        try:
            if tests:
                result = run_model_tests(
                    tests=tests,
                    snapshots=self.local_snapshots,
                    engine_adapter=self._test_engine_adapter,
                    verbosity=verbosity,
                    patterns=match_patterns,
                    ignore_patterns=self.ignore_patterns,
                )
            else:
                result = run_all_model_tests(
                    path=self.test_directory_path,
                    snapshots=self.local_snapshots,
                    engine_adapter=self._test_engine_adapter,
                    verbosity=verbosity,
                    patterns=match_patterns,
                    ignore_patterns=self.ignore_patterns,
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
        latest: t.Optional[TimeLike] = None,
    ) -> None:
        """Audit models.

        Args:
            start: The start of the interval to audit.
            end: The end of the interval to audit.
            models: The models to audit. All models will be audited if not specified.
            latest: The latest time used for non incremental datasets.

        """

        snapshots = (
            [self.snapshots[model] for model in models] if models else self.snapshots.values()
        )

        num_audits = sum(len(snapshot.model.audits) for snapshot in snapshots)
        self.console.log_status_update(f"Found {num_audits} audit(s).")
        errors = []
        for snapshot in snapshots:
            for audit_result in self.snapshot_evaluator.audit(
                snapshot=snapshot,
                start=start,
                end=end,
                snapshots=self.snapshots,
                raise_exception=False,
            ):
                if audit_result.count:
                    errors.append(audit_result)
                    self.console.log_status_update(f"{audit_result.audit.name} FAIL.")
                else:
                    self.console.log_status_update(f"{audit_result.audit.name} PASS.")

        self.console.log_status_update(f"\nFinished with {len(errors)} audit error(s).")
        for error in errors:
            self.console.log_status_update(
                f"\nFailure in audit {error.audit.name} ({error.audit._path})."
            )
            self.console.log_status_update(f"Got {error.count} results, expected 0.")
            self.console.show_sql(f"{error.query}")
        self.console.log_status_update("Done.")

    def close(self) -> None:
        """Releases all resources allocated by this context."""
        self.snapshot_evaluator.close()

    def _run_plan_tests(
        self, skip_tests: bool = False
    ) -> t.Tuple[t.Optional[unittest.result.TestResult], t.Optional[str]]:
        if self._test_engine_adapter and not skip_tests:
            test_output_io = StringIO()
            with contextlib.redirect_stderr(test_output_io):
                result = self.test()
            test_output = test_output_io.getvalue()
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
            else snapshot.qualified_view_name.for_environment(c.PROD)
            for name, snapshot in self.snapshots.items()
        }

    def _context_diff(
        self,
        environment: str | Environment,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        create_from: t.Optional[str] = None,
    ) -> ContextDiff:
        environment = Environment.normalize_name(environment)
        return ContextDiff.create(
            environment, snapshots or self.snapshots, create_from or c.PROD, self.state_reader
        )

    def _load_config(self, config: t.Union[str, Config]) -> Config:
        if isinstance(config, Config):
            return config

        lookup_paths = [
            self.sqlmesh_path / "config.yml",
            self.sqlmesh_path / "config.yaml",
            self.path / "config.py",
            self.path / "config.yml",
            self.path / "config.yaml",
        ]
        return load_config_from_paths(*lookup_paths, config_name=config)

    def _add_model_to_dag(self, model: Model) -> None:
        self.dag.graph[model.name] = set()

        self.dag.add(model.name, model.depends_on)

    def _run_janitor(self) -> None:
        expired_environments = self.state_sync.delete_expired_environments()
        for expired_environment in expired_environments:
            self.snapshot_evaluator.demote(expired_environment.snapshots, expired_environment.name)

        expired_snapshots = self.state_sync.delete_expired_snapshots()
        self.snapshot_evaluator.cleanup(expired_snapshots)
