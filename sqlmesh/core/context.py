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
context = Context(path="example", config="test_config")
context.run_tests()
```
"""
from __future__ import annotations

import abc
import contextlib
import importlib
import types
import typing as t
import unittest.result
from io import StringIO
from pathlib import Path

from sqlglot import exp, parse

from sqlmesh.core import constants as c
from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.audit import Audit
from sqlmesh.core.config import Config
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.dag import DAG
from sqlmesh.core.dialect import extend_sqlglot, format_model_expressions
from sqlmesh.core.engine_adapter import DF, EngineAdapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.macros import macro
from sqlmesh.core.model import Model
from sqlmesh.core.model import model as model_registry
from sqlmesh.core.plan import Plan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.core.state_sync import StateReader, StateSync
from sqlmesh.core.test import run_all_model_tests
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.date import TimeLike, yesterday_ds
from sqlmesh.utils.errors import ConfigError, MissingDependencyError, PlanError
from sqlmesh.utils.file_cache import FileCache

if t.TYPE_CHECKING:
    import graphviz

    MODEL_OR_SNAPSHOT = t.Union[str, Model, Snapshot]

extend_sqlglot()


class BaseContext(abc.ABC):
    """The base context which defines methods to execute a model."""

    @property
    @abc.abstractmethod
    def model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""

    @property
    @abc.abstractmethod
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""

    @property
    def spark(self) -> t.Optional["pyspark.sql.SparkSession"]:  # type: ignore
        """Returns the spark session if it exists."""
        return self.engine_adapter.spark

    def table(self, model_name: str) -> str:
        """Gets the physical table name for a given model.

        Args:
            model_name: The model name.

        Returns:
            The physical table name.
        """
        return self.model_tables[model_name]

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> DF:
        """Fetches a dataframe given a sql string or sqlglot expression.

        Args:
            query: SQL string or sqlglot expression.

        Returns:
            The default dataframe is Pandas, but for Spark a PySpark dataframe is returned.
        """
        return self.engine_adapter.fetchdf(query)


class ExecutionContext(BaseContext):
    """The minimal context needed to execute a model.

    Args:
        engine_adapter: The engine adapter to execute queries against.
        mapping: A mapping of models to physical tables.
    """

    def __init__(self, engine_adapter: EngineAdapter, model_tables: t.Dict[str, str]):
        self._engine_adapter = engine_adapter
        self._model_tables = model_tables

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    @property
    def model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""
        return self._model_tables


class Context(BaseContext):
    """Encapsulates a SQLMesh environment supplying convenient functions to perform various tasks.

    Args:
        engine_adapter: The default engine adapter to use.
        notification_targets: The notification target to use. Defaults to what is defined in config.
        dialect: Default dialect of the sql in models.
        physical_schema: The schema used to store physical materialized tables.
        snapshot_ttl: Duration before unpromoted snapshots are removed.
        path: The directory containing SQLMesh files.
        ddl_concurrent_task: The number of concurrent tasks used for DDL
            operations (table / view creation, deletion, etc). Default: 1.
        config: A Config object or the name of a Config object in config.py.
        test_config: A Config object or name of a Config object in config.py to use for testing only
        load: Whether or not to automatically load all models and macros (default True).
        console: The rich instance used for printing out CLI command results.
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
        ddl_concurrent_tasks: t.Optional[int] = None,
        config: t.Optional[t.Union[Config, str]] = None,
        test_config: t.Optional[t.Union[Config, str]] = None,
        load: bool = True,
        console: t.Optional[Console] = None,
    ):
        self.console = console or get_console()
        self.path = Path(path).absolute()
        self.config = self._load_config(config)

        self.test_config = None
        try:
            self.test_config = self._load_config(test_config or "test_config")
        except ConfigError:
            self.console.log_error(
                "Running without test support since `test_config` was not provided and ` "
                "test_config` variable was not found in the namespace"
            )

        # Initialize cache
        cache_path = self.path.joinpath(c.CACHE_PATH)
        cache_path.mkdir(exist_ok=True)
        self.table_info_cache = FileCache(cache_path.joinpath(c.TABLE_INFO_CACHE))
        self.dialect = dialect or self.config.dialect or self.config.engine_dialect
        self.physical_schema = (
            physical_schema or self.config.physical_schema or "sqlmesh"
        )
        self.snapshot_ttl = (
            snapshot_ttl or self.config.snapshot_ttl or c.DEFAULT_SNAPSHOT_TTL
        )
        self.models = UniqueKeyDict("models")
        self.macros = UniqueKeyDict("macros")
        self.dag: DAG[str] = DAG()

        self.ddl_concurrent_tasks = (
            ddl_concurrent_tasks or self.config.ddl_concurrent_tasks
        )

        self._engine_adapter = engine_adapter or EngineAdapter(
            self.config.engine_connection_factory,
            self.config.engine_dialect,
            multithreaded=self.ddl_concurrent_tasks > 1,
        )
        self.test_engine_adapter = (
            EngineAdapter(
                self.test_config.engine_connection_factory,
                self.test_config.engine_dialect,
                multithreaded=self.test_config.ddl_concurrent_tasks > 1,
            )
            if self.test_config
            else None
        )

        self.snapshot_evaluator = SnapshotEvaluator(
            self.engine_adapter, ddl_concurrent_tasks=self.ddl_concurrent_tasks
        )

        self.notification_targets = self.config.notification_targets + (
            notification_targets or []
        )

        self._provided_state_sync: t.Optional[StateSync] = state_sync
        self._state_sync: t.Optional[StateSync] = None
        self._state_reader: t.Optional[StateReader] = None

        self._ignore_patterns = c.IGNORE_PATTERNS + self.config.ignore_patterns
        self._path_mtimes: t.Dict[Path, float] = {}

        if load:
            self.load()

    @property
    def engine_adapter(self) -> EngineAdapter:
        """Returns an engine adapter."""
        return self._engine_adapter

    def upsert_model(self, model: t.Union[str, Model] = "", **kwargs) -> Model:
        """Update or insert a model.

        The context's models dictionary will be updated to include these changes.

        Args:
            model: Model name or instance to update. If no model is passed in a new one is created.
            kwargs: The kwargs to update the model with.

        Returns:
            A new instance of the updated or inserted model.
        """
        if not model:
            model = Model(**kwargs)
        elif isinstance(model, str):
            model = self.models[model]

        model = Model(**{**model.dict(), **kwargs})  # type: ignore
        self.models.update({model.name: model})
        self._add_model_to_dag(model)
        return model

    def scheduler(self, global_state: bool = False) -> Scheduler:
        """Returns the built-in scheduler.

        Args:
            global_state: Whether to initialize the scheduler from the persisted state
                or from the currently loaded local state. Default: False.

        Returns:
            The built-in scheduler instance.
        """
        snapshots: t.Iterable[Snapshot]
        if global_state:
            snapshots = self.state_sync.get_snapshots(None).values()
        else:
            snapshots = self.snapshots.values()
        return Scheduler(
            snapshots,
            self.snapshot_evaluator,
            self.state_sync,
            console=self.console,
        )

    @property
    def state_sync(self) -> StateSync:
        if not self._state_sync:
            self._state_sync = (
                self._provided_state_sync
                or self.config.scheduler_backend.create_state_sync(self)
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
                self._state_reader = self.config.scheduler_backend.create_state_reader(
                    self
                )
            if not self._state_reader:
                raise ConfigError(
                    "Invalid configuration: neither State Sync nor Reader has been configured"
                )
        return self._state_reader

    @property
    def models_directory_path(self) -> Path:
        """Path to the directory where the models are defined"""
        return self.path.joinpath("models")

    @property
    def macro_directory_path(self) -> Path:
        """Path to the drectory where the macros are defined"""
        return self.path.joinpath("macros")

    @property
    def test_directory_path(self) -> Path:
        return self.path.joinpath("tests")

    @property
    def audits_directory_path(self) -> Path:
        return self.path.joinpath("audits")

    def refresh(self) -> None:
        """Refresh all models that have been updated."""
        for path, initial_mtime in self._path_mtimes.items():
            if path.stat().st_mtime > initial_mtime:
                self._path_mtimes.clear()
                self.models.clear()
                self.load()
                return

    def load(self) -> Context:
        """Load all files in the context's path."""
        self._load_macros()
        self._load_models()
        self._load_audits()
        return self

    def run(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        global_state: bool = False,
    ) -> None:
        """Run the entire dag through the scheduler.

        Args:
            start: The start of the interval to render.
            end: The end of the interval to render.
            latest: The latest time used for non incremental datasets.
            global_state: If set to True runs against the persisted state,
                otherwise uses the currently loaded local state. Default: False.
        """
        return self.scheduler(global_state).run(start, end, latest)

    @property
    def snapshots(self) -> t.Dict[str, Snapshot]:
        """Gets all snapshots in this context."""
        snapshots = {}
        fingerprint_cache: t.Dict[str, str] = {}
        with self.table_info_cache as table_info_cache:
            for model in self.models.values():
                snapshot = Snapshot.from_model(
                    model,
                    physical_schema=self.physical_schema,
                    models=self.models,
                    ttl=self.snapshot_ttl,
                    cache=fingerprint_cache,
                )
                cached = table_info_cache.get(snapshot.snapshot_id)
                if cached:
                    snapshot.version = cached.version
                    snapshot.previous_versions = cached.previous_versions
                snapshots[model.name] = snapshot
        return snapshots

    @property
    def model_tables(self) -> t.Dict[str, str]:
        """Mapping of model name to physical table name.

        If a snapshot has not been versioned yet, its view name will be returned.
        """
        return {
            name: snapshot.table_name
            if snapshot.version
            else snapshot.qualified_view_name.for_environment(c.PROD)
            for name, snapshot in self.snapshots.items()
        }

    def render(
        self,
        model_or_snapshot: MODEL_OR_SNAPSHOT,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Union[bool, t.Iterable[str]] = False,
        **kwargs,
    ) -> exp.Expression:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to render.
            end: The end of the interval to render.
            latest: The latest time used for non incremental datasets.
            mappings: A dictionary of table mappings.
            expand: Whether or not to use expand materialized models, defaults to False.
                If True, all referenced models are expanded as raw queries.
                If a list, only referenced models are expanded as raw queries.

        Returns:
            The rendered expression.
        """
        latest = latest or yesterday_ds()

        if isinstance(model_or_snapshot, str):
            model = self.models[model_or_snapshot]
        elif isinstance(model_or_snapshot, Snapshot):
            model = model_or_snapshot.model
        else:
            model = model_or_snapshot

        expand = self.dag.upstream(model.name) if expand is True else expand or []

        return model.render_query(
            start=start,
            end=end,
            latest=latest,
            snapshots=self.snapshots,
            mapping=mapping,
            expand=expand,
            **kwargs,
        )

    def evaluate(
        self,
        model_or_snapshot: MODEL_OR_SNAPSHOT,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
        limit: t.Optional[int] = None,
        **kwargs,
    ) -> DF:
        """Evaluate a model or snapshot (running its query against a DB/Engine).

        This method is used to test or iterate on models without side effects.

        Args:
            model_or_snapshot: The model, model name, or snapshot to render.
            start: The start of the interval to evaluate.
            end: The end of the interval to evaluate.
            latest: The latest time used for non incremental datasets.
            limit: A limit applied to the model, this must be > 0.
        """
        if isinstance(model_or_snapshot, str):
            snapshot = self.snapshots[model_or_snapshot]
        elif isinstance(model_or_snapshot, Model):
            snapshot = self.snapshots[model_or_snapshot.name]
        else:
            snapshot = model_or_snapshot

        if not limit or limit <= 0:
            limit = 1000

        return self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            latest,
            mapping=self.model_tables,
            limit=limit,
        )

    def format(self) -> None:
        """Format all models in a given directory."""
        for model in self.models.values():
            with open(model._path, "r+", encoding="utf-8") as file:
                expressions = [e for e in parse(file.read(), read=self.dialect) if e]
                file.seek(0)
                file.write(format_model_expressions(expressions, model.dialect))
                file.truncate()

    def _run_plan_tests(
        self, skip_tests: bool = False
    ) -> t.Tuple[t.Optional[unittest.result.TestResult], t.Optional[str]]:
        if self.test_engine_adapter and not skip_tests:
            result, test_output = self.run_tests()
            self.console.log_test_results(
                result, test_output, self.test_engine_adapter.dialect
            )
            if not result.wasSuccessful():
                raise PlanError(
                    "Cannot generate plan due to failing test(s). Fix test(s) and run again"
                )
            return result, test_output
        return None, None

    def plan(
        self,
        environment: t.Optional[str] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        from_: t.Optional[str] = None,
        skip_tests: bool = False,
        restate_from: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
    ) -> Plan:
        """Interactively create a migration plan.

        This method compares the current context with an environment. It then presents
        the differences and asks whether to backfill each modified model.

        Args:
            environment: The environment to diff and plan against.
            start: The start date of the backfill if there is one.
            end: The end date of the backfill if there is one.
            from_: The environment to base the plan on instead of local files.
            skip_tests: Unit tests are run by default so this will skip them if enabled
            restate_from: A list of upstream tables outside of the scope of SQLMesh that need to be restated
                for the given plan interval. Restatement means ALL snapshots that depended on these
                upstream tables will have their intervals deleted (even ones not in this current environment).
                Only the snapshots in this environment will be backfilled whereas others need to be recovered
                on a future plan application.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
            The populated Plan object.
        """
        self._run_plan_tests(skip_tests)

        if from_:
            env = self.state_reader.get_environment(from_)
            if not env:
                raise PlanError(f"Environment '{from_}' not found.")

            start = start or env.start
            end = end or env.start
            snapshots = {
                snapshot.name: snapshot
                for snapshot in self.state_reader.get_snapshots(env.snapshots).values()
            }
        else:
            snapshots = {}

        plan = Plan(
            context_diff=self._context_diff(environment or c.PROD, snapshots),
            dag=self.dag,
            state_reader=self.state_reader,
            start=start,
            end=end,
            apply=self.apply,
            restate_from=restate_from,
            no_gaps=no_gaps,
        )

        self.console.plan(plan)

        return plan

    def apply(self, plan: Plan) -> None:
        """Applies a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state sync and then uses the scheduler
        to backfill all models.

        Args:
            plan: The plan to apply.
        """
        self.config.scheduler_backend.create_plan_evaluator(self).evaluate(plan)

    def diff(self, environment: t.Optional[str] = None, detailed: bool = False) -> None:
        """Show a diff of the current context with a given environment.

        Args:
            environment: The environment to diff against.
            detailed: Show the actual SQL differences if True.
        """
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

    def run_tests(
        self, path: t.Optional[str] = None
    ) -> t.Tuple[unittest.result.TestResult, str]:
        """Discover and run model tests"""
        if not self.test_engine_adapter:
            raise ConfigError("Tried to run tests but test_config is not defined")
        test_output = StringIO()
        with contextlib.redirect_stderr(test_output):
            result = run_all_model_tests(
                path=Path(path) if path else self.test_directory_path,
                snapshots=self.snapshots,
                engine_adapter=self.test_engine_adapter,
                ignore_patterns=self._ignore_patterns,
            )
        return result, test_output.getvalue()

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
            [self.snapshots[model] for model in models]
            if models
            else self.snapshots.values()
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
                f"\nFailure in audit {error.audit.name} for model {error.audit.model} ({error.audit._path})."
            )
            self.console.log_status_update(f"Got {error.count} results, expected 0.")
            self.console.show_sql(f"{error.query}")
        self.console.log_status_update("Done.")

    def close(self):
        """Releases all resources allocated by this context."""
        self.snapshot_evaluator.close()

    def _context_diff(
        self,
        environment: str | Environment,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
    ) -> ContextDiff:
        return ContextDiff.create(
            environment, snapshots or self.snapshots, self.state_reader
        )

    def _load_config(self, config: t.Optional[t.Union[Config, str]]) -> Config:
        if isinstance(config, Config):
            return config
        config_module = self._import_python_file(self.path.joinpath("config.py"))
        config_obj = None
        if config_module:
            if config is None:
                config_obj = getattr(config_module, "config")
            else:
                try:
                    config_obj = getattr(config_module, config)
                except AttributeError:
                    raise ConfigError(f"Config {config} not found.")
        if config_obj is None:
            raise ConfigError(
                "SQLMesh Config could not be found. Point the cli to the right path with `sqlmesh --path`. If you haven't set up SQLMesh, run `sqlmesh init`."
            )
        if not isinstance(config_obj, Config):
            raise ConfigError(
                f"Config needs to be of type sqlmesh.core.context.Context. Found `{config_obj}` instead."
            )
        return config_obj

    def _load_macros(self) -> None:
        # Store a copy of the macro registry
        standard_macros = macro.get_registry()

        # Import project python files so custom macros will be registered
        for path in self._glob_path(self.macro_directory_path, ".py"):
            if self._import_python_file(path):
                self._path_mtimes[path] = path.stat().st_mtime
        self.macros = macro.get_registry()

        # Restore the macro registry
        macro.set_registry(standard_macros)

    def _load_models(self):
        """Load all models."""
        module = self.path.name

        for path in self._glob_path(self.models_directory_path, ".sql"):
            self._path_mtimes[path] = path.stat().st_mtime
            with open(path, "r", encoding="utf-8") as file:
                expressions = parse(file.read(), read=self.dialect)
                model = Model.load(
                    expressions,
                    module=module,
                    macros=self.macros,
                    path=Path(path).absolute(),
                    dialect=self.dialect,
                    time_column_format=self.config.time_column_format,
                )
                self.models[model.name] = model
                self._add_model_to_dag(model)

        registry = model_registry.registry()
        registry.clear()
        registered = set()

        for path in self._glob_path(self.models_directory_path, ".py"):
            self._path_mtimes[path] = path.stat().st_mtime
            if self._import_python_file(path):
                self._path_mtimes[path] = path.stat().st_mtime
            new = registry.keys() - registered
            registered |= new
            for name in new:
                model = registry[name].model(
                    module=module,
                    path=path,
                    time_column_format=self.config.time_column_format,
                )
                self.models[model.name] = model
                self._add_model_to_dag(model)

    def _load_audits(self) -> None:
        for path in self._glob_path(self.audits_directory_path, ".sql"):
            self._path_mtimes[path] = path.stat().st_mtime
            with open(path, "r", encoding="utf-8") as file:
                expressions = parse(file.read(), read=self.dialect)
                for audit in Audit.load_multiple(
                    expressions=expressions,
                    path=path,
                    dialect=self.dialect,
                ):
                    if not audit.skip:
                        self.models[audit.model].audits[audit.name] = audit

    def _import_python_file(self, path: Path) -> t.Optional[types.ModuleType]:
        spec = importlib.util.spec_from_file_location(self.path.name, path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(module)
                return module
            except FileNotFoundError:
                pass
            except Exception as e:
                raise ConfigError(f"Error importing {path}.") from e
        return None

    def _glob_path(
        self, path: Path, file_extension: str
    ) -> t.Generator[Path, None, None]:
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
            for ignore_pattern in self._ignore_patterns:
                if filepath.match(ignore_pattern):
                    break
            else:
                yield filepath

    def _add_model_to_dag(self, model: Model) -> None:
        self.dag.graph[model.name] = set()

        self.dag.add(model.name, model.depends_on)
