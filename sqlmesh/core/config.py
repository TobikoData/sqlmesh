"""
# Configuring SQLMesh

You can configure your project in multiple places, and SQLMesh will prioritize configurations according to
the following order. From least to greatest precedence:

- A Config object defined in a config.py file at the root of your project:
    ```python
    # config.py
    import duckdb
    from sqlmesh.core.engine_adapter import EngineAdapter
    local_config = Config(
        engine_connection_factory=duckdb.connect,
        engine_dialect="duckdb"
    )
    # End config.py

    >>> from sqlmesh import Context
    >>> context = Context(path="example", config="local_config")

    ```
- A Config object used when initializing a Context:
    ```python
    >>> from sqlmesh import Context
    >>> from sqlmesh.core.config import Config
    >>> my_config = Config(
    ...     engine_connection_factory=duckdb.connect,
    ...     engine_dialect="duckdb"
    ... )
    >>> context = Context(path="example", config=my_config)

    ```
- Individual config parameters used when initializing a Context:
    ```python
    >>> from sqlmesh import Context
    >>> from sqlmesh.core.engine_adapter import create_engine_adapter
    >>> adapter = create_engine_adapter(duckdb.connect, "duckdb")
    >>> context = Context(
    ...     path="example",
    ...     engine_adapter=adapter,
    ...     dialect="duckdb",
    ... )

    ```

# Using Config

The most common way to configure your SQLMesh project is with a `config.py` module at the root of the
project. A SQLMesh Context will automatically look for Config objects there. You can have multiple
Config objects defined, and then tell Context which one to use. For example, you can have different
Configs for local and production environments, Airflow, and Model tests.

Example config.py:
```python
import duckdb

from sqlmesh.core.config import Config, AirflowSchedulerBackend

from my_project.utils import load_test_data


DEFAULT_KWARGS = {
    "engine_dialect": "duckdb",
    "engine_connection_factory": duckdb.connect,
}

# An in memory DuckDB config.
config = Config(**DEFAULT_KWARGS)

# A stateful DuckDB config.
local_config = Config(
    **{
        **DEFAULT_KWARGS,
        "engine_connection_factory": lambda: duckdb.connect(
            database=f"{DATA_DIR}/local.duckdb"
        ),
    }
)

# The config to run model tests.
test_config = Config(
    **DEFAULT_KWARGS,
)

# A config that uses Airflow
airflow_config = Config(
    "scheduler_backend": AirflowSchedulerBackend(),
    **DEFAULT_KWARGS,
)
```

To use a Config, pass in its variable name to Context.
```python
>>> from sqlmesh import Context
>>> context = Context(path="example", config="local_config")

```

For more information about the Config class and its parameters, see `sqlmesh.core.config.Config`.
"""
from __future__ import annotations

import abc
import typing as t

import duckdb
from pydantic import root_validator, validator
from requests import Session

from sqlmesh.core import constants as c
from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.console import Console
from sqlmesh.core.plan import AirflowPlanEvaluator, BuiltInPlanEvaluator, PlanEvaluator
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateReader, StateSync
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.schedulers.airflow.common import AIRFLOW_LOCAL_URL
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from google.auth.transport.requests import AuthorizedSession

    from sqlmesh.core.context import Context


class SchedulerBackend(abc.ABC):
    """Abstract base class for Scheduler configurations."""

    @abc.abstractmethod
    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        """Creates a Plan Evaluator instance.

        Args:
            context: The SQLMesh Context.
        """

    def create_state_sync(self, context: Context) -> t.Optional[StateSync]:
        """Creates a State Sync instance.

        Args:
            context: The SQLMesh Context.

        Returns:
            The StateSync instance.
        """
        return None

    def create_state_reader(self, context: Context) -> t.Optional[StateReader]:
        """Creates a State Reader instance.

        Functionality related to evaluation on a client side (Context.evaluate, Context.run, etc.)
        will be unavailable if a State Reader instance is available but a State Sync instance is not.

        Args:
            context: The SQLMesh Context.

        Returns:
            The StateReader instance.
        """
        return None


class BuiltInSchedulerBackend(SchedulerBackend):
    """The Built-In Scheduler configuration."""

    def create_state_sync(self, context: Context) -> t.Optional[StateSync]:
        return EngineAdapterStateSync(
            context.engine_adapter, context.physical_schema, context.table_info_cache
        )

    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        return BuiltInPlanEvaluator(
            state_sync=context.state_sync,
            snapshot_evaluator=context.snapshot_evaluator,
            backfill_concurrent_tasks=context.backfill_concurrent_tasks,
            console=context.console,
        )


class AirflowSchedulerBackend(SchedulerBackend, PydanticModel):
    """The Airflow Scheduler configuration."""

    airflow_url: str = AIRFLOW_LOCAL_URL
    username: str = "airflow"
    password: str = "airflow"
    max_concurrent_requests: int = 2
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        session = Session()
        session.headers.update({"Content-Type": "application/json"})
        session.auth = (self.username, self.password)

        return AirflowClient(
            session=session,
            airflow_url=self.airflow_url,
            console=console,
        )

    def create_state_reader(self, context: Context) -> t.Optional[StateReader]:
        from sqlmesh.schedulers.airflow.state_sync import HttpStateReader

        return HttpStateReader(
            table_info_cache=context.table_info_cache,
            client=self.get_client(context.console),
            max_concurrent_requests=self.max_concurrent_requests,
            dag_run_poll_interval_secs=self.dag_run_poll_interval_secs,
            console=context.console,
        )

    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        return AirflowPlanEvaluator(
            airflow_client=self.get_client(context.console),
            dag_run_poll_interval_secs=self.dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs=self.dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts=self.dag_creation_max_retry_attempts,
            console=context.console,
            notification_targets=context.notification_targets,
            backfill_concurrent_tasks=context.backfill_concurrent_tasks,
            ddl_concurrent_tasks=context.ddl_concurrent_tasks,
            users=context.users,
        )


class CloudComposerSchedulerBackend(AirflowSchedulerBackend, PydanticModel):
    airflow_url: str
    max_concurrent_requests: int = 2
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    class Config:
        # See `check_supported_fields` for the supported extra fields
        extra = "allow"

    def __init__(self, **data):
        super().__init__(**data)
        self._session: t.Optional[AuthorizedSession] = data.get("session")

    @property
    def session(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        if self._session is None:
            self._session = AuthorizedSession(
                google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )[0]
            )
            self._session.headers.update({"Content-Type": "application/json"})
        return self._session

    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        return AirflowClient(
            airflow_url=self.airflow_url,
            session=self.session,
            console=console,
        )

    @root_validator(pre=True)
    def check_supported_fields(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        allowed_field_names = {field.alias for field in cls.__fields__.values()}
        allowed_field_names.add("session")

        for field_name in values:
            if field_name not in allowed_field_names:
                raise ValueError(f"Unsupported Field: {field_name}")
        return values


class Config(PydanticModel):
    """
    An object used by a Context to configure your SQLMesh project.

    Args:
        engine_connection_factory: The calllable which creates a new engine connection on each call.
        engine_dialect: The engine dialect.
        scheduler_backend: Identifies which scheduler backend to use.
        notification_targets: The notification targets to use.
        dialect: The default sql dialect of model queries. Default: same as engine dialect.
        physical_schema: The default schema used to store materialized tables.
        snapshot_ttl: Duration before unpromoted snapshots are removed.
        time_column_format: The default format to use for all model time columns. Defaults to %Y-%m-%d.
            This time format uses python format codes. https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during
            plan application. Default: 1.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL
            operations (table / view creation, deletion, etc). Default: 1.
        evaluation_concurrent_tasks: The number of concurrent tasks used for model evaluation when
            running with the built-in scheduler. Default: 1.
        users: A list of users that can be used for approvals/notifications.
    """

    engine_connection_factory: t.Callable[[], t.Any] = duckdb.connect
    engine_dialect: str = "duckdb"
    scheduler_backend: SchedulerBackend = BuiltInSchedulerBackend()
    notification_targets: t.List[NotificationTarget] = []
    dialect: str = ""
    physical_schema: str = ""
    snapshot_ttl: str = ""
    ignore_patterns: t.List[str] = []
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    backfill_concurrent_tasks: int = 1
    ddl_concurrent_tasks: int = 1
    evaluation_concurrent_tasks: int = 1
    users: t.List[User] = []

    class Config:
        arbitrary_types_allowed = True

    @validator(
        "backfill_concurrent_tasks",
        "ddl_concurrent_tasks",
        "evaluation_concurrent_tasks",
        pre=True,
    )
    def _concurrent_tasks_validator(cls, v: t.Any) -> int:
        if not isinstance(v, int) or v <= 0:
            raise ConfigError(
                f"The number of concurrent tasks must be an integer value greater than 0. '{v}' was provided"
            )
        return v
