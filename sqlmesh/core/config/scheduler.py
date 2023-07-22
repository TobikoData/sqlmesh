from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field, root_validator
from requests import Session

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.console import Console
from sqlmesh.core.plan import AirflowPlanEvaluator, BuiltInPlanEvaluator, PlanEvaluator
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateSync
from sqlmesh.schedulers.airflow.client import AirflowClient

if t.TYPE_CHECKING:
    from google.auth.transport.requests import AuthorizedSession

    from sqlmesh.core.context import Context

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class _SchedulerConfig(abc.ABC):
    """Abstract base class for Scheduler configurations."""

    @abc.abstractmethod
    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        """Creates a Plan Evaluator instance.

        Args:
            context: The SQLMesh Context.
        """

    @abc.abstractmethod
    def create_state_sync(self, context: Context) -> StateSync:
        """Creates a State Sync instance.

        Args:
            context: The SQLMesh Context.

        Returns:
            The StateSync instance.
        """


class BuiltInSchedulerConfig(_SchedulerConfig, BaseConfig):
    """The Built-In Scheduler configuration."""

    type_: Literal["builtin"] = Field(alias="type", default="builtin")

    def create_state_sync(self, context: Context) -> StateSync:
        state_connection = context.config.get_state_connection(context.gateway)
        engine_adapter = (
            state_connection.create_engine_adapter() if state_connection else context.engine_adapter
        )
        schema = context.config.get_state_schema(context.gateway)
        return EngineAdapterStateSync(engine_adapter, schema=schema, console=context.console)

    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        return BuiltInPlanEvaluator(
            state_sync=context.state_sync,
            snapshot_evaluator=context.snapshot_evaluator,
            backfill_concurrent_tasks=context.concurrent_tasks,
            console=context.console,
            notification_target_manager=context.notification_target_manager,
        )


class _BaseAirflowSchedulerConfig(_SchedulerConfig):
    dag_run_poll_interval_secs: int
    dag_creation_poll_interval_secs: int
    dag_creation_max_retry_attempts: int

    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int

    @abc.abstractmethod
    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        """Constructs the Airflow Client instance."""

    def create_state_sync(self, context: Context) -> StateSync:
        from sqlmesh.schedulers.airflow.state_sync import HttpStateSync

        return HttpStateSync(
            client=self.get_client(context.console),
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
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=context.users,
        )


class AirflowSchedulerConfig(_BaseAirflowSchedulerConfig, BaseConfig):
    """The Airflow Scheduler configuration.

    Args:
        airflow_url: The URL of the Airflow Webserver.
        username: The Airflow username.
        password: The Airflow password.
        dag_run_poll_interval_secs: Determines how often a running DAG can be polled (in seconds).
        dag_creation_poll_interval_secs: Determines how often SQLMesh should check whether a DAG has been created (in seconds).
        dag_creation_max_retry_attempts: Determines the maximum number of attempts that SQLMesh will make while checking for
            whether a DAG has been created.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
    """

    airflow_url: str = "http://localhost:8080/"
    username: str = "airflow"
    password: str = "airflow"
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    type_: Literal["airflow"] = Field(alias="type", default="airflow")

    _concurrent_tasks_validator = concurrent_tasks_validator

    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        session = Session()
        session.headers.update({"Content-Type": "application/json"})
        session.auth = (self.username, self.password)

        return AirflowClient(
            session=session,
            airflow_url=self.airflow_url,
            console=console,
        )


class CloudComposerSchedulerConfig(_BaseAirflowSchedulerConfig, BaseConfig):
    """The Google Cloud Composer configuration.

    Args:
        airflow_url: The URL of the Airflow Webserver.
        dag_run_poll_interval_secs: Determines how often a running DAG can be polled (in seconds).
        dag_creation_poll_interval_secs: Determines how often SQLMesh should check whether a DAG has been created (in seconds).
        dag_creation_max_retry_attempts: Determines the maximum number of attempts that SQLMesh will make while checking for
            whether a DAG has been created.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
    """

    airflow_url: str
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    type_: Literal["cloud_composer"] = Field(alias="type", default="cloud_composer")

    _concurrent_tasks_validator = concurrent_tasks_validator

    class Config:
        # See `check_supported_fields` for the supported extra fields
        extra = "allow"

    def __init__(self, **data: t.Any) -> None:
        super().__init__(**data)
        self._session: t.Optional[AuthorizedSession] = data.get("session")

    @property
    def session(self) -> AuthorizedSession:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        if self._session is None:
            self._session = AuthorizedSession(
                google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])[0]
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


SchedulerConfig = Annotated[
    t.Union[BuiltInSchedulerConfig, AirflowSchedulerConfig, CloudComposerSchedulerConfig],
    Field(discriminator="type_"),
]
