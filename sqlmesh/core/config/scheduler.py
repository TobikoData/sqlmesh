from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field
from requests import Session

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.console import Console
from sqlmesh.core.plan import (
    AirflowPlanEvaluator,
    BuiltInPlanEvaluator,
    MWAAPlanEvaluator,
    PlanEvaluator,
)
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateSync
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.schedulers.airflow.common import DEFAULT_CATALOG_VARIABLE_NAME
from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.pydantic import model_validator, model_validator_v1_args

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

    @abc.abstractmethod
    def get_default_catalog(self, context: Context) -> t.Optional[str]:
        """Returns the default catalog for the Scheduler.

        Args:
            context: The SQLMesh Context.
        """


class _EngineAdapterStateSyncSchedulerConfig(_SchedulerConfig):
    def create_state_sync(self, context: Context) -> StateSync:
        state_connection = context.config.get_state_connection(context.gateway)
        engine_adapter = (
            state_connection.create_engine_adapter() if state_connection else context.engine_adapter
        )
        if not engine_adapter.SUPPORTS_ROW_LEVEL_OP:
            raise ConfigError(
                f"The {engine_adapter.DIALECT.upper()} engine cannot be used to store SQLMesh state - please specify a different `state_connection` engine."
                + " See https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#gateways for more information."
            )
        schema = context.config.get_state_schema(context.gateway)
        return EngineAdapterStateSync(engine_adapter, schema=schema, console=context.console)


class BuiltInSchedulerConfig(_EngineAdapterStateSyncSchedulerConfig, BaseConfig):
    """The Built-In Scheduler configuration."""

    type_: Literal["builtin"] = Field(alias="type", default="builtin")

    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        return BuiltInPlanEvaluator(
            state_sync=context.state_sync,
            snapshot_evaluator=context.snapshot_evaluator,
            default_catalog=self.get_default_catalog(context),
            backfill_concurrent_tasks=context.concurrent_tasks,
            console=context.console,
            notification_target_manager=context.notification_target_manager,
        )

    def get_default_catalog(self, context: Context) -> t.Optional[str]:
        return context.engine_adapter.default_catalog


class _BaseAirflowSchedulerConfig(_EngineAdapterStateSyncSchedulerConfig):
    dag_run_poll_interval_secs: int
    dag_creation_poll_interval_secs: int
    dag_creation_max_retry_attempts: int

    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int

    use_state_connection: bool

    @abc.abstractmethod
    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        """Constructs the Airflow Client instance."""

    def create_state_sync(self, context: Context) -> StateSync:
        if self.use_state_connection:
            return super().create_state_sync(context)

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
            state_sync=context.state_sync if self.use_state_connection else None,
        )

    def get_default_catalog(self, context: Context) -> t.Optional[str]:
        default_catalog = self.get_client(context.console).get_variable(
            DEFAULT_CATALOG_VARIABLE_NAME
        )
        if not default_catalog:
            raise SQLMeshError(
                "Must define `default_catalog` when creating `SQLMeshAirflow` object. See docs for more info: https://sqlmesh.readthedocs.io/en/stable/integrations/airflow/#airflow-cluster-configuration"
            )
        return default_catalog


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
        max_snapshot_ids_per_request: The maximum number of snapshot IDs that can be sent in a single HTTP GET request to the Airflow Webserver.
        use_state_connection: Whether to use the `state_connection` configuration to access the SQLMesh state.
    """

    airflow_url: str = "http://localhost:8080/"
    username: str = "airflow"
    password: str = "airflow"
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    max_snapshot_ids_per_request: t.Optional[int] = None
    use_state_connection: bool = False

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
            snapshot_ids_batch_size=self.max_snapshot_ids_per_request,
        )


class CloudComposerSchedulerConfig(_BaseAirflowSchedulerConfig, BaseConfig, extra="allow"):
    """The Google Cloud Composer configuration.

    Args:
        airflow_url: The URL of the Airflow Webserver.
        dag_run_poll_interval_secs: Determines how often a running DAG can be polled (in seconds).
        dag_creation_poll_interval_secs: Determines how often SQLMesh should check whether a DAG has been created (in seconds).
        dag_creation_max_retry_attempts: Determines the maximum number of attempts that SQLMesh will make while checking for
            whether a DAG has been created.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
        max_snapshot_ids_per_request: The maximum number of snapshot IDs that can be sent in a single HTTP GET request to the Airflow Webserver.
        use_state_connection: Whether to use the `state_connection` configuration to access the SQLMesh state.
    """

    airflow_url: str
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    max_snapshot_ids_per_request: t.Optional[int] = 40
    use_state_connection: bool = False

    type_: Literal["cloud_composer"] = Field(alias="type", default="cloud_composer")

    _concurrent_tasks_validator = concurrent_tasks_validator

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
            snapshot_ids_batch_size=self.max_snapshot_ids_per_request,
        )

    @model_validator(mode="before")
    @model_validator_v1_args
    def check_supported_fields(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        allowed_field_names = {field.alias or name for name, field in cls.all_field_infos().items()}
        allowed_field_names.add("session")

        for field_name in values:
            if field_name not in allowed_field_names:
                raise ValueError(f"Unsupported Field: {field_name}")
        return values


class MWAASchedulerConfig(_EngineAdapterStateSyncSchedulerConfig, BaseConfig):
    """The AWS MWAA Scheduler configuration.

    Args:
        environment: The name of the MWAA environment.
        dag_run_poll_interval_secs: Determines how often a running DAG can be polled (in seconds).
        dag_creation_poll_interval_secs: Determines how often SQLMesh should check whether a DAG has been created (in seconds).
        dag_creation_max_retry_attempts: Determines the maximum number of attempts that SQLMesh will make while checking for
            whether a DAG has been created.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
    """

    environment: str
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    type_: Literal["mwaa"] = Field(alias="type", default="mwaa")

    _concurrent_tasks_validator = concurrent_tasks_validator

    def get_client(self, console: t.Optional[Console] = None) -> MWAAClient:
        return MWAAClient(self.environment, console=console)

    def create_plan_evaluator(self, context: Context) -> PlanEvaluator:
        return MWAAPlanEvaluator(
            client=self.get_client(context.console),
            state_sync=context.state_sync,
            console=context.console,
            dag_run_poll_interval_secs=self.dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs=self.dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts=self.dag_creation_max_retry_attempts,
            notification_targets=context.notification_targets,
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=context.users,
        )

    def get_default_catalog(self, context: Context) -> t.Optional[str]:
        default_catalog = self.get_client(context.console).get_variable(
            DEFAULT_CATALOG_VARIABLE_NAME
        )
        if not default_catalog:
            raise SQLMeshError(
                "Must define `default_catalog` when creating `SQLMeshAirflow` object. See docs for more info: https://sqlmesh.readthedocs.io/en/stable/integrations/airflow/#airflow-cluster-configuration"
            )
        return default_catalog


SchedulerConfig = Annotated[
    t.Union[
        BuiltInSchedulerConfig,
        AirflowSchedulerConfig,
        CloudComposerSchedulerConfig,
        MWAASchedulerConfig,
    ],
    Field(discriminator="type_"),
]
