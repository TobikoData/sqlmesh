from __future__ import annotations

import abc
import logging
import sys
import typing as t

from pydantic import Field
from requests import Session

from sqlglot.helper import subclasses
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
from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.hashing import md5
from sqlmesh.utils.pydantic import model_validator, model_validator_v1_args, field_validator

if t.TYPE_CHECKING:
    from google.auth.transport.requests import AuthorizedSession

    from sqlmesh.core.context import GenericContext

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


logger = logging.getLogger(__name__)


class SchedulerConfig(abc.ABC):
    """Abstract base class for Scheduler configurations."""

    @abc.abstractmethod
    def create_plan_evaluator(self, context: GenericContext) -> PlanEvaluator:
        """Creates a Plan Evaluator instance.

        Args:
            context: The SQLMesh Context.
        """

    @abc.abstractmethod
    def create_state_sync(self, context: GenericContext) -> StateSync:
        """Creates a State Sync instance.

        Args:
            context: The SQLMesh Context.

        Returns:
            The StateSync instance.
        """

    @abc.abstractmethod
    def get_default_catalog(self, context: GenericContext) -> t.Optional[str]:
        """Returns the default catalog for the Scheduler.

        Args:
            context: The SQLMesh Context.
        """

    @abc.abstractmethod
    def state_sync_fingerprint(self, context: GenericContext) -> str:
        """Returns the fingerprint of the State Sync configuration.

        Args:
            context: The SQLMesh Context.
        """


class _EngineAdapterStateSyncSchedulerConfig(SchedulerConfig):
    def create_state_sync(self, context: GenericContext) -> StateSync:
        state_connection = (
            context.config.get_state_connection(context.gateway) or context._connection_config
        )
        engine_adapter = state_connection.create_engine_adapter()
        if state_connection.is_forbidden_for_state_sync:
            raise ConfigError(
                f"The {engine_adapter.DIALECT.upper()} engine cannot be used to store SQLMesh state - please specify a different `state_connection` engine."
                + " See https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#gateways for more information."
            )
        if not state_connection.is_recommended_for_state_sync:
            logger.warning(
                f"The {state_connection.type_} engine is not recommended for storing SQLMesh state in production deployments. Please see"
                + " https://sqlmesh.readthedocs.io/en/stable/guides/configuration/#state-connection for a list of recommended engines and more information."
            )
        schema = context.config.get_state_schema(context.gateway)
        return EngineAdapterStateSync(
            engine_adapter, schema=schema, context_path=context.path, console=context.console
        )

    def state_sync_fingerprint(self, context: GenericContext) -> str:
        state_connection = (
            context.config.get_state_connection(context.gateway) or context._connection_config
        )
        return md5(
            [
                state_connection.json(
                    sort_keys=True,
                    exclude={
                        "access_token",
                        "concurrent_tasks",
                        "user",
                        "password",
                        "keytab",
                        "keyfile",
                        "keyfile_json",
                        "pre_ping",
                        "principal",
                        "private_key",
                        "private_key_passphrase",
                        "private_key_path",
                        "refresh_token",
                        "register_comments",
                        "token",
                    },
                )
            ]
        )


class BuiltInSchedulerConfig(_EngineAdapterStateSyncSchedulerConfig, BaseConfig):
    """The Built-In Scheduler configuration."""

    type_: Literal["builtin"] = Field(alias="type", default="builtin")

    def create_plan_evaluator(self, context: GenericContext) -> PlanEvaluator:
        return BuiltInPlanEvaluator(
            state_sync=context.state_sync,
            snapshot_evaluator=context.snapshot_evaluator,
            create_scheduler=context.create_scheduler,
            default_catalog=self.get_default_catalog(context),
            console=context.console,
        )

    def get_default_catalog(self, context: GenericContext) -> t.Optional[str]:
        return context.engine_adapter.default_catalog


class _BaseAirflowSchedulerConfig(_EngineAdapterStateSyncSchedulerConfig):
    airflow_url: str
    dag_run_poll_interval_secs: int
    dag_creation_poll_interval_secs: int
    dag_creation_max_retry_attempts: int

    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int

    use_state_connection: bool

    default_catalog_override: t.Optional[str]

    @abc.abstractmethod
    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        """Constructs the Airflow Client instance."""

    def create_state_sync(self, context: GenericContext) -> StateSync:
        if self.use_state_connection:
            return super().create_state_sync(context)

        from sqlmesh.schedulers.airflow.state_sync import HttpStateSync

        return HttpStateSync(
            client=self.get_client(context.console),
            dag_run_poll_interval_secs=self.dag_run_poll_interval_secs,
            console=context.console,
        )

    def state_sync_fingerprint(self, context: GenericContext) -> str:
        if self.use_state_connection:
            return super().state_sync_fingerprint(context)
        return md5([self.airflow_url])

    def create_plan_evaluator(self, context: GenericContext) -> PlanEvaluator:
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

    def get_default_catalog(self, context: GenericContext) -> t.Optional[str]:
        default_catalog = self.get_client(context.console).default_catalog
        return self.default_catalog_override or default_catalog


def _max_snapshot_ids_per_request_validator(v: t.Any) -> t.Optional[int]:
    logger.warning(
        "The `max_snapshot_ids_per_request` field is deprecated and will be removed in a future release."
    )
    return None


max_snapshot_ids_per_request_validator: t.Any = field_validator(
    "max_snapshot_ids_per_request", mode="before"
)(_max_snapshot_ids_per_request_validator)


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
        max_snapshot_ids_per_request: The maximum number of snapshot IDs that can be sent in a single HTTP GET request to the Airflow Webserver (Deprecated).
        use_state_connection: Whether to use the `state_connection` configuration to access the SQLMesh state.
        default_catalog_override: Overrides the default catalog value for this project. If specified, this value takes precedence
            over the default catalog value set on the Airflow side.
    """

    airflow_url: str = "http://localhost:8080/"
    username: str = "airflow"
    password: str = "airflow"
    token: t.Optional[str] = None
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    max_snapshot_ids_per_request: t.Optional[int] = None
    use_state_connection: bool = False

    default_catalog_override: t.Optional[str] = None

    type_: Literal["airflow"] = Field(alias="type", default="airflow")

    _concurrent_tasks_validator = concurrent_tasks_validator
    _max_snapshot_ids_per_request_validator = max_snapshot_ids_per_request_validator

    def get_client(self, console: t.Optional[Console] = None) -> AirflowClient:
        session = Session()
        if self.token is None:
            session.auth = (self.username, self.password)
        else:
            session.headers.update({"Authorization": f"Bearer {self.token}"})

        return AirflowClient(
            session=session,
            airflow_url=self.airflow_url,
            console=console,
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
        max_snapshot_ids_per_request: The maximum number of snapshot IDs that can be sent in a single HTTP GET request to the Airflow Webserver (Deprecated).
        use_state_connection: Whether to use the `state_connection` configuration to access the SQLMesh state.
        default_catalog_override: Overrides the default catalog value for this project. If specified, this value takes precedence
            over the default catalog value set on the Airflow side.
    """

    airflow_url: str
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    max_snapshot_ids_per_request: t.Optional[int] = 20
    use_state_connection: bool = False

    default_catalog_override: t.Optional[str] = None

    type_: Literal["cloud_composer"] = Field(alias="type", default="cloud_composer")

    _concurrent_tasks_validator = concurrent_tasks_validator
    _max_snapshot_ids_per_request_validator = max_snapshot_ids_per_request_validator

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
        default_catalog_override: Overrides the default catalog value for this project. If specified, this value takes precedence
            over the default catalog value set on the Airflow side.
    """

    environment: str
    dag_run_poll_interval_secs: int = 10
    dag_creation_poll_interval_secs: int = 30
    dag_creation_max_retry_attempts: int = 10

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4

    default_catalog_override: t.Optional[str] = None

    type_: Literal["mwaa"] = Field(alias="type", default="mwaa")

    _concurrent_tasks_validator = concurrent_tasks_validator

    def get_client(self, console: t.Optional[Console] = None) -> MWAAClient:
        return MWAAClient(self.environment, console=console)

    def create_plan_evaluator(self, context: GenericContext) -> PlanEvaluator:
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

    def get_default_catalog(self, context: GenericContext) -> t.Optional[str]:
        default_catalog = self.get_client(context.console).default_catalog
        return self.default_catalog_override or default_catalog


SCHEDULER_CONFIG_TO_TYPE = {
    tpe.all_field_infos()["type_"].default: tpe
    for tpe in subclasses(__name__, BaseConfig, exclude=(BaseConfig,))
}


def _scheduler_config_validator(
    cls: t.Type, v: SchedulerConfig | t.Dict[str, t.Any] | None
) -> SchedulerConfig | None:
    if v is None or isinstance(v, SchedulerConfig):
        return v

    if "type" not in v:
        raise ConfigError("Missing scheduler type.")

    scheduler_type = v["type"]
    if scheduler_type not in SCHEDULER_CONFIG_TO_TYPE:
        raise ConfigError(f"Unknown scheduler type '{scheduler_type}'.")

    return SCHEDULER_CONFIG_TO_TYPE[scheduler_type](**v)


scheduler_config_validator = field_validator(
    "scheduler",
    "default_scheduler",
    mode="before",
    check_fields=False,
)(_scheduler_config_validator)
