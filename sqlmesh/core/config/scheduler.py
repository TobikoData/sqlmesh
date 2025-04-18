from __future__ import annotations

import abc
import typing as t

from pydantic import Field

from sqlglot.helper import subclasses
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.console import get_console
from sqlmesh.core.plan import (
    BuiltInPlanEvaluator,
    PlanEvaluator,
)
from sqlmesh.core.config import DuckDBConnectionConfig
from sqlmesh.core.state_sync import EngineAdapterStateSync, StateSync
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.hashing import md5
from sqlmesh.utils.pydantic import field_validator

if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext

from sqlmesh.utils.config import sensitive_fields, excluded_fields


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

        warehouse_connection = context.config.get_connection(context.gateway)

        if (
            isinstance(state_connection, DuckDBConnectionConfig)
            and state_connection.concurrent_tasks <= 1
        ):
            # If we are using DuckDB, ensure that multithreaded mode gets enabled if necessary
            if warehouse_connection.concurrent_tasks > 1:
                get_console().log_warning(
                    "The duckdb state connection is configured for single threaded mode but the warehouse connection is configured for "
                    + f"multi threaded mode with {warehouse_connection.concurrent_tasks} concurrent tasks."
                    + " This can cause SQLMesh to hang. Overriding the duckdb state connection config to use multi threaded mode."
                )
                # this triggers multithreaded mode and has to happen before the engine adapter is created below
                state_connection.concurrent_tasks = warehouse_connection.concurrent_tasks

        engine_adapter = state_connection.create_engine_adapter()
        if state_connection.is_forbidden_for_state_sync:
            raise ConfigError(
                f"The {engine_adapter.DIALECT.upper()} engine cannot be used to store SQLMesh state - please specify a different `state_connection` engine."
                + " See https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#gateways for more information."
            )

        # If the user is using DuckDB for both the state and the warehouse connection, they are most likely running an example project
        # or POC. To reduce friction, we wont log a warning about DuckDB being used for state until they change to a proper warehouse
        if not isinstance(state_connection, DuckDBConnectionConfig) or not isinstance(
            warehouse_connection, DuckDBConnectionConfig
        ):
            if not state_connection.is_recommended_for_state_sync:
                get_console().log_warning(
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
                    exclude=sensitive_fields.union(excluded_fields),
                )
            ]
        )


class BuiltInSchedulerConfig(_EngineAdapterStateSyncSchedulerConfig, BaseConfig):
    """The Built-In Scheduler configuration."""

    type_: t.Literal["builtin"] = Field(alias="type", default="builtin")

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
