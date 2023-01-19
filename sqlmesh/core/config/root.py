from __future__ import annotations

import typing as t

from pydantic import validator

from sqlmesh.core import constants as c
from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.config.connection import ConnectionConfig, DuckDBConnectionConfig
from sqlmesh.core.config.scheduler import BuiltInSchedulerConfig, SchedulerConfig
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    pass


class Config(PydanticModel):
    """
    An object used by a Context to configure your SQLMesh project.

    Args:
        connections: Supported connections and their configurations. Key represents a unique name of a connection.
            Only applicable when used with the built-in scheduler.
        scheduler: The scheduler configuration.
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

    connections: t.Union[
        t.Dict[str, ConnectionConfig], ConnectionConfig
    ] = DuckDBConnectionConfig()
    scheduler: SchedulerConfig = BuiltInSchedulerConfig()
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

    def get_connection_config(self, name: t.Optional[str] = None) -> ConnectionConfig:
        if isinstance(self.connections, dict):
            if name is None:
                return next(iter(self.connections.values()))

            if name not in self.connections:
                raise ConfigError(f"Missing connection with name '{name}'.")

            return self.connections[name]
        else:
            if name is not None:
                raise ConfigError(
                    "Connection name is not supported when only one connection is configured."
                )
            return self.connections

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
