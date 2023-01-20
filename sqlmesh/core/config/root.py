from __future__ import annotations

import typing as t

from pydantic import Field, root_validator, validator

from sqlmesh.core import constants as c
from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.core.config.connection import ConnectionConfig, DuckDBConnectionConfig
from sqlmesh.core.config.scheduler import BuiltInSchedulerConfig, SchedulerConfig
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError


class Config(BaseConfig):
    """An object used by a Context to configure your SQLMesh project.

    Args:
        connections: Supported connections and their configurations. Key represents a unique name of a connection.
        default_connection: The name of a connection to use by default.
        test_connection: The connection settings for tests. Can be a name which refers to an existing configuration
            in `connections`.
        scheduler: The scheduler configuration.
        notification_targets: The notification targets to use.
        dialect: The default sql dialect of model queries. Default: same as engine dialect.
        physical_schema: The default schema used to store materialized tables.
        snapshot_ttl: Duration before unpromoted snapshots are removed.
        time_column_format: The default format to use for all model time columns. Defaults to %Y-%m-%d.
            This time format uses python format codes. https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.
        users: A list of users that can be used for approvals/notifications.
    """

    connections: t.Union[
        t.Dict[str, ConnectionConfig], ConnectionConfig
    ] = DuckDBConnectionConfig()
    default_connection: str = ""
    test_connection_: t.Union[ConnectionConfig, str] = Field(
        alias="test_connection", default=DuckDBConnectionConfig()
    )
    scheduler: SchedulerConfig = BuiltInSchedulerConfig()
    notification_targets: t.List[NotificationTarget] = []
    dialect: str = ""
    physical_schema: str = ""
    snapshot_ttl: str = ""
    ignore_patterns: t.List[str] = []
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    users: t.List[User] = []

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "connections": UpdateStrategy.KEY_UPDATE,
        "notification_targets": UpdateStrategy.EXTEND,
        "ignore_patterns": UpdateStrategy.EXTEND,
        "users": UpdateStrategy.EXTEND,
    }

    @validator("connections", always=True)
    def _connections_ensure_dict(
        cls, value: t.Union[t.Dict[str, ConnectionConfig], ConnectionConfig]
    ) -> t.Dict[str, ConnectionConfig]:
        if not isinstance(value, dict):
            return {"": value}
        return value

    @root_validator(pre=True)
    def _normalize_fields(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        if "connections" not in values and "connection" in values:
            values["connections"] = values.pop("connection")

        return values

    def get_connection(self, name: t.Optional[str] = None) -> ConnectionConfig:
        if isinstance(self.connections, dict):
            if name is None:
                if self.default_connection:
                    if self.default_connection not in self.connections:
                        raise ConfigError(
                            f"Missing connection with name '{self.default_connection}'"
                        )
                    return self.connections[self.default_connection]

                if "" in self.connections:
                    return self.connections[""]

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

    @property
    def test_connection(self) -> ConnectionConfig:
        if isinstance(self.test_connection_, str):
            return self.get_connection(self.test_connection_)
        return self.test_connection_
