from __future__ import annotations

import typing as t

from pydantic import root_validator, validator

from sqlmesh.core import constants as c
from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.core.config.categorizer import CategorizerConfig
from sqlmesh.core.config.connection import ConnectionConfig, DuckDBConnectionConfig
from sqlmesh.core.config.gateway import GatewayConfig
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.core.config.scheduler import BuiltInSchedulerConfig, SchedulerConfig
from sqlmesh.core.loader import Loader, SqlMeshLoader
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.hashing import crc32


class Config(BaseConfig):
    """An object used by a Context to configure your SQLMesh project.

    Args:
        gateways: Supported gateways and their configurations. Key represents a unique name of a gateway.
        default_connection: The default connection to use if one is not specified in a gateway.
        default_test_connection: The default connection to use for tests if one is not specified in a gateway.
        default_scheduler: The default scheduler configuration to use if one is not specified in a gateway.
        default_gateway: The default gateway.
        notification_targets: The notification targets to use.
        dialect: The default sql dialect of model queries. Default: same as engine dialect.
        project: The project name of this config. Used for multi-repo setups.
        snapshot_ttl: The period of time that a model snapshot that is not a part of any environment should exist before being deleted.
        environment_ttl: The period of time that a development environment should exist before being deleted.
        ignore_patterns: Files that match glob patterns specified in this list are ignored when scanning the project folder.
        time_column_format: The default format to use for all model time columns. Defaults to %Y-%m-%d.
            This time format uses python format codes. https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.
        auto_categorize_changes: Indicates whether SQLMesh should attempt to automatically categorize model changes (breaking / non-breaking)
            during plan creation.
        users: A list of users that can be used for approvals/notifications.
        pinned_environments: A list of development environment names that should not be deleted by the janitor task.
        model_defaults: Default values for model definitions.
        include_unmodified: Indicates whether to include unmodified models in the target development environment.
    """

    gateways: t.Union[t.Dict[str, GatewayConfig], GatewayConfig] = GatewayConfig()
    default_connection: ConnectionConfig = DuckDBConnectionConfig()
    default_test_connection: ConnectionConfig = DuckDBConnectionConfig()
    default_scheduler: SchedulerConfig = BuiltInSchedulerConfig()
    default_gateway: str = ""
    notification_targets: t.List[NotificationTarget] = []
    project: str = ""
    snapshot_ttl: str = c.DEFAULT_SNAPSHOT_TTL
    environment_ttl: t.Optional[str] = c.DEFAULT_ENVIRONMENT_TTL
    ignore_patterns: t.List[str] = c.IGNORE_PATTERNS
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    auto_categorize_changes: CategorizerConfig = CategorizerConfig()
    users: t.List[User] = []
    model_defaults: ModelDefaultsConfig = ModelDefaultsConfig()
    pinned_environments: t.Set[str] = set()
    loader: t.Type[Loader] = SqlMeshLoader
    env_vars: t.Dict[str, str] = {}
    username: str = ""
    include_unmodified: bool = False
    physical_schema_override: t.Dict[str, str] = {}

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "gateways": UpdateStrategy.KEY_UPDATE,
        "notification_targets": UpdateStrategy.EXTEND,
        "ignore_patterns": UpdateStrategy.EXTEND,
        "users": UpdateStrategy.EXTEND,
        "model_defaults": UpdateStrategy.NESTED_UPDATE,
        "auto_categorize_changes": UpdateStrategy.NESTED_UPDATE,
        "pinned_environments": UpdateStrategy.EXTEND,
        "physical_schema_override": UpdateStrategy.KEY_UPDATE,
    }

    @validator("gateways", always=True)
    def _gateways_ensure_dict(
        cls, value: t.Union[t.Dict[str, GatewayConfig], GatewayConfig]
    ) -> t.Dict[str, GatewayConfig]:
        if not isinstance(value, dict):
            return {"": value}
        return value

    @root_validator(pre=True)
    def _normalize_fields(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        if "gateways" not in values and "gateway" in values:
            values["gateways"] = values.pop("gateway")

        return values

    def get_gateway(self, name: t.Optional[str] = None) -> GatewayConfig:
        if isinstance(self.gateways, dict):
            if name is None:
                if self.default_gateway:
                    if self.default_gateway not in self.gateways:
                        raise ConfigError(f"Missing gateway with name '{self.default_gateway}'")
                    return self.gateways[self.default_gateway]

                if "" in self.gateways:
                    return self.gateways[""]

                return next(iter(self.gateways.values()))

            if name not in self.gateways:
                raise ConfigError(f"Missing gateway with name '{name}'.")

            return self.gateways[name]
        else:
            if name is not None:
                raise ConfigError(
                    "Gateway name is not supported when only one gateway is configured."
                )
            return self.gateways

    def get_connection(self, gateway_name: t.Optional[str] = None) -> ConnectionConfig:
        return self.get_gateway(gateway_name).connection or self.default_connection

    def get_state_connection(
        self, gateway_name: t.Optional[str] = None
    ) -> t.Optional[ConnectionConfig]:
        return self.get_gateway(gateway_name).state_connection

    def get_test_connection(self, gateway_name: t.Optional[str] = None) -> ConnectionConfig:
        return self.get_gateway(gateway_name).test_connection or self.default_test_connection

    def get_scheduler(self, gateway_name: t.Optional[str] = None) -> SchedulerConfig:
        return self.get_gateway(gateway_name).scheduler or self.default_scheduler

    def get_state_schema(self, gateway_name: t.Optional[str] = None) -> t.Optional[str]:
        return self.get_gateway(gateway_name).state_schema

    @property
    def dialect(self) -> t.Optional[str]:
        return self.model_defaults.dialect

    @property
    def fingerprint(self) -> str:
        return crc32(self.json(exclude={"loader"}))
