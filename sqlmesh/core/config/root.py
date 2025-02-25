from __future__ import annotations

import pickle
import re
import typing as t
import zlib

from pydantic import Field
from sqlglot import exp
from sqlglot.helper import first
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.cicd.config import CICDBotConfig
from sqlmesh.core import constants as c
from sqlmesh.core.console import get_console
from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.core.config.common import variables_validator, compile_regex_mapping
from sqlmesh.core.config.connection import (
    ConnectionConfig,
    DuckDBConnectionConfig,
    SerializableConnectionConfig,
    connection_config_validator,
)
from sqlmesh.core.config.feature_flag import FeatureFlag
from sqlmesh.core.config.format import FormatConfig
from sqlmesh.core.config.gateway import GatewayConfig
from sqlmesh.core.config.migration import MigrationConfig
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.core.config.naming import NameInferenceConfig as NameInferenceConfig
from sqlmesh.core.config.plan import PlanConfig
from sqlmesh.core.config.run import RunConfig
from sqlmesh.core.config.scheduler import (
    BuiltInSchedulerConfig,
    SchedulerConfig,
    scheduler_config_validator,
)
from sqlmesh.core.config.ui import UIConfig
from sqlmesh.core.loader import Loader, SqlMeshLoader
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.user import User
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import field_validator, model_validator

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import Self


class Config(BaseConfig):
    """An object used by a Context to configure your SQLMesh project.

    Args:
        gateways: Supported gateways and their configurations. Key represents a unique name of a gateway.
        default_connection: The default connection to use if one is not specified in a gateway.
        default_test_connection: The default connection to use for tests if one is not specified in a gateway.
        default_scheduler: The default scheduler configuration to use if one is not specified in a gateway.
        default_gateway: The default gateway.
        notification_targets: The notification targets to use.
        project: The project name of this config. Used for multi-repo setups.
        snapshot_ttl: The period of time that a model snapshot that is not a part of any environment should exist before being deleted.
        environment_ttl: The period of time that a development environment should exist before being deleted.
        ignore_patterns: Files that match glob patterns specified in this list are ignored when scanning the project folder.
        time_column_format: The default format to use for all model time columns. Defaults to %Y-%m-%d.
            This time format uses python format codes. https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.
        users: A list of users that can be used for approvals/notifications.
        username: Name of a single user who should receive approvals/notification, instead of all users in the `users` list.
        pinned_environments: A list of development environment names that should not be deleted by the janitor task.
        loader: Loader class used for loading project files.
        loader_kwargs: Key-value arguments to pass to the loader instance.
        env_vars: A dictionary of environmental variable names and values.
        model_defaults: Default values for model definitions.
        physical_schema_mapping: A mapping from regular expressions to names of schemas in which physical tables for corresponding models will be placed.
        environment_suffix_target: Indicates whether to append the environment name to the schema or table name.
        environment_catalog_mapping: A mapping from regular expressions to catalog names. The catalog name is used to determine the target catalog for a given environment.
        default_target_environment: The name of the environment that will be the default target for the `sqlmesh plan` and `sqlmesh run` commands.
        log_limit: The default number of logs to keep.
        format: The formatting options for SQL code.
        ui: The UI configuration for SQLMesh.
        feature_flags: Feature flags to enable/disable certain features.
        plan: The plan configuration.
        migration: The migration configuration.
        variables: A dictionary of variables that can be used in models / macros.
        disable_anonymized_analytics: Whether to disable the anonymized analytics collection.
        before_all: SQL statements or macros to be executed at the start of the `sqlmesh plan` and `sqlmesh run` commands.
        after_all: SQL statements or macros to be executed at the end of the `sqlmesh plan` and `sqlmesh run` commands.
    """

    gateways: t.Dict[str, GatewayConfig] = {"": GatewayConfig()}
    default_connection: SerializableConnectionConfig = DuckDBConnectionConfig()
    default_test_connection_: t.Optional[SerializableConnectionConfig] = Field(
        default=None, alias="default_test_connection"
    )
    default_scheduler: SchedulerConfig = BuiltInSchedulerConfig()
    default_gateway: str = ""
    notification_targets: t.List[NotificationTarget] = []
    project: str = ""
    snapshot_ttl: str = c.DEFAULT_SNAPSHOT_TTL
    environment_ttl: t.Optional[str] = c.DEFAULT_ENVIRONMENT_TTL
    ignore_patterns: t.List[str] = c.IGNORE_PATTERNS
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    users: t.List[User] = []
    model_defaults: ModelDefaultsConfig = ModelDefaultsConfig()
    pinned_environments: t.Set[str] = set()
    loader: t.Type[Loader] = SqlMeshLoader
    loader_kwargs: t.Dict[str, t.Any] = {}
    env_vars: t.Dict[str, str] = {}
    username: str = ""
    physical_schema_mapping: t.Dict[re.Pattern, str] = {}
    environment_suffix_target: EnvironmentSuffixTarget = Field(
        default=EnvironmentSuffixTarget.default
    )
    environment_catalog_mapping: t.Dict[re.Pattern, str] = {}
    default_target_environment: str = c.PROD
    log_limit: int = c.DEFAULT_LOG_LIMIT
    cicd_bot: t.Optional[CICDBotConfig] = None
    run: RunConfig = RunConfig()
    format: FormatConfig = FormatConfig()
    ui: UIConfig = UIConfig()
    feature_flags: FeatureFlag = FeatureFlag()
    plan: PlanConfig = PlanConfig()
    migration: MigrationConfig = MigrationConfig()
    model_naming: NameInferenceConfig = NameInferenceConfig()
    variables: t.Dict[str, t.Any] = {}
    disable_anonymized_analytics: bool = False
    before_all: t.Optional[t.List[str]] = None
    after_all: t.Optional[t.List[str]] = None

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "gateways": UpdateStrategy.NESTED_UPDATE,
        "notification_targets": UpdateStrategy.EXTEND,
        "ignore_patterns": UpdateStrategy.EXTEND,
        "users": UpdateStrategy.EXTEND,
        "model_defaults": UpdateStrategy.NESTED_UPDATE,
        "auto_categorize_changes": UpdateStrategy.NESTED_UPDATE,
        "pinned_environments": UpdateStrategy.EXTEND,
        "physical_schema_override": UpdateStrategy.KEY_UPDATE,
        "run": UpdateStrategy.NESTED_UPDATE,
        "format": UpdateStrategy.NESTED_UPDATE,
        "ui": UpdateStrategy.NESTED_UPDATE,
        "loader_kwargs": UpdateStrategy.KEY_UPDATE,
        "plan": UpdateStrategy.NESTED_UPDATE,
    }

    _connection_config_validator = connection_config_validator
    _scheduler_config_validator = scheduler_config_validator
    _variables_validator = variables_validator

    @field_validator("gateways", mode="before")
    @classmethod
    def _gateways_ensure_dict(cls, value: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        try:
            if not isinstance(value, GatewayConfig):
                GatewayConfig.parse_obj(value)
            return {"": value}
        except Exception:
            return value

    @field_validator("environment_catalog_mapping", "physical_schema_mapping", mode="before")
    @classmethod
    def _validate_regex_keys(
        cls, value: t.Dict[str | re.Pattern, t.Any]
    ) -> t.Dict[re.Pattern, t.Any]:
        return compile_regex_mapping(value)

    @model_validator(mode="before")
    def _normalize_and_validate_fields(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        if "gateways" not in data and "gateway" in data:
            data["gateways"] = data.pop("gateway")

        for plan_deprecated in ("auto_categorize_changes", "include_unmodified"):
            if plan_deprecated in data:
                raise ConfigError(
                    f"The `{plan_deprecated}` config is deprecated. Please use the `plan.{plan_deprecated}` config instead."
                )

        if "physical_schema_override" in data:
            get_console().log_warning(
                "`physical_schema_override` is deprecated. Please use `physical_schema_mapping` instead."
            )

            if "physical_schema_mapping" in data:
                raise ConfigError(
                    "Only one of `physical_schema_override` and `physical_schema_mapping` can be specified."
                )

            physical_schema_override: t.Dict[str, str] = data.pop("physical_schema_override")
            # translate physical_schema_override to physical_schema_mapping
            data["physical_schema_mapping"] = {
                f"^{k}$": v for k, v in physical_schema_override.items()
            }

        return data

    @model_validator(mode="after")
    def _normalize_fields_after(self) -> Self:
        dialect = self.model_defaults.dialect

        def _normalize_identifiers(key: str) -> None:
            setattr(
                self,
                key,
                {
                    k: normalize_identifiers(v, dialect=dialect).name
                    for k, v in getattr(self, key, {}).items()
                },
            )

        if self.environment_catalog_mapping:
            _normalize_identifiers("environment_catalog_mapping")
        if self.physical_schema_mapping:
            _normalize_identifiers("physical_schema_mapping")

        return self

    def get_default_test_connection(
        self,
        default_catalog: t.Optional[str] = None,
        default_catalog_dialect: t.Optional[str] = None,
    ) -> ConnectionConfig:
        return self.default_test_connection_ or DuckDBConnectionConfig(
            catalogs=(
                None
                if default_catalog is None
                else {
                    # transpile catalog name from main connection dialect to DuckDB
                    exp.parse_identifier(default_catalog, dialect=default_catalog_dialect).sql(
                        dialect="duckdb"
                    ): ":memory:"
                }
            )
        )

    def get_gateway(self, name: t.Optional[str] = None) -> GatewayConfig:
        if isinstance(self.gateways, dict):
            if name is None:
                if self.default_gateway:
                    if self.default_gateway not in self.gateways:
                        raise ConfigError(f"Missing gateway with name '{self.default_gateway}'")
                    return self.gateways[self.default_gateway]

                if "" in self.gateways:
                    return self.gateways[""]

                return first(self.gateways.values())

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

    def get_test_connection(
        self,
        gateway_name: t.Optional[str] = None,
        default_catalog: t.Optional[str] = None,
        default_catalog_dialect: t.Optional[str] = None,
    ) -> ConnectionConfig:
        return self.get_gateway(gateway_name).test_connection or self.get_default_test_connection(
            default_catalog=default_catalog, default_catalog_dialect=default_catalog_dialect
        )

    def get_scheduler(self, gateway_name: t.Optional[str] = None) -> SchedulerConfig:
        return self.get_gateway(gateway_name).scheduler or self.default_scheduler

    def get_state_schema(self, gateway_name: t.Optional[str] = None) -> t.Optional[str]:
        return self.get_gateway(gateway_name).state_schema

    @property
    def default_gateway_name(self) -> str:
        if self.default_gateway:
            return self.default_gateway
        if "" in self.gateways:
            return ""
        return first(self.gateways)

    @property
    def dialect(self) -> t.Optional[str]:
        return self.model_defaults.dialect

    @property
    def fingerprint(self) -> str:
        return str(zlib.crc32(pickle.dumps(self.dict(exclude={"loader", "notification_targets"}))))
