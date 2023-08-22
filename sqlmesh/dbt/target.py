from __future__ import annotations

import abc
import sys
import typing as t
from pathlib import Path

from dbt.adapters.base import BaseRelation, Column
from dbt.contracts.relation import Policy
from pydantic import Field

from sqlmesh.core.config.connection import (
    BigQueryConnectionConfig,
    BigQueryConnectionMethod,
    BigQueryPriority,
    ConnectionConfig,
    DatabricksConnectionConfig,
    DuckDBConnectionConfig,
    PostgresConnectionConfig,
    RedshiftConnectionConfig,
    SnowflakeConnectionConfig,
)
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    IncrementalUnmanagedKind,
)
from sqlmesh.dbt.common import DbtConfig
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import field_validator, model_validator

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal

IncrementalKind = t.Union[
    t.Type[IncrementalByUniqueKeyKind],
    t.Type[IncrementalByTimeRangeKind],
    t.Type[IncrementalUnmanagedKind],
]


class TargetConfig(abc.ABC, DbtConfig):
    """
    Configuration for DBT profile target

    Args:
        type: The type of the data warehouse
        name: The name of this target
        database: Name of the database
        schema_: Name of the schema
        threads: The number of threads to run on
    """

    # dbt
    type: str = "none"
    name: str
    database: str
    schema_: str = Field(alias="schema")
    threads: int = 1
    profile_name: t.Optional[str] = None

    @classmethod
    def load(cls, data: t.Dict[str, t.Any]) -> TargetConfig:
        """
        Loads the configuration from the yaml provided for a profile target

        Args:
            data: The yaml for the project's target output

        Returns:
            The configuration of the provided profile target
        """
        db_type = data["type"]
        if db_type == "databricks":
            return DatabricksConfig(**data)
        elif db_type == "duckdb":
            return DuckDbConfig(**data)
        elif db_type == "postgres":
            return PostgresConfig(**data)
        elif db_type == "redshift":
            return RedshiftConfig(**data)
        elif db_type == "snowflake":
            return SnowflakeConfig(**data)
        elif db_type == "bigquery":
            return BigQueryConfig(**data)

        raise ConfigError(f"{db_type} not supported.")

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        """The default incremental strategy for the db"""
        raise NotImplementedError

    def to_sqlmesh(self) -> ConnectionConfig:
        """Converts target config to SQLMesh connection config"""
        raise NotImplementedError

    def attribute_dict(self) -> AttributeDict:
        fields = self.dict().copy()
        fields["target_name"] = self.name
        return AttributeDict(fields)

    @property
    def quote_policy(self) -> Policy:
        return Policy()

    @property
    def extra(self) -> t.Set[str]:
        return self.extra_fields(set(self.dict()))

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        return BaseRelation

    @property
    def column_class(self) -> t.Type[Column]:
        return Column


DUCKDB_IN_MEMORY = ":memory:"


class DuckDbConfig(TargetConfig):
    """
    Connection config for DuckDb target

    Args:
        path: Location of the database file. If not specified, an in memory database is used.
    """

    type: Literal["duckdb"] = "duckdb"
    database: str = "main"
    schema_: str = Field(default="main", alias="schema")
    path: str = DUCKDB_IN_MEMORY

    @model_validator(mode="before")
    @classmethod
    def validate_authentication(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        if "database" not in values and DBT_VERSION >= (1, 5):
            path = values.get("path")
            values["database"] = (
                "memory"
                if path is None or path == DUCKDB_IN_MEMORY
                else Path(t.cast(str, path)).stem
            )
        return values

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "delete+insert"

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        from dbt.adapters.duckdb.relation import DuckDBRelation

        return DuckDBRelation

    def to_sqlmesh(self) -> ConnectionConfig:
        return DuckDBConnectionConfig(database=self.path, concurrent_tasks=self.threads)


class SnowflakeConfig(TargetConfig):
    """
    Project connection and operational configuration for the Snowflake target

    Args:
        account: Snowflake account
        warehouse: Name of the warehouse
        user: Name of the user
        password: User's password
        role: Role of the user
        client_session_keep_alive: A boolean flag to extend the duration of the Snowflake session beyond 4 hours
        query_tag: tag for the query in Snowflake
        connect_retries: Number of times to retry if the Snowflake connector encounters an error
        connect_timeout: Number of seconds to wait between failed attempts
        retry_on_database_errors: A boolean flag to retry if a Snowflake connector Database error is encountered
        retry_all: A boolean flag to retry on all Snowflake connector errors
    """

    type: Literal["snowflake"] = "snowflake"
    account: str
    user: str

    # User and password authentication
    password: t.Optional[str] = None

    # SSO authentication
    authenticator: t.Optional[str] = None

    # TODO add other forms of authentication

    # Optional
    warehouse: t.Optional[str] = None
    role: t.Optional[str] = None
    client_session_keep_alive: bool = False
    query_tag: t.Optional[str] = None
    connect_retries: int = 0
    connect_timeout: int = 10
    retry_on_database_errors: bool = False
    retry_all: bool = False

    @model_validator(mode="before")
    @classmethod
    def validate_authentication(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        if values.get("password") or values.get("authenticator"):
            return values
        raise ConfigError("No supported Snowflake authentication method found in target profile.")

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        from dbt.adapters.snowflake import SnowflakeRelation

        return SnowflakeRelation

    @property
    def column_class(self) -> t.Type[Column]:
        from dbt.adapters.snowflake import SnowflakeColumn

        return SnowflakeColumn

    def to_sqlmesh(self) -> ConnectionConfig:
        return SnowflakeConnectionConfig(
            user=self.user,
            password=self.password,
            authenticator=self.authenticator,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            concurrent_tasks=self.threads,
        )

    @property
    def quote_policy(self) -> Policy:
        return Policy(database=False, schema=False, identifier=False)


class PostgresConfig(TargetConfig):
    """
    Project connection and operational configuration for the Postgres target

    Args:
        host: The Postgres host to connect to
        user: Name of the user
        password: User's password
        port: The port to connect to
        dbname: Name of the database
        keepalives_idle: Seconds between TCP keepalive packets
        connect_timeout: Number of seconds to wait between failed attempts
        retries: Number of times to retry if the Postgres connector encounters an error
        search_path: Overrides the default search path
        role: Role of the user
        sslmode: SSL Mode used to connect to the database
    """

    type: Literal["postgres"] = "postgres"
    host: str
    user: str
    password: str
    port: int
    dbname: str
    keepalives_idle: t.Optional[int] = None
    connect_timeout: int = 10
    retries: int = 1  # Currently Unsupported by SQLMesh
    search_path: t.Optional[str] = None  # Currently Unsupported by SQLMesh
    role: t.Optional[str] = None
    sslmode: t.Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def validate_database(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        values["database"] = values.get("database") or values.get("dbname")
        if not values["database"]:
            raise ConfigError("Either database or dbname must be set")
        return values

    @field_validator("port")
    @classmethod
    def _validate_port(cls, v: t.Union[int, str]) -> int:
        return int(v)

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "delete+insert" if kind is IncrementalByUniqueKeyKind else "append"

    def to_sqlmesh(self) -> ConnectionConfig:
        return PostgresConnectionConfig(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=self.dbname,
            keepalives_idle=self.keepalives_idle,
            concurrent_tasks=self.threads,
            connect_timeout=self.connect_timeout,
            role=self.role,
            sslmode=self.sslmode,
        )


class RedshiftConfig(TargetConfig):
    """
    Project connection and operational configuration for the Redshift target

    Args:
        host: The Redshift host to connect to
        user: Name of the user
        password: User's password
        port: The port to connect to
        dbname: Name of the database
        keepalives_idle: Seconds between TCP keepalive packets
        connect_timeout: Number of seconds to wait between failed attempts
        ra3_node: Enables cross-database sources
        search_path: Overrides the default search path
        sslmode: SSL Mode used to connect to the database
    """

    # TODO add other forms of authentication
    type: Literal["redshift"] = "redshift"
    host: str
    user: str
    password: str
    port: int
    dbname: str
    keepalives_idle: int = 240
    connect_timeout: int = 10
    ra3_node: bool = True
    search_path: t.Optional[str] = None
    sslmode: t.Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def validate_database(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        values["database"] = values.get("database") or values.get("dbname")
        if not values["database"]:
            raise ConfigError("Either database or dbname must be set")
        return values

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "append"

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        from dbt.adapters.redshift import RedshiftRelation

        return RedshiftRelation

    @property
    def column_class(self) -> t.Type[Column]:
        if DBT_VERSION < (1, 6):
            from dbt.adapters.redshift import RedshiftColumn  # type: ignore

            return RedshiftColumn
        else:
            return super().column_class

    def to_sqlmesh(self) -> ConnectionConfig:
        return RedshiftConnectionConfig(
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
            sslmode=self.sslmode,
            timeout=self.connect_timeout,
            concurrent_tasks=self.threads,
        )


class DatabricksConfig(TargetConfig):
    """
    Project connection and operational configuration for the Databricks target

    Args:
        catalog: Catalog name to use for Unity Catalog
        host: The Databricks host to connect to
        http_path: The Databricks compute resources URL
        token: Personal access token
        database: Name of the database. Not applicable for Databricks and ignored
    """

    type: Literal["databricks"] = "databricks"
    catalog: t.Optional[str] = None
    host: str
    http_path: str
    token: str
    database: t.Optional[str] = None  # type: ignore

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        from dbt.adapters.databricks.relation import DatabricksRelation

        return DatabricksRelation

    @property
    def column_class(self) -> t.Type[Column]:
        from dbt.adapters.databricks.column import DatabricksColumn

        return DatabricksColumn

    def to_sqlmesh(self) -> ConnectionConfig:
        return DatabricksConnectionConfig(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.token,
            concurrent_tasks=self.threads,
            catalog=self.catalog,
        )


class BigQueryConfig(TargetConfig):
    """
    Project connection and operational configuration for the BigQuery target

    Args:
        type: The type of the target (bigquery)
        method: The BigQuery authentication method to use
        project: The BigQuery project to connect to
        location: The BigQuery location to connect to
        keyfile: The path to the BigQuery keyfile
        keyfile_json: The BigQuery keyfile as a JSON string
        token: The BigQuery token
        refresh_token: The BigQuery refresh token
        client_id: The BigQuery client ID
        client_secret: The BigQuery client secret
        token_uri: The BigQuery token URI
        scopes: The BigQuery scopes
    """

    type: Literal["bigquery"] = "bigquery"
    method: t.Optional[str] = BigQueryConnectionMethod.OAUTH
    dataset: t.Optional[str] = None
    project: t.Optional[str] = None
    location: t.Optional[str] = None
    keyfile: t.Optional[str] = None
    keyfile_json: t.Optional[t.Dict[str, t.Any]] = None
    token: t.Optional[str] = None
    refresh_token: t.Optional[str] = None
    client_id: t.Optional[str] = None
    client_secret: t.Optional[str] = None
    token_uri: t.Optional[str] = None
    scopes: t.Tuple[str, ...] = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    )
    job_execution_timeout_seconds: t.Optional[int] = None
    timeout_seconds: t.Optional[int] = None  # To support legacy config
    job_retries: t.Optional[int] = None
    retries: int = 1  # To support legacy config
    job_retry_deadline_seconds: t.Optional[int] = None
    priority: BigQueryPriority = BigQueryPriority.INTERACTIVE
    maximum_bytes_billed: t.Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def validate_fields(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        values["schema"] = values.get("schema") or values.get("dataset")
        if not values["schema"]:
            raise ConfigError("Either schema or dataset must be set")
        values["database"] = values.get("database") or values.get("project")
        if not values["database"]:
            raise ConfigError("Either database or project must be set")
        return values

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    @property
    def relation_class(self) -> t.Type[BaseRelation]:
        from dbt.adapters.bigquery.relation import BigQueryRelation

        return BigQueryRelation

    @property
    def column_class(self) -> t.Type[Column]:
        from dbt.adapters.bigquery import BigQueryColumn

        return BigQueryColumn

    def to_sqlmesh(self) -> ConnectionConfig:
        job_retries = self.job_retries if self.job_retries is not None else self.retries
        job_execution_timeout_seconds = (
            self.job_execution_timeout_seconds
            if self.job_execution_timeout_seconds is not None
            else self.timeout_seconds
        )
        return BigQueryConnectionConfig(
            method=self.method,
            project=self.database,
            location=self.location,
            concurrent_tasks=self.threads,
            keyfile=self.keyfile,
            keyfile_json=self.keyfile_json,
            token=self.token,
            refresh_token=self.refresh_token,
            client_id=self.client_id,
            client_secret=self.client_secret,
            token_uri=self.token_uri,
            scopes=self.scopes,
            job_execution_timeout_seconds=job_execution_timeout_seconds,
            job_retries=job_retries,
            job_retry_deadline_seconds=self.job_retry_deadline_seconds,
            priority=self.priority,
            maximum_bytes_billed=self.maximum_bytes_billed,
        )
