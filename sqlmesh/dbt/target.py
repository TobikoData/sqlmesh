from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field, root_validator

from sqlmesh.core.config.connection import (
    BigQueryConnectionConfig,
    BigQueryConnectionMethod,
    ConnectionConfig,
    DatabricksSQLConnectionConfig,
    DuckDBConnectionConfig,
    RedshiftConnectionConfig,
    SnowflakeConnectionConfig,
)
from sqlmesh.core.model import IncrementalByTimeRangeKind, IncrementalByUniqueKeyKind
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal

IncrementalKind = t.Union[t.Type[IncrementalByUniqueKeyKind], t.Type[IncrementalByTimeRangeKind]]


class TargetConfig(abc.ABC, PydanticModel):
    """
    Configuration for DBT profile target

    Args:
        name: The name of this target
        type: The type of the data warehouse
        schema_: The target schema for this project
        threads: The number of threads to run on
    """

    # dbt
    type: str
    name: str = ""
    schema_: str = Field(alias="schema")
    threads: int = 1
    profile_name: t.Optional[str] = None

    @classmethod
    def load(cls, name: str, data: t.Dict[str, t.Any]) -> TargetConfig:
        """
        Loads the configuration from the yaml provided for a profile target

        Args:
            data: The yaml for the project's target output

        Returns:
            The configuration of the provided profile target
        """
        db_type = data["type"]
        if db_type == "databricks":
            return DatabricksConfig(name=name, **data)
        elif db_type == "duckdb":
            return DuckDbConfig(name=name, **data)
        elif db_type == "postgres":
            return PostgresConfig(name=name, **data)
        elif db_type == "redshift":
            return RedshiftConfig(name=name, **data)
        elif db_type == "snowflake":
            return SnowflakeConfig(name=name, **data)
        elif db_type == "bigquery":
            return BigQueryConfig(name=name, **data)

        raise ConfigError(f"{db_type} not supported.")

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        """The default incremental strategy for the db"""
        raise NotImplementedError

    def to_sqlmesh(self) -> ConnectionConfig:
        """Converts target config to SQLMesh connection config"""
        raise NotImplementedError

    def target_jinja(self, profile_name: str) -> AttributeDict:
        fields = self.dict().copy()
        fields["profile_name"] = profile_name
        fields["target_name"] = self.name
        return AttributeDict(fields)


class DuckDbConfig(TargetConfig):
    """
    Connection config for DuckDb target

    Args:
        path: Location of the database file. If not specified, an in memory database is used.
    """

    type: Literal["duckdb"] = "duckdb"
    path: t.Optional[str] = None

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "delete+insert"

    def to_sqlmesh(self) -> ConnectionConfig:
        return DuckDBConnectionConfig(database=self.path, concurrent_tasks=self.threads)


class SnowflakeConfig(TargetConfig):
    """
    Project connection and operational configuration for the Snowflake target

    Args:
        account: Snowflake account
        warehouse: Name of the warehouse
        database: Name of the database
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

    # TODO add other forms of authentication
    type: Literal["snowflake"] = "snowflake"
    account: str
    warehouse: str
    database: str
    user: str
    password: str
    role: t.Optional[str]
    client_session_keep_alive: bool = False
    query_tag: t.Optional[str]
    connect_retries: int = 0
    connect_timeout: int = 10
    retry_on_database_errors: bool = False
    retry_all: bool = False

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    def to_sqlmesh(self) -> ConnectionConfig:
        return SnowflakeConnectionConfig(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            concurrent_tasks=self.threads,
        )


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
    keepalives_idle: int = 0
    connect_timeout: int = 10
    retries: int = 1
    search_path: t.Optional[str] = None
    role: t.Optional[str] = None
    sslmode: t.Optional[str] = None

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "delete+insert" if kind is IncrementalByUniqueKeyKind else "append"

    def to_sqlmesh(self) -> ConnectionConfig:
        raise ConfigError("PostgreSQL is not supported by SQLMesh yet.")


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

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "append"

    def to_sqlmesh(self) -> ConnectionConfig:
        return RedshiftConnectionConfig(
            user=self.user,
            password=self.password,
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
    """

    type: Literal["databricks"] = "databricks"
    catalog: t.Optional[str] = None
    host: str
    http_path: str
    token: str

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    def to_sqlmesh(self) -> ConnectionConfig:
        return DatabricksSQLConnectionConfig(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.token,
            concurrent_tasks=self.threads,
        )


class BigQueryConfig(TargetConfig):
    """
    Project connection and operational configuration for the BigQuery target

    Args:
        schema_: Overrides
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
    schema_: t.Optional[str] = Field(None, alias="schema")  # type: ignore
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

    @root_validator
    def validate_schema(
        cls, values: t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]
    ) -> t.Dict[str, t.Union[t.Tuple[str, ...], t.Optional[str], t.Dict[str, t.Any]]]:
        values["schema_"] = values.get("schema_") or values.get("dataset")
        if not values["schema_"]:
            raise ConfigError("Either schema or dataset must be set")
        return values

    def default_incremental_strategy(self, kind: IncrementalKind) -> str:
        return "merge"

    def to_sqlmesh(self) -> ConnectionConfig:
        return BigQueryConnectionConfig(
            method=self.method,
            project=self.project,
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
        )
