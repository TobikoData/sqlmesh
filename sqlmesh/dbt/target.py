from __future__ import annotations

import abc
import typing as t

from pydantic import Field

from sqlmesh.core.config import (
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

    type: str = "duckdb"
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
    type: str = "snowflake"
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

    type: str = "postgres"
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
    type: str = "redshift"
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

    type: str = "databricks"
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
