from __future__ import annotations

import abc
import typing as t

from pydantic import Field

from sqlmesh.core.config import (
    ConnectionConfig,
    DuckDBConnectionConfig,
    SnowflakeConnectionConfig,
)
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


class TargetConfig(abc.ABC, PydanticModel):
    """
    Configuration for DBT profile target

    Args:
        name: The name of this target
        type: The type of the data warehouse
        schema_: The target schema for this project
        threads: The number of threads to run on
    """

    # sqlmesh
    name: str = ""

    # dbt
    type: str
    schema_: str = Field(alias="schema")
    threads: int = 1

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

    def to_sqlmesh(self) -> ConnectionConfig:
        """Converts target config to SQLMesh connection config"""
        raise NotImplementedError

    def target_jinja(self, profile_name: str) -> TargetJinja:
        fields = self.dict().copy()
        fields["profile_name"] = profile_name
        return TargetJinja(fields)


class DuckDbConfig(TargetConfig):
    """
    Connection config for DuckDb target

    Args:
        path: Location of the database file. If not specified, an in memory database is used.
    """

    path: t.Optional[str] = None

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

    def to_sqlmesh(self) -> ConnectionConfig:
        raise NotImplementedError


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

    def to_sqlmesh(self) -> ConnectionConfig:
        raise NotImplementedError


class DatabricksConfig(TargetConfig):
    """
    Project connection and operational configuration for the Databricks target

    Args:
        catalog: Catalog name to use for Unity Catalog
        host: The Databricks host to connect to
        http_path: The Databricks compute resources URL
        token: Personal access token
    """

    catalog: t.Optional[str] = None
    host: str
    http_path: str
    token: str

    def to_sqlmesh(self) -> ConnectionConfig:
        raise NotImplementedError


class TargetJinja:
    def __init__(self, fields: t.Dict[str, t.Any]):
        self._fields = fields

    def __getattr__(self, name: str) -> t.Any:
        value = self._fields.get(name)
        if value is None:
            raise AttributeError(f"{self.__class__.__name__}.{name} not found")
        return value
