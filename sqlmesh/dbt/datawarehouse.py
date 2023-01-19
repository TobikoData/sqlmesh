from __future__ import annotations

import abc
import typing as t

from pydantic import Field

from sqlmesh.core.config import Config, SnowflakeConnectionConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


class DataWarehouseConfig(abc.ABC, PydanticModel):
    """
    Project connection and operational configuration for the data warehouse

    Args:
        type: The type of the data warehouse
        schema_: The data warehouse schema for this project
        threads: The number of threads to run on
    """

    type: str
    schema_: str = Field(alias="schema")
    threads: int = 1

    @classmethod
    def load(cls, data: t.Dict[str, t.Any]) -> DataWarehouseConfig:
        """
        Loads the data warehouse configuration from the yaml for project's target output

        Args:
            data: The yaml for the project's target output

        Returns:
            The data warehouse configuration for the provided target output
        """
        db_type = data["type"]
        if db_type == "snowflake":
            return SnowflakeConfig(**data)
        elif db_type == "postgres":
            return PostgresConfig(**data)
        elif db_type == "redshift":
            return RedshiftConfig(**data)
        elif db_type == "databricks":
            return DatabricksConfig(**data)

        # TODO add other data warehouses
        raise ConfigError(f"{db_type} not supported")


class SnowflakeConfig(DataWarehouseConfig):
    """
    Project connection and operational configuration for the Snowflake data warehouse

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

    def to_sqlmesh(self) -> Config:
        snowflake_connection_config = SnowflakeConnectionConfig(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
        )

        return Config(
            connections={"default": snowflake_connection_config},
        )


class PostgresConfig(DataWarehouseConfig):
    """
    Project connection and operational configuration for the Postgres data warehouse

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


class RedshiftConfig(DataWarehouseConfig):
    """
    Project connection and operational configuration for the Redshift data warehouse

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


class DatabricksConfig(DataWarehouseConfig):
    """
    Project connection and operational configuration for the Databricks data warehouse

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
