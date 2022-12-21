from __future__ import annotations

import abc
import typing as t

from pydantic import Field

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
        client_session_keep_alive: A boolean flag to extend the duration of the snowflake session beyond 4 hours
        query_tag: tag for the query in snowflake
        connect_retries: Number of times to retry if the snowflake connector encounters an error
        connect_timeouts: Number of seconds to wait between failed attempts
        retry_on_database_errors: A boolean flag to retry if a snowflake connector Database error is encountered
        retry_all: A boolean flag to retry on all snowflake connector errors
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
