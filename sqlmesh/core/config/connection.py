from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field

from sqlmesh.core import engine_adapter
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.engine_adapter import EngineAdapter

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class _ConnectionConfig(abc.ABC):
    concurrent_tasks: int

    @abc.abstractmethod
    def create_engine_adapter(self) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter."""


class DuckDBConnectionConfig(_ConnectionConfig, BaseConfig):
    """Configuration for the DuckDB connection.

    Args:
        database: The optional database name. If not specified the in-memory database will be used.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
    """

    database: t.Optional[str]

    concurrent_tasks: Literal[1] = 1

    type_: Literal["duckdb"] = Field(alias="type", default="duckdb")

    def create_engine_adapter(self) -> EngineAdapter:
        import duckdb

        if self.database:
            connection_factory = lambda: duckdb.connect(self.database)
        else:
            connection_factory = duckdb.connect

        return engine_adapter.DuckDBEngineAdapter(connection_factory)


class SnowflakeConnectionConfig(_ConnectionConfig, BaseConfig):
    """Configuration for the Snowflake connection.

    Args:
        user: The Snowflake username.
        password: The Snowflake password.
        account: The Snowflake account name.
        warehouse: The optional warehouse name.
        database: The optional database name.
        role: The optional role name.
        concurrent_tasks: The maximum number of tasks that can use this connection concurrently.
    """

    user: str
    password: str
    account: str
    warehouse: t.Optional[str]
    database: t.Optional[str]
    role: t.Optional[str]

    concurrent_tasks: int = 4

    type_: Literal["snowflake"] = Field(alias="type", default="snowflake")

    _concurrent_tasks_validator = concurrent_tasks_validator

    def create_engine_adapter(self) -> EngineAdapter:
        from snowflake import connector

        kwargs = {
            "user": self.user,
            "password": self.password,
            "account": self.account,
        }
        if self.warehouse is not None:
            kwargs["warehouse"] = self.warehouse
        if self.database is not None:
            kwargs["database"] = self.database
        if self.role is not None:
            kwargs["role"] = self.role

        return engine_adapter.SnowflakeEngineAdapter(
            lambda: connector.connect(**kwargs), multithreaded=self.concurrent_tasks > 1
        )


ConnectionConfig = Annotated[
    t.Union[DuckDBConnectionConfig, SnowflakeConnectionConfig],
    Field(discriminator="type_"),
]
