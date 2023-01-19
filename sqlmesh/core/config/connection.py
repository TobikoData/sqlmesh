from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field

from sqlmesh.core import engine_adapter
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class _ConnectionConfig(abc.ABC):
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    evaluation_concurrent_tasks: int

    @abc.abstractmethod
    def create_engine_adapter(self, multithreaded: bool) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter.

        Args:
            multithreaded: Indicates whether this adapter will be used by more than one thread.

        """


class DuckDBConnectionConfig(_ConnectionConfig, PydanticModel):
    """Configuration for the DuckDB connection.

    Args:
        database: The optional database name. If not specified the in-memory database will be used.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
        evaluation_concurrent_tasks: The number of concurrent tasks used for model evaluation.
    """

    database: t.Optional[str]

    backfill_concurrent_tasks: Literal[1] = 1
    ddl_concurrent_tasks: Literal[1] = 1
    evaluation_concurrent_tasks: Literal[1] = 1

    type_: Literal["duckdb"] = Field(alias="type", default="duckdb")

    def create_engine_adapter(self, multithreaded: bool) -> EngineAdapter:
        if multithreaded:
            raise ConfigError(
                "The DuckDB connection can't be used with multiple threads."
            )

        import duckdb

        if self.database:
            connection_factory = lambda: duckdb.connect(self.database)
        else:
            connection_factory = duckdb.connect

        return engine_adapter.DuckDBEngineAdapter(connection_factory)


class SnowflakeConnectionConfig(_ConnectionConfig, PydanticModel):
    """Configuration for the Snowflake connection.

    Args:
        user: The Snowflake username.
        password: The Snowflake password.
        account: The Snowflake account name.
        warehouse: The optional warehouse name.
        database: The optional database name.
        role: The optional role name.
        backfill_concurrent_tasks: The number of concurrent tasks used for model backfilling during plan application.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc).
        evaluation_concurrent_tasks: The number of concurrent tasks used for model evaluation.
    """

    user: str
    password: str
    account: str
    warehouse: t.Optional[str]
    database: t.Optional[str]
    role: t.Optional[str]

    backfill_concurrent_tasks: int = 4
    ddl_concurrent_tasks: int = 4
    evaluation_concurrent_tasks: int = 4

    type_: Literal["snowflake"] = Field(alias="type", default="snowflake")

    _concurrent_tasks_validator = concurrent_tasks_validator

    def create_engine_adapter(self, multithreaded: bool) -> EngineAdapter:
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
            lambda: connector.connect(**kwargs), multithreaded=multithreaded
        )


ConnectionConfig = Annotated[
    t.Union[DuckDBConnectionConfig, SnowflakeConnectionConfig],
    Field(discriminator="type_"),
]
