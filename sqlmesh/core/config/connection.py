from __future__ import annotations

import abc
import sys
import typing as t

from pydantic import Field

from sqlmesh.core import engine_adapter
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class _ConnectionConfig(abc.ABC):
    @abc.abstractmethod
    def create_engine_adapter(self, multithreaded: bool) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter.

        Args:
            multithreaded: Indicates whether this adapter will be used by more than one thread.

        """


class DuckDBConnectionConfig(_ConnectionConfig, PydanticModel):
    """Configuration for the DuckDB connection."""

    database: t.Optional[str]

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
    """Configuration for the Snowflake connection."""

    user: str
    password: str
    account: str
    warehouse: t.Optional[str]
    database: t.Optional[str]
    role: t.Optional[str]

    type_: Literal["snowflake"] = Field(alias="type", default="snowflake")

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
