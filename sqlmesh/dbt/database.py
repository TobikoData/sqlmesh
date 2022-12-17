from __future__ import annotations

import abc
import typing as t

from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.errors import ConfigError

class DatabaseConfig(abc.ABC, PydanticModel):
    type: str
    schema: str

    @classmethod
    def parse(cls, data: t.Dict[str, t.Any]) -> DatabaseConfig:
        db_type = data["type"]
        if db_type == "snowflake":
            return SnowflakeConfig(data)

        # TODO add other databases    
        raise ConfigError(f"{db_type} not supported")


class SnowflakeConfig(DatabaseConfig):
    # TODO add other forms of authentication
    account: str 
    warehouse: str
    database: str
    user: str
    password: str
    role: t.Optional[str]
    threads: int = 1
    client_session_keep_alive: bool = False
    query_tag: t.Optional[str]
    connect_retries: int = 0
    connect_timeout: int = 10
    retry_on_database_errors: bool = False
    retry_all: bool = False