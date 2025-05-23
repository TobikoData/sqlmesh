import typing as t

from sqlmesh.core import constants as c
from sqlmesh.core.analytics import disable_analytics
from sqlmesh.core.config import Config, DuckDBConnectionConfig
from sqlmesh.core.config.connection import ConnectionConfig

c.MAX_FORK_WORKERS = 1
disable_analytics()


# Replace the implementation of Config._get_connection with a new one that always
# returns a ConnectionConfig to simplify testing
original_get_connection = Config.get_connection


def _lax_get_connection(self, gateway_name: t.Optional[str] = None) -> ConnectionConfig:
    try:
        connection = original_get_connection(self, gateway_name)
    except:
        connection = DuckDBConnectionConfig()
    return connection


setattr(Config, "original_get_connection", original_get_connection)
setattr(Config, "get_connection", _lax_get_connection)
