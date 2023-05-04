from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.config.connection import ConnectionConfig
from sqlmesh.core.config.scheduler import SchedulerConfig


class GatewayConfig(BaseConfig):
    """Gateway configuration defines how SQLMesh should connect to the data warehouse,
    the state backend and the scheduler.

    Args:
        connection: Connection configuration for the data warehouse.
        state_connection: Connection configuration for the state backend. If not provided,
            the same connection as the data warehouse will be used.
        test_connection: Connection configuration for running unit tests.
        scheduler: The scheduler configuration.
    """

    connection: t.Optional[ConnectionConfig] = None
    state_connection: t.Optional[ConnectionConfig] = None
    test_connection: t.Optional[ConnectionConfig] = None
    scheduler: t.Optional[SchedulerConfig] = None
