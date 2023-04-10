from __future__ import annotations

import logging
import time
import typing as t

from airflow.plugins_manager import AirflowPlugin

from sqlmesh.core import constants as c
from sqlmesh.schedulers.airflow import util
from sqlmesh.schedulers.airflow.api import sqlmesh_api_v1

logger = logging.getLogger(__name__)


class SqlmeshAirflowPlugin(AirflowPlugin):
    name = c.SQLMESH
    flask_blueprints = [sqlmesh_api_v1]

    @classmethod
    def on_load(cls, *args: t.Any, **kwargs: t.Any) -> None:
        with util.scoped_state_sync() as state_sync:
            try:
                state_sync.migrate()
            except Exception as ex:
                # This method is called once for each Gunicorn worker spawned by the Airflow Webserver,
                # which leads to SQLMesh schema being initialized concurrently from multiple processes.
                # There is a known issue in Postgres (https://stackoverflow.com/a/29908840) which occurs
                # due to a race condition when a new schema is being created concurrently. Here we retry
                # the schema initialization once as a workaround.
                logger.warning("Failed to initialize the SQLMesh State Sync: %s. Retrying...", ex)
                time.sleep(1)
                state_sync.migrate()
