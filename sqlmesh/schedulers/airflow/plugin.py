from __future__ import annotations

import logging
import os
import time
import typing as t

from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin

from sqlmesh.core import constants as c
from sqlmesh.schedulers.airflow import util
from sqlmesh.schedulers.airflow.api import sqlmesh_api_v1
from sqlmesh.schedulers.airflow.common import DEFAULT_CATALOG_VARIABLE_NAME
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


class SqlmeshAirflowPlugin(AirflowPlugin):
    name = c.SQLMESH
    flask_blueprints = [sqlmesh_api_v1]

    @classmethod
    def on_load(cls, *args: t.Any, **kwargs: t.Any) -> None:
        if os.environ.get("MWAA_AIRFLOW_COMPONENT", "").lower() == "webserver":
            # When using MWAA, the Webserver instance might not have access to the external state database.
            logger.info("MWAA Webserver instance detected. Skipping SQLMesh state migration...")
            return

        default_catalog = Variable.get(DEFAULT_CATALOG_VARIABLE_NAME, default_var=None)

        with util.scoped_state_sync() as state_sync:
            try:
                # If default catalog is not defined then we want to raise an error unless
                # this is a fresh install since we know nothing needs to be migrated and
                # the client will prevent making any changes until the default catalog is set.
                if not default_catalog:
                    versions = state_sync.get_versions(validate=False)
                    if versions.schema_version != 0:
                        raise SQLMeshError(
                            f"Must define `default_catalog` when creating `SQLMeshAirflow` object. See docs for more info: https://sqlmesh.readthedocs.io/en/stable/integrations/airflow/#airflow-cluster-configuration"
                        )
                logger.info("Migrating SQLMesh state ...")
                state_sync.migrate(default_catalog=default_catalog)
            except Exception as ex:
                # This method is called once for each Gunicorn worker spawned by the Airflow Webserver,
                # which leads to SQLMesh schema being initialized concurrently from multiple processes.
                # There is a known issue in Postgres (https://stackoverflow.com/a/29908840) which occurs
                # due to a race condition when a new schema is being created concurrently. Here we retry
                # the schema initialization once as a workaround.
                logger.warning("Failed to initialize the SQLMesh State Sync: %s. Retrying...", ex)
                time.sleep(1)
                state_sync.migrate(default_catalog=default_catalog)
