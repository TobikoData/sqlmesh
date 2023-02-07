from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from sqlmesh.core import constants as c
from sqlmesh.schedulers.airflow import util
from sqlmesh.schedulers.airflow.api import sqlmesh_api_v1


class SqlmeshAirflowPlugin(AirflowPlugin):
    name = c.SQLMESH
    flask_blueprints = [sqlmesh_api_v1]


with util.scoped_state_sync() as state_sync:
    state_sync.init_schema()
