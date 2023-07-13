from pathlib import Path

from sqlmesh.core.config import AirflowSchedulerConfig, GooglePostgresConnectionConfig
from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(
    Path(__file__).parent,
    state_connection=GooglePostgresConnectionConfig(
        instance_connection_string="tobiko-1:us-central1:postgres-state-db",
        user="chris@tobikodata.com",
        enable_iam_auth=True,
        db="state",
    ),
)


test_config = config


airflow_config = sqlmesh_config(Path(__file__).parent, default_scheduler=AirflowSchedulerConfig())
