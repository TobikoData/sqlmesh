from pathlib import Path

from sqlmesh.core.config import AirflowSchedulerConfig
from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent, default_sql_dialect="")


test_config = config


airflow_config = sqlmesh_config(
    Path(__file__).parent,
    default_scheduler=AirflowSchedulerConfig(),
    default_sql_dialect="",
)
