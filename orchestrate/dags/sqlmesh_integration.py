import os

from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

# FIX ME. We want to run on Snowflake
engine_operator = os.environ.get("AIRFLOW_ENGINE_OPERATOR", "spark")
sqlmesh_airflow = SQLMeshAirflow(engine_operator, default_catalog="spark_catalog")

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag

