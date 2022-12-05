"""
# Overview

SQLMesh provides first-class support for Airflow with the following capabilities:
* A dedicated DAG is generated **dynamically** for each model version. Each DAG accounts for all its upstream dependencies defined within SQLMesh and only runs after upstream DAGs succeed for a time period that is being processed.
* Similarly each Plan application leads to the creation of a dynamically generated DAG dedicated specifically to that Plan.
* The Airflow XCom mechanism is used for persistence of the SQLMesh state. This means that no external storage and no additional configuration associated with it is required for SQLMesh to work.
* The Janitor DAG runs periodically and automatically cleans up DAGs and other SQLMesh artifacts that are no longer needed.
* Support for any SQL engine can be added by providing a custom Airflow Operator for it.

To integrate SQLMesh with Airflow follow a few simple steps as outlined below.

# Airflow Cluster Configuration
To enable SQLMesh support on a target Airflow cluster, the SQLMesh package should first be [installed](https://github.com/tobymao/tobiko/blob/main/README.md#getting-started) in that cluster.

Once the package is installed a new Python module should be created in the target DAG repository with the following content:
```python
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow("spark")

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
```
The name of the module file can be arbitrary but we recommend something descriptive like eg. `sqlmesh.py` or `sqlmesh_integration.py`.

Please note that the name of the Engine Operator is the only mandatory parameter that is needed for `sqlmesh.schedulers.airflow.integration.SQLMeshAirflow`. You can find currently supported engines in the [Engine Support](#Engine-Support) section.

# SQLMesh Client Configuration
In your SQLMesh repository create the following configuration:
```python
from sqlmesh.core.config import Config, AirflowSchedulerBackend

airflow_config = Config(
    dialect="spark",
    scheduler_backend=AirflowSchedulerBackend(
        airflow_url="https://<Airflow Webserver Host>:<Airflow Webserver Port>/",
        username="<Airflow Username>",
        password="<Airflow Password>",
    )
)
```
Please note that the `dialect` parameter represents the dialect in which models are defined in the SQLMesh repository and can be different from the target engine used with Airflow.

## Google Cloud Composer
TBD

# Engine Support
SQLMesh supports a variety of engines in Airflow. Support for each engine is provided by a custom Airflow Operator implementation. Below is a list of operators supported out of the box as well as the guidelines for adding new operators in cases when the existing ones don't meet the integration requirements.

## Spark
**Engine Name:** `spark` / `spark-submit` / `spark_submit`.

The SQLMesh Spark Operator is very similar to the Airflow [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator) and relies on the same [SparkSubmitHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html#airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook) implementation.

To enable support for this operator the Airflow Spark Provider package should be installed on the target Airflow Cluster:
```
pip install apache-airflow-providers-apache-spark
```

The operator requires an [Airflow Connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target cluster, queue and deploy mode in which the Spark Job should be submitted. Please see [Apache Spark Connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html) for more details.

By default the Connection ID is set to `spark_default` but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance like in the example below:
```python
sqlmesh_airflow = SQLMeshAirflow(
    "spark",
    engine_operator_args={
        "connection_id": "<Connection ID>"
    },
)
```
Similarly the `engine_operator_args` parameter can be used to override other job submission parameters like number of allocated cores, executors, etc. The full list of parameters that can be overridden can be found in `sqlmesh.schedulers.airflow.operators.spark_submit.SQLMeshSparkSubmitOperator`.

### Cluster Mode
Each Spark job submitted by SQLMesh is a PySpark application which depends on the SQLMesh library in its Driver process (but not in Executors).

This means that if the Airflow Connection is configured to submit jobs in `cluster` mode as opposed to `client` mode, it's a responsibility of a user to make sure that the SQLMesh Python library is installed on each node of a cluster to which Spark jobs are submitted. This is required because there is no way to know in advance a specific node to which a Driver process will be scheduled.

No additional configuration is required if the deploy mode is set to `client`.

## Databricks
**Engine Name:** `databricks` / `databricks-sql` / `databricks_sql`.

The SQLMesh Databricks SQL Operator is similar to the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator) and relies on the same [DatabricksSqlHook](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/hooks/databricks_sql/index.html#airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook) implementation.

To enable support for this operator the Airflow Spark Provider package should be installed on the target Airflow Cluster:
```
pip install apache-airflow-providers-databricks
```

The operator requires an [Airflow Connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Databricks cluster. Please see [Databricks Connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) for more details.

By default the Connection ID is set to `databricks_default` but can be overriden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance like in the example below:
```python
sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    engine_operator_args={
        "databricks_conn_id": "<Connection ID>"
    },
)
```
## Custom
TBD

"""
