# Airflow

SQLMesh provides first-class support for Airflow with the following capabilities:
* A dedicated Directed Acyclic Graph (DAG) is generated dynamically for each model version. Each DAG accounts for all its upstream dependencies defined within SQLMesh, and only runs after upstream DAGs succeed for the time period being processed.
* Each Plan application leads to the creation of a dynamically-generated DAG dedicated specifically to that Plan.
* The Airflow XCom mechanism is used for persistence of the SQLMesh state, meaning no external storage or additional configuration is required for SQLMesh to work.
* The janitor DAG runs periodically and automatically to clean up DAGs and other SQLMesh artifacts no longer needed.
* Support for any SQL engine can be added by providing a custom Airflow Operator.

To integrate SQLMesh with Airflow, configure the following:

# Airflow cluster configuration
To enable SQLMesh support on a target Airflow cluster, the SQLMesh package should first be installed in that cluster. Refer to `sqlmesh.docs.getting_started`.

Once the package is installed, a new Python module is created in the target DAG repository with the following content:
```python
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow("spark")

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
```
The name of the module file can be arbitrary, but we recommend something descriptive such as `sqlmesh.py` or `sqlmesh_integration.py`.

**Note**: The name of the Engine operator is the only mandatory parameter needed for `sqlmesh.schedulers.airflow.integration.SQLMeshAirflow`. Currently supported engines are listed in the [Engine support](#engine-support) section.

# SQLMesh client configuration
In your SQLMesh repository, create the following configuration:
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
**Note:** The `dialect` parameter represents the dialect in which models are defined in the SQLMesh repository, and can be different from the target engine used with Airflow.

# Engine support
SQLMesh supports a variety of engines in Airflow. Support for each engine is provided by a custom Airflow operator implementation. Below is a list of operators supported out of the box, as well as guidelines for adding new operators in cases where existing ones don't meet your integration requirements.

## Spark
**Engine Name:** `spark` / `spark-submit` / `spark_submit`.

The SQLMesh Spark operator is very similar to the Airflow [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator), and relies on the same [SparkSubmitHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html#airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook) implementation.

To enable support for this operator, the Airflow Spark provider package should be installed on the target Airflow cluster as follows:
```
pip install apache-airflow-providers-apache-spark
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target cluster, queue, and deploy mode in which the Spark Job should be submitted. Please see [Apache Spark connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html) for more details.

By default, the connection ID is set to `spark_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python
sqlmesh_airflow = SQLMeshAirflow(
    "spark",
    engine_operator_args={
        "connection_id": "<Connection ID>"
    },
)
```
Similarly, the `engine_operator_args` parameter can be used to override other job submission parameters, such as  number of allocated cores, executors, and so forth. The full list of parameters that can be overridden is found in `sqlmesh.schedulers.airflow.operators.spark_submit.SQLMeshSparkSubmitOperator`.

### Cluster mode
Each Spark job submitted by SQLMesh is a PySpark application that depends on the SQLMesh library in its Driver process (but not in Executors).

This means that if the Airflow connection is configured to submit jobs in `cluster` mode as opposed to `client` mode, the user must ensure that the SQLMesh Python library is installed on each node of a cluster where Spark jobs are submitted. This is because there is no way to know in advance which specific node to which a Driver process will be scheduled.

No additional configuration is required if the deploy mode is set to `client`.

## Databricks
**Engine Name:** `databricks` / `databricks-sql` / `databricks_sql`.

The SQLMesh Databricks SQL Operator is similar to the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator), and relies on the same [DatabricksSqlHook](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/hooks/databricks_sql/index.html#airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook) implementation.

To enable support for this operator, the Airflow Spark provider package should be installed on the target Airflow cluster as follows:
```
pip install apache-airflow-providers-databricks
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Databricks cluster. Please see [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) for more details.

By default, the connection ID is set to `databricks_default`, but can be overriden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python
sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    engine_operator_args={
        "databricks_conn_id": "<Connection ID>"
    },
)
```

## Snowflake
**Engine Name:** `snowflake` / `snowflake-sql` / `snowflake_sql`.

The SQLMesh Snowflake Operator is similar to the [SnowflakeOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html), and relies on the same [SnowflakeHook](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html) implementation.

To enable support for this operator, the Airflow Snowflake provider package should be installed on the target Airflow cluster along with the Snowflake connector with pandas extra:
```
pip install "apache-airflow-providers-snowflake[common.sql]"
pip install "snowflake-connector-python[pandas]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Snowflake account. Please see [Snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) for more details.

By default, the connection ID is set to `snowflake_default`, but can be overriden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python
sqlmesh_airflow = SQLMeshAirflow(
    "snowflake",
    engine_operator_args={
        "snowflake_conn_id": "<Connection ID>"
    },
)
```
