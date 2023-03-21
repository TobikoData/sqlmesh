# Airflow

SQLMesh provides first-class support for Airflow with the following capabilities:

* A Directed Acyclic Graph (DAG) generated dynamically for each model version. Each DAG accounts for all its upstream dependencies defined within SQLMesh, and only runs after upstream DAGs succeed for the time period being processed.
* Each Plan application leads to the creation of a dynamically-generated DAG dedicated specifically to that Plan.
* The Airflow [Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) is used for persistence of the SQLMesh state, meaning no external storage or additional configuration is required for SQLMesh to work.
* The janitor DAG runs periodically and automatically to clean up DAGs and other SQLMesh artifacts no longer needed.
* Support for any SQL engine can be added by providing a custom Airflow Operator.

## Airflow cluster configuration
To enable SQLMesh support on a target Airflow cluster, the SQLMesh package should first be installed on that cluster. Make sure it is installed with the extras for your engine (if needed). Ex: `sqlmesh[databricks]` for Databricks. Check [setup.py](https://github.com/TobikoData/sqlmesh/blob/main/setup.py) for a list of extras. 

**Note:** The Airflow Webserver instance(s) must be restarted after installation.

Once the package is installed, the following Python module must be created in the `dags/` folder of the target DAG repository with the following contents:

```python linenums="1"
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow("spark")

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
```
The name of the module file can be arbitrary, but we recommend something descriptive such as `sqlmesh.py` or `sqlmesh_integration.py`.

**Note**: The name of the Engine operator is the only mandatory parameter needed for `sqlmesh.schedulers.airflow.integration.SQLMeshAirflow`. Currently supported engines are listed in the [Engine support](#engine-support) section.

## SQLMesh client configuration
In your SQLMesh repository, create the following configuration:
```python linenums="1"
from sqlmesh.core.config import Config, AirflowSchedulerConfig

airflow_config = Config(
    scheduler=AirflowSchedulerConfig(
        airflow_url="https://<Airflow Webserver Host>:<Airflow Webserver Port>/",
        username="<Airflow Username>",
        password="<Airflow Password>",
    )
)
```

## Engine support
SQLMesh supports a variety of engines in Airflow. Support for each engine is provided by a custom Airflow operator implementation. Below is a list of operators supported out of the box, as well as guidelines for adding new operators in cases where existing ones don't meet your integration requirements.

### Spark
**Engine Name:** `spark`

The SQLMesh Spark operator is very similar to the Airflow [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator), and relies on the same [SparkSubmitHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html#airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook) implementation.

To enable support for this operator, the Airflow Spark provider package should be installed on the target Airflow cluster as follows:
```
pip install apache-airflow-providers-apache-spark
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target cluster, queue, and deploy mode in which the Spark Job should be submitted. Please see [Apache Spark connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html) for more details.

By default, the connection ID is set to `spark_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "spark",
    engine_operator_args={
        "connection_id": "<Connection ID>"
    },
)
```
Similarly, the `engine_operator_args` parameter can be used to override other job submission parameters, such as  number of allocated cores, executors, and so forth. The full list of parameters that can be overridden is found in `sqlmesh.schedulers.airflow.operators.spark_submit.SQLMeshSparkSubmitOperator`.

**Cluster mode**
<br><br>

Each Spark job submitted by SQLMesh is a PySpark application that depends on the SQLMesh library in its Driver process (but not in Executors). This means that if the Airflow connection is configured to submit jobs in `cluster` mode as opposed to `client` mode, the user must ensure that the SQLMesh Python library is installed on each node of a cluster where Spark jobs are submitted. This is because there is no way to know in advance which specific node to which a Driver process will be scheduled. No additional configuration is required if the deploy mode is set to `client`.

### Databricks
**Engine Name:** `databricks` / `databricks-submit` / `databricks-sql`.

Databricks has multiple operators to help differentiating running a SQL query vs. running a Python script.

#### Engine: `databricks` (Recommended)

When evaluating models, the SQLMesh Databricks integration implements the [DatabricksSubmitRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/1.0.0/operators.html). This is needed to be able to run either SQL or Python scripts on the Databricks cluster.

When performing environment management operations, the SQLMesh Databricks integration is similar to the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator), and relies on the same [DatabricksSqlHook](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/hooks/databricks_sql/index.html#airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook) implementation. 
All environment management operations are SQL based and the overhead of submitting jobs can be avoided.

#### Engine: `databricks-submit`

Whether evaluating models or performing environment management operations, the SQLMesh Databricks integration implements the [DatabricksSubmitRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/1.0.0/operators.html).

#### Engine: `databricks-sql`

Forces the SQLMesh Databricks integration to use the operator based on the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator) for all operations. If your project is pure SQL operations then this is an option. 

To enable support for this operator, the Airflow Databricks provider package should be installed on the target Airflow cluster along with the SQLMesh package with databricks extra as follows :
```
pip install apache-airflow-providers-databricks
sqlmesh[databricks]
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Databricks cluster. Please see [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) for more details. SQLMesh requires that `http_path` be defined in the connection since it uses this to determine the cluster for both SQL and submit operators.

Example format: `databricks://<hostname>?token=<token>&http_path=<http_path>`

By default, the connection ID is set to `databricks_default`, but can be overriden using both the `engine_operator_args` and the `ddl_engine_operator_args` parameters to the `SQLMeshAirflow` instance.
In addition, one special configuration that SQLMesh Airflow evaluation operator requires is a dbfs path to store an application to load a given SQLMesh model. Also a payload is stored that contains the information require for SQLMesh to do the loading. This must be defined in the `evaluate_engine_operator_args` parameter. Example of defining both:

```python linenums="1"
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    engine_operator_args={
        "databricks_conn_id": "<Connection ID>",
        "dbfs_location": "dbfs:/FileStore/sqlmesh",
    },
    ddl_engine_operator_args={
        "databricks_conn_id": "<Connection ID>",
    }
)

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
```

Note: If your Databricks connection is configured to run on serverless [DBSQL](https://www.databricks.com/product/databricks-sql) then you need to define `existing_cluster_id` or `new_cluster` in your `engine_operator_args`. Example:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    engine_operator_args={
        "dbfs_location": "dbfs:/FileStore/sqlmesh",
        "existing_cluster_id": "1234-123456-slid123",
    }
)
```

### Snowflake
**Engine Name:** `snowflake` / `snowflake-sql` / `snowflake_sql`.

The SQLMesh Snowflake Operator is similar to the [SnowflakeOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html), and relies on the same [SnowflakeHook](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html) implementation.

To enable support for this operator, the Airflow Snowflake provider package should be installed on the target Airflow cluster along with SQLMesh with the Snowflake extra:
```
pip install "apache-airflow-providers-snowflake[common.sql]"
pip install "sqlmesh[snowflake]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Snowflake account. Please see [Snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) for more details.

By default, the connection ID is set to `snowflake_default`, but can be overriden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "snowflake",
    engine_operator_args={
        "snowflake_conn_id": "<Connection ID>"
    },
)
```
