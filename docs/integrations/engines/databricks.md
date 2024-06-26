# Databricks

## Local/Built-in Scheduler
**Engine Adapter Type**: `databricks`

### Installation
```
pip install "sqlmesh[databricks]"
```

### Connection info

If you are always running SQLMesh commands directly on a Databricks Cluster (like in a Databricks Notebook using the [notebook magic commands](../../reference/notebook.md)) then the only relevant configuration is `catalog` and it is optional.
The SparkSession provided by Databricks will be used to execute all SQLMesh commands.

Otherwise SQLMesh's Databricks implementation uses the [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html) to connect to Databricks by default.
If your project contains PySpark DataFrames in Python models then it will use [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html) to connect to Databricks.
SQLMesh's Databricks Connect implementation supports Databricks Runtime 13.0 or higher. If SQLMesh detects you have Databricks Connect installed then it will use it for all Python models (so both Pandas and PySpark DataFrames).

Databricks connect execution can be routed to a different cluster than the SQL Connector by setting the `databricks_connect_*` properties.
For example this allows SQLMesh to be configured to run SQL on a [Databricks SQL Warehouse](https://docs.databricks.com/sql/admin/create-sql-warehouse.html) while still routing DataFrame operations to a normal Databricks Cluster.

Note: If using Databricks Connect please note the [requirements](https://docs.databricks.com/dev-tools/databricks-connect.html#requirements) and [limitations](https://docs.databricks.com/dev-tools/databricks-connect.html#limitations)

### Connection options

| Option                               | Description                                                                                                                                                                                                                                                                                     |  Type  | Required |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                               | Engine type name - must be `databricks`                                                                                                                                                                                                                                                         | string |    Y     |
| `server_hostname`                    | Databricks instance host name                                                                                                                                                                                                                                                                   | string |    N     |
| `http_path`                          | HTTP path, either to a DBSQL endpoint (such as `/sql/1.0/endpoints/1234567890abcdef`) or to an All-Purpose cluster (such as `/sql/protocolv1/o/1234567890123456/1234-123456-slid123`)                                                                                                           | string |    N     |
| `access_token`                       | HTTP Bearer access token, such as Databricks Personal Access Token                                                                                                                                                                                                                              | string |    N     |
| `catalog`                            | Spark 3.4+ Only if not using SQL Connector. The name of the catalog to use for the connection. [Defaults to use Databricks cluster default](https://docs.databricks.com/en/data-governance/unity-catalog/create-catalogs.html#the-default-catalog-configuration-when-unity-catalog-is-enabled). | string |    N     |
| `http_headers`                       | SQL Connector Only: An optional dictionary of HTTP headers that will be set on every request                                                                                                                                                                                                    |  dict  |    N     |
| `session_configuration`              | SQL Connector Only: An optional dictionary of Spark session parameters. Execute the SQL command `SET -v` to get a full list of available commands.                                                                                                                                              |  dict  |    N     |
| `databricks_connect_server_hostname` | Databricks Connect Only: Databricks Connect server hostname. Uses `server_hostname` if not set.                                                                                                                                                                                                 | string |    N     |
| `databricks_connect_access_token`    | Databricks Connect Only: Databricks Connect access token. Uses `access_token` if not set.                                                                                                                                                                                                       | string |    N     |
| `databricks_connect_cluster_id`      | Databricks Connect Only: Databricks Connect cluster ID. Uses `http_path` if not set. Cannot be a Databricks SQL Warehouse.                                                                                                                                                                      | string |    N     |
| `force_databricks_connect`           | When running locally, force the use of Databricks Connect for all model operations (so don't use SQL Connector for SQL models)                                                                                                                                                                  |  bool  |    N     |
| `disable_databricks_connect`         | When running locally, disable the use of Databricks Connect for all model operations (so use SQL Connector for all models)                                                                                                                                                                      |  bool  |    N     |
| `disable_spark_session`              | Do not use SparkSession if it is available (like when running in a notebook).                                                                                                                                                                                                                   |  bool  |    N     |

## Airflow Scheduler
**Engine Name:** `databricks` / `databricks-submit` / `databricks-sql`.

Databricks has multiple operators to help differentiate running a SQL query vs. running a Python script.

### Engine: `databricks` (Recommended)

When evaluating models, the SQLMesh Databricks integration implements the [DatabricksSubmitRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/1.0.0/operators.html). This is needed to be able to run either SQL or Python scripts on the Databricks cluster.

When performing environment management operations, the SQLMesh Databricks integration is similar to the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator), and relies on the same [DatabricksSqlHook](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/hooks/databricks_sql/index.html#airflow.providers.databricks.hooks.databricks_sql.DatabricksSqlHook) implementation.
All environment management operations are SQL-based, and the overhead of submitting jobs can be avoided.

### Engine: `databricks-submit`

Whether evaluating models or performing environment management operations, the SQLMesh Databricks integration implements the [DatabricksSubmitRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/1.0.0/operators.html).

### Engine: `databricks-sql`

Forces the SQLMesh Databricks integration to use the operator based on the [DatabricksSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/sql.html#databrickssqloperator) for all operations. If your project is pure SQL operations, then this is an option.

To enable support for this operator, the Airflow Databricks provider package should be installed on the target Airflow cluster along with the SQLMesh package with databricks extra as follows:
```
pip install apache-airflow-providers-databricks
sqlmesh[databricks]
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Databricks cluster. Refer to [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) for more details. SQLMesh requires that `http_path` be defined in the connection since it uses this to determine the cluster for both SQL and submit operators.

Example format: `databricks://<hostname>?token=<token>&http_path=<http_path>`

By default, the connection ID is set to `databricks_default`, but it can be overridden using both the `engine_operator_args` and the `ddl_engine_operator_args` parameters to the `SQLMeshAirflow` instance.
In addition, one special configuration that the SQLMesh Airflow evaluation operator requires is a dbfs path to store an application to load a given SQLMesh model. Also, a payload is stored that contains the information required for SQLMesh to do the loading. This must be defined in the `evaluate_engine_operator_args` parameter. Example of defining both:

```python linenums="1"
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    default_catalog="<catalog name>",
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

**Note:** If your Databricks connection is configured to run on serverless [DBSQL](https://www.databricks.com/product/databricks-sql), then you need to define `existing_cluster_id` or `new_cluster` in your `engine_operator_args`. Example:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "databricks",
    default_catalog="<catalog name>",
    engine_operator_args={
        "dbfs_location": "dbfs:/FileStore/sqlmesh",
        "existing_cluster_id": "1234-123456-slid123",
    }
)
```

## Model table properties to support altering tables

If you are making a change to the structure of a table that is [forward only](../../guides/incremental_time.md#forward-only-models), then you may need to add the following to your model's `physical_properties`:


```sql
MODEL (
    name sqlmesh_example.new_model,
    ...
    physical_properties (
        'delta.columnMapping.mode' = 'name'
    ),
)
```

If you attempt to alter without having this property set, you will get an error similar to `databricks.sql.exc.ServerOperationError: [DELTA_UNSUPPORTED_DROP_COLUMN] DROP COLUMN is not supported for your Delta table.`.
[Databricks Documentation for more details](https://docs.databricks.com/en/delta/column-mapping.html#requirements).
