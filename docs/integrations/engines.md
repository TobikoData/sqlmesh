# Execution Engines

# BigQuery
## BigQuery - Local/Built-in Scheduler
| Option          | Description                                                                                   |  Type  | Required |
|-----------------|-----------------------------------------------------------------------------------------------|:------:|:--------:|
| `method`        | Connection methods. Can be `oath`, `oauth-secrets`, `service-account`, `service-account-json` | string |    N     |
| `project`       | The name of the GCP project                                                                   | string |    N     |
| `location`      | The location of for the datasets (can be regional or multi-regional)                          | string |    Y     |
| `keyfile`       | Path to the keyfile to be used with service-account method                                    | string |    Y     |
| `keyfile_json`  | Keyfile information provided inline (not recommended)                                         |  dict  |    N     |
| `token`         | Oath secret auth token                                                                        | string |    N     |
| `refresh_token` | Oath secret auth refresh_token                                                                | string |    N     |
| `client_id`     | Oath secret auth client_id                                                                    | string |    N     |
| `client_secret` | Oath secret auth client_secret                                                                | string |    N     |
| `token_uri`     | Oath secret auth token_uri                                                                    | string |    N     |
| `scopes`        | Oath secret auth scopes                                                                       |  list  |    N     |

## BigQuery - Airflow Scheduler
**Engine Name:** `bigquery`

In order to share a common implementation across local and Airflow, SQLMesh BigQuery implements its own hook and operator. 

To enable support for this operator, the Airflow BigQuery provider package should be installed on the target Airflow cluster along with SQLMesh with the BigQuery extra:
```
pip install "apache-airflow-providers-google"
pip install "sqlmesh[bigquery]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target BigQuery account. Please see [GoogleBaseHook](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/common/hooks/base_google/index.html#airflow.providers.google.common.hooks.base_google.GoogleBaseHook) for more details.

By default, the connection ID is set to `sqlmesh_google_cloud_bigquery_default`, but it can be overridden using the `default_engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    default_engine_operator_args={
        "sqlmesh_gcp_conn_id": "<Connection ID>"
    },
)
```

# Databricks
## Databricks - Local/Built-in Scheduler
If your project contains Python models that use PySpark DataFrames AND you are using the built-in scheduler, then you must run plan/apply on a Databricks cluster. 
This can be done using the [Notebook magic](../reference/notebook.md) or by using the [CLI](../reference/cli.md).
This is something we are looking into improving &mdash; please leave us feedback in [our Slack channel](https://join.slack.com/t/tobiko-data/shared_invite/zt-1ma66d79v-a4dbf4DUpLAQJ8ptQrJygg) if this impacts you.
A potential workaround until this support is added is to use [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html). This will make it look like you are running on a cluster, and should theoretically work.

Databricks has a few options for connection types to choose from:
### Type: databricks (Recommended)
This type will automatically detect if you are running in an environment that already has a SparkSession defined. 
If it detects a SparkSession, then it assumes this is a Databricks SparkSession and uses that. 
If it doesn't detect a SparkSession, then it will use the connection configuration to connect to Databricks over
the [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html). 
See [databricks_sql configuration](#type--databrickssql) for the connection configuration.

### Type: databricks_spark_session
This connection type assumes that wherever you are running you have access to a Databricks SparkSession. 
This will simplify the required configuration to run since you will not need to provide connection configuration.

### Type: databricks_sql
This connection type assumes you only need to run SQL queries against Databricks.
If all of your models are SQL models or if Python doesn't use PySpark DataFrame, then this can be used.
Below is the connection configuration for this type:

| Option                  | Description                                                                                                                                                                              |  Type  | Required |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `server_hostname`       | Databricks instance host name                                                                                                                                                            | string |    Y     |
| `http_path`             | HTTP path, either to a DBSQL endpoint (such as `/sql/1.0/endpoints/1234567890abcdef`) or to a DBR interactive cluster (such as `/sql/protocolv1/o/1234567890123456/1234-123456-slid123`) | string |    Y     |
| `access_token`          | HTTP Bearer access token, such as Databricks Personal Access Token                                                                                                                       | string |    Y     |
| `http_headers`          | An optional dictionary of HTTP headers that will be set on every request                                                                                                                 |  dict  |    N     |
| `session_configuration` | An optional dictionary of Spark session parameters                                                                                                                                      |  dict  |    N     |

## Databricks - Airflow Scheduler
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
    engine_operator_args={
        "dbfs_location": "dbfs:/FileStore/sqlmesh",
        "existing_cluster_id": "1234-123456-slid123",
    }
)
```

# DuckDB
## DuckDB - Local/Built-in Scheduler
| Option     | Description                                                                  |  Type  | Required |
|------------|------------------------------------------------------------------------------|:------:|:--------:|
| `database` | The optional database name. If not specified, the in-memory database is used | string |    N     |

## DuckDB - Airflow
DuckDB only works when running locally; therefore it does not support Airflow. 

# Redshift
## Redshift - Local/Built-in Scheduler
| Option                  | Description                                                                                                 |  Type  | Required |
|-------------------------|-------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `user`                  | The username to use for authentication with the Amazon Redshift cluster                                     | string |    N     |
| `password`              | The password to use for authentication with the Amazon Redshift cluster                                     | string |    N     |
| `database`              | The name of the database instance to connect to                                                             | string |    N     |
| `host`                  | The hostname of the Amazon Redshift cluster                                                                 | string |    N     |
| `port`                  | The port number of the Amazon Redshift cluster                                                              |  int   |    N     |
| `ssl`                   | Is SSL enabled. SSL must be enabled when authenticating using IAM (Default: `True`)                         |  bool  |    N     |
| `sslmode`               | The security of the connection to the Amazon Redshift cluster. `verify-ca` and `verify-full` are supported. | string |    N     |
| `timeout`               | The number of seconds before the connection to the server will timeout.                                     |  int   |    N     |
| `tcp_keepalive`         | Is [TCP keepalive](https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive) used. (Default: `True`)           |  bool  |    N     |
| `application_name`      | The name of the application                                                                                 | string |    N     |
| `preferred_role`        | The IAM role preferred for the current connection                                                           | string |    N     |
| `principal_arn`         | The ARN of the IAM entity (user or role) for which you are generating a policy                              | string |    N     |
| `credentials_provider`  | The class name of the IdP that will be used for authenticating with the Amazon Redshift cluster             | string |    N     |
| `region`                | The AWS region of the Amazon Redshift cluster                                                               | string |    N     |
| `cluster_identifier`    | The cluster identifier of the Amazon Redshift cluster                                                       | string |    N     |
| `iam`                   | If IAM authentication is enabled. IAM must be True when authenticating using an IdP                         |  dict  |    N     |
| `is_serverless`         | If the Amazon Redshift cluster is serverless (Default: `False`)                                             |  bool  |    N     |
| `serverless_acct_id`    | The account ID of the serverless cluster                                                                    | string |    N     |
| `serverless_work_group` | The name of work group for serverless end point                                                             | string |    N     |

## Redshift - Airflow Scheduler
**Engine Name:** `redshift`

In order to share a common implementation across local and Airflow, SQLMesh Bigquery implements its own hook and operator. 

To enable support for this operator, the Airflow BigQuery provider package should be installed on the target Airflow cluster along with SQLMesh with the Redshift extra:
```
pip install "apache-airflow-providers-amazon"
pip install "sqlmesh[redshift]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Redshift account. Refer to [AmazonRedshiftConnection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html#authenticating-to-amazon-redshift) for details on how to define a connection string.

By default, the connection ID is set to `sqlmesh_redshift_default`, but it can be overridden using the `default_engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "redshift",
    default_engine_operator_args={
        "sqlmesh_redshift_conn_id": "<Connection ID>"
    },
)
```

# Snowflake
## Snowflake - Local/Built-in Scheduler
| Option          | Description                        |  Type  | Required |
|-----------------|------------------------------------|:------:|:--------:|
| `user`          | The Snowflake username             | string |    N     |
| `password`      | The Snowflake password             | string |    N     |
| `authenticator` | The Snowflake authenticator method | string |    N     |
| `account`       | The Snowflake account name         | string |    Y     |
| `warehouse`     | The Snowflake warehouse name       | string |    N     |
| `database`      | The Snowflake database name        | string |    N     |
| `role`          | The Snowflake role name            | string |    N     |

## Snowflake - Airflow Scheduler
**Engine Name:** `snowflake`

The SQLMesh Snowflake Operator is similar to the [SnowflakeOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html), and relies on the same [SnowflakeHook](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html) implementation.

To enable support for this operator, the Airflow Snowflake provider package should be installed on the target Airflow cluster along with SQLMesh with the Snowflake extra:
```
pip install "apache-airflow-providers-snowflake[common.sql]"
pip install "sqlmesh[snowflake]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Snowflake account. Refer to [Snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) for more details.

By default, the connection ID is set to `snowflake_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "snowflake",
    engine_operator_args={
        "snowflake_conn_id": "<Connection ID>"
    },
)
```

# Spark
## Spark - Local/Built-in Scheduler
Spark on Local/Built-in Scheduler is current unsupported. [Issue to track progress](https://github.com/TobikoData/sqlmesh/issues/575)

## Spark - Airflow Scheduler
**Engine Name:** `spark`

The SQLMesh Spark operator is very similar to the Airflow [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator), and relies on the same [SparkSubmitHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html#airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook) implementation.

To enable support for this operator, the Airflow Spark provider package should be installed on the target Airflow cluster as follows:
```
pip install apache-airflow-providers-apache-spark
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target cluster, queue, and deploy mode in which the Spark Job should be submitted. Refer to [Apache Spark connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html) for more details.

By default, the connection ID is set to `spark_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "spark",
    engine_operator_args={
        "connection_id": "<Connection ID>"
    },
)
```
Similarly, the `engine_operator_args` parameter can be used to override other job submission parameters, such as number of allocated cores, executors, and so forth. The full list of parameters that can be overridden can be found in `sqlmesh.schedulers.airflow.operators.spark_submit.SQLMeshSparkSubmitOperator`.

**Cluster mode**
<br><br>

Each Spark job submitted by SQLMesh is a PySpark application that depends on the SQLMesh library in its Driver process (but not in Executors). This means that if the Airflow connection is configured to submit jobs in `cluster` mode as opposed to `client` mode, the user must ensure that the SQLMesh Python library is installed on each node of a cluster where Spark jobs are submitted. This is because there is no way to know in advance which specific node to which a Driver process will be scheduled. No additional configuration is required if the deploy mode is set to `client`.
