# Execution Engines

## BigQuery
### BigQuery - Local/Built-in Scheduler
**Engine Adapter Type**: `bigquery`

| Option                          | Description                                                                                                    |  Type  | Required |
|---------------------------------|----------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `method`                        | Connection methods. Can be `oath`, `oauth-secrets`, `service-account`, `service-account-json`. Default: `oath` | string |    N     |
| `project`                       | The name of the GCP project                                                                                    | string |    N     |
| `location`                      | The location of for the datasets (can be regional or multi-regional)                                           | string |    N     |
| `keyfile`                       | Path to the keyfile to be used with service-account method                                                     | string |    N     |
| `keyfile_json`                  | Keyfile information provided inline (not recommended)                                                          |  dict  |    N     |
| `token`                         | OAuth 2.0 access token                                                                                         | string |    N     |
| `refresh_token`                 | OAuth 2.0 refresh token                                                                                        | string |    N     |
| `client_id`                     | OAuth 2.0 client ID                                                                                            | string |    N     |
| `client_secret`                 | OAuth 2.0 client secret                                                                                        | string |    N     |
| `token_uri`                     | OAuth 2.0 authorization server's toke endpoint URI                                                             | string |    N     |
| `scopes`                        | The scopes used to obtain authorization                                                                        |  list  |    N     |
| `job_creation_timeout_seconds`  | The maximum amount of time, in seconds, to wait for the underlying job to be created.                          |  int   |    N     |
| `job_execution_timeout_seconds` | The maximum amount of time, in seconds, to wait for the underlying job to complete.                            |  int   |    N     |
| `job_retries`                   | The number of times to retry the underlying job if it fails. (Default: `1`)                                    |  int   |    N     |
| `priority`                      | The priority of the underlying job. (Default: `INTERACTIVE`)                                                   | string |    N     |
| `maximum_bytes_billed`          | The maximum number of bytes to be billed for the underlying job.                                               |  int   |    N     |

### BigQuery - Airflow Scheduler
**Engine Name:** `bigquery`

In order to share a common implementation across local and Airflow, SQLMesh BigQuery implements its own hook and operator.

To enable support for this operator, the Airflow BigQuery provider package should be installed on the target Airflow cluster along with SQLMesh with the BigQuery extra:
```
pip install "apache-airflow-providers-google"
pip install "sqlmesh[bigquery]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target BigQuery account. Please see [GoogleBaseHook](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/common/hooks/base_google/index.html#airflow.providers.google.common.hooks.base_google.GoogleBaseHook) and [GCP connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)for more details. The goal is to create a working GCP connection on Airflow. The only difference is that you should use the `sqlmesh_google_cloud_bigquery_default` (by default) connection ID instead of the `google_cloud_default` one in the Airflow guide.

By default, the connection ID is set to `sqlmesh_google_cloud_bigquery_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    engine_operator_args={
        "bigquery_conn_id": "<Connection ID>"
    },
)
```

### Connection Methods
* [oath](https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.default) (default)
  * Related Credential Configuration:
    * `scopes` (Optional)
* [oauth-secrets](https://google-auth.readthedocs.io/en/stable/reference/google.oauth2.credentials.html)
  * Related Credential Configuration:
    * `token` (Optional): Can be None if refresh information is provided.
    * `refresh_token` (Optional): If specified, credentials can be refreshed.
    * `client_id` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
    * `client_secret` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
    * `token_uri` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
    * `scopes` (Optional): OAuth 2.0 credentials can not request additional scopes after authorization. The scopes must be derivable from the refresh token if refresh information is provided (e.g. The refresh token scopes are a superset of this or contain a wild card scope like 'https://www.googleapis.com/auth/any-api')
* [service-account](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_file)
  * Related Credential Configuration:
    * `keyfile` (Required)
    * `scopes` (Optional)
* [service-account-json](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_info)
  * Related Credential Configuration:
    * `keyfile_json` (Required)
    * `scopes` (Optional)

## Databricks
### Databricks - Local/Built-in Scheduler
**Engine Adapter Type**: `databricks`

If you are always running SQLMesh commands directly on a Databricks Cluster (like in a Databricks Notebook) then the only relevant configuration is `catalog` and it is optional.
The SparkSession provided by Databricks will be used to execute all SQLMesh commands.

Otherwise SQLMesh's Databricks implementation uses the [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html) to connect to Databricks by default.
If your project contains PySpark DataFrames in Python models then it will use [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html) to connect to Databricks.
SQLMesh's Databricks Connect implementation supports Databricks Runtime 13.0 or higher. If SQLMesh detects you have Databricks Connect installed then it will use it for all Python models (so both Pandas and PySpark DataFrames).

Databricks connect execution can be routed to a different cluster than the SQL Connector by setting the `databricks_connect_*` properties.
For example this allows SQLMesh to be configured to run SQL on a [Databricks SQL Warehouse](https://docs.databricks.com/sql/admin/create-sql-warehouse.html) while still routing DataFrame operations to a normal Databricks Cluster.

Note: If using Databricks Connect please note the [requirements](https://docs.databricks.com/dev-tools/databricks-connect.html#requirements) and [limitations](https://docs.databricks.com/dev-tools/databricks-connect.html#limitations)

| Option                               | Description                                                                                                                                                                              |  Type  | Required |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `server_hostname`                    | Databricks instance host name                                                                                                                                                            | string |    N     |
| `http_path`                          | HTTP path, either to a DBSQL endpoint (such as `/sql/1.0/endpoints/1234567890abcdef`) or to a DBR interactive cluster (such as `/sql/protocolv1/o/1234567890123456/1234-123456-slid123`) | string |    N     |
| `access_token`                       | HTTP Bearer access token, such as Databricks Personal Access Token                                                                                                                       | string |    N     |
| `catalog`                            | Spark 3.4+ Only if not using SQL Connector. The name of the catalog to use for the connection. Defaults to use Databricks cluster default (most likely `hive_metastore`).                | string |    N     |
| `http_headers`                       | SQL Connector Only: An optional dictionary of HTTP headers that will be set on every request                                                                                             |  dict  |    N     |
| `session_configuration`              | SQL Connector Only: An optional dictionary of Spark session parameters. Execute the SQL command `SET -v` to get a full list of available commands.                                       |  dict  |    N     |
| `databricks_connect_server_hostname` | Databricks Connect Only: Databricks Connect server hostname. Uses `server_hostname` if not set.                                                                                          | string |    N     |
| `databricks_connect_access_token`    | Databricks Connect Only: Databricks Connect access token. Uses `access_token` if not set.                                                                                                | string |    N     |
| `databricks_connect_cluster_id`      | Databricks Connect Only: Databricks Connect cluster ID. Uses `http_path` if not set. Cannot be a Databricks SQL Warehouse.                                                               | string |    N     |
| `force_databricks_connect`           | When running locally, force the use of Databricks Connect for all model operations (so don't use SQL Connector for SQL models)                                                           |  bool  |    N     |
| `disable_databricks_connect`         | When running locally, disable the use of Databricks Connect for all model operations (so use SQL Connector for all models)                                                               |  bool  |    N     |

### Databricks - Airflow Scheduler
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

## DuckDB
### DuckDB - Local/Built-in Scheduler
**Engine Adapter Type**: `duckdb`

| Option     | Description                                                                  |  Type  | Required |
|------------|------------------------------------------------------------------------------|:------:|:--------:|
| `database` | The optional database name. If not specified, the in-memory database is used | string |    N     |

### DuckDB - Airflow
DuckDB only works when running locally; therefore it does not support Airflow.

## Postgres
### Postgres - Local/Built-in Scheduler
**Engine Adapter Type**: `postgres`

| Option            | Description                                                                     |  Type  | Required |
|-------------------|---------------------------------------------------------------------------------|:------:|:--------:|
| `host`            | The hostname of the Postgres server                                             | string |    Y     |
| `user`            | The username to use for authentication with the Postgres server                 | string |    Y     |
| `password`        | The password to use for authentication with the Postgres server                 | string |    Y     |
| `port`            | The port number of the Postgres server                                          |  int   |    Y     |
| `database`        | The name of the database instance to connect to                                 | string |    Y     |
| `keepalives_idle` | The number of seconds between each keepalive packet sent to the server.         |  int   |    N     |
| `connect_timeout` | The number of seconds to wait for the connection to the server. (Default: `10`) |  int   |    N     |
| `role`            | The role to use for authentication with the Postgres server                     | string |    N     |
| `sslmode`         | The security of the connection to the Postgres server.                          | string |    N     |

### Postgres - Airflow
**Engine Name:** `postgres`

The SQLMesh Postgres Operator is similar to the [PostgresOperator](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/operators/postgres/index.html), and relies on the same [PostgresHook](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html) implementation.

To enable support for this operator, the Airflow Postgres provider package should be installed on the target Airflow cluster along with SQLMesh with the Postgres extra:
```
pip install "apache-airflow-providers-postgres"
pip install "sqlmesh[postgres]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Postgres account. Refer to [Postgres connection](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html) for more details.

By default, the connection ID is set to `postgres_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "postgres",
    engine_operator_args={
        "postgres_conn_id": "<Connection ID>"
    },
)
```

## Redshift
### Redshift - Local/Built-in Scheduler
**Engine Adapter Type**: `Redshift`

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

### Redshift - Airflow Scheduler
**Engine Name:** `redshift`

In order to share a common implementation across local and Airflow, SQLMesh's Redshift engine implements its own hook and operator.

To enable support for this operator, the Airflow Redshift provider package should be installed on the target Airflow cluster along with SQLMesh with the Redshift extra:
```
pip install "apache-airflow-providers-amazon"
pip install "sqlmesh[redshift]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Redshift account. Refer to [AmazonRedshiftConnection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html#authenticating-to-amazon-redshift) for details on how to define a connection string.

By default, the connection ID is set to `sqlmesh_redshift_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "redshift",
    engine_operator_args={
        "redshift_conn_id": "<Connection ID>"
    },
)
```

## GCP Postgres
### GCP Postgres - Local/Built-in Scheduler
**Engine Adapter Type**: `postgres`

| Option                    | Description                                                                         |  Type   | Required |
|---------------------------|-------------------------------------------------------------------------------------|:-------:|:--------:|
| `instance_connection_str` | Connection name for the postgres instance                                           | string  |    Y     |
| `user`                    | The username (posgres or IAM) to use for authentication                             | string  |    Y     |
| `password`                | The password to use for authentication. Required when connecting as a Postgres user | string  |    N     |
| `enable_iam_auth`         | Enables IAM authentication. Required when connecting as an IAM user                 | boolean |    N     |
| `db`                      | The name of the database instance to connect to                                     | string  |    Y     |

## Snowflake
### Snowflake - Local/Built-in Scheduler
**Engine Adapter Type**: `snowflake`

| Option          | Description                        |  Type  | Required |
|-----------------|------------------------------------|:------:|:--------:|
| `user`          | The Snowflake username             | string |    N     |
| `password`      | The Snowflake password             | string |    N     |
| `authenticator` | The Snowflake authenticator method | string |    N     |
| `account`       | The Snowflake account name         | string |    Y     |
| `warehouse`     | The Snowflake warehouse name       | string |    N     |
| `database`      | The Snowflake database name        | string |    N     |
| `role`          | The Snowflake role name            | string |    N     |

### Snowflake - Airflow Scheduler
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

## Spark
### Spark - Local/Built-in Scheduler
**Engine Adapter Type**: `spark`

| Option          | Description                                               |  Type  | Required |
|-----------------|-----------------------------------------------------------|:------:|:--------:|
| `config_dir`    | Value to set for `SPARK_CONFIG_DIR`                       | string |    N     |
| `catalog`       | Spark 3.4+ Only. The catalog to use when issuing commands | string |    N     |
| `config`        | Key/value pairs to set for the Spark Configuration.       |  dict  |    N     |

### Spark - Airflow Scheduler
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
