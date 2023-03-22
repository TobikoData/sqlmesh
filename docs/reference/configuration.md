# Configuration

This page lists the available SQLMesh configurations that can be set either as environment variables in the `config.yaml` file within a project folder, or in the file with the same name in the `~/.sqlmesh` folder.

Configuration options from different sources have the following order of precedence:

1. Set as an environment variable (for example, `SQLMESH__MODEL_DEFAULTS__DIALECT`).
2. Set in `config.yaml` in a project folder.
3. Set in `config.yaml` in the `~/.sqlmesh` folder.

## Connections

A dictionary of supported connections and their configurations. The key represents a unique connection name. If there is only one connection, its configuration can be provided directly, omitting the dictionary.

```yaml linenums="1"
connections:
    my_connection:
        type: snowflake
        user: <username>
        password: <password>
        account: <account>
```

### Root Level Connection Config
| Option               | Description                                                                                                         |  Type  | Required |
|----------------------|---------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `default_connection` | The name of a connection to use by default (Default: A connection defined first in the `connections` option)        | string |    N     |
| `test_connection`    | The name of a connection to use when running tests (Default: A DuckDB connection that creates an in-memory database | string |    N     |

### Shared Connection Config
| Option             | Description                                                        | Type | Required |
|--------------------|--------------------------------------------------------------------|:----:|:--------:|
| `concurrent_tasks` | The maximum number of concurrent tasks that will be run by SQLMesh | int  |    N     |

### Duckdb
#### Local/Built-in Scheduler
| Option     | Description                                                                  |  Type  | Required |
|------------|------------------------------------------------------------------------------|:------:|:--------:|
| `database` | The optional database name. If not specified, the in-memory database is used | string |    N     |

##### Airflow
DuckDB only works when running locally and therefore does not support Airflow. 

### Snowflake
#### Local/Built-in Scheduler
| Option      | Description                  |  Type  | Required |
|-------------|------------------------------|:------:|:--------:|
| `user`      | The Snowflake username       | string |    Y     |
| `password`  | The Snowflake password       | string |    Y     |
| `account`   | The Snowflake account name   | string |    Y     |
| `warehouse` | The Snowflake warehouse name | string |    N     |
| `database`  | The Snowflake database name  | string |    N     |
| `role`      | The Snowflake role name      | string |    N     |

#### Airflow Scheduler
[Airflow Configuration Information](../integrations/airflow.md#snowflake)

### Databricks
#### Local/Built-in Scheduler
If your project contains Python models that use PySpark DataFrames AND you are using the built-in scheduler, then you must run plan/apply on a Databricks cluster. 
This can be done using the [Notebook magic](../reference/notebook.md) or using the [CLI](../reference/cli.md).
This is something we are looking into improving and please leave us feedback in slack if this impacts you.
A potential workaround until this support is added is to use [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html). This will make it look like you are running on a cluster and should theoretically work.

Databricks has a few options for connection types to choose from:
###### Type: databricks
The recommended connection type configuration for Databricks is the `databricks` type. 
This type will automatically detect if you are running in an environment that already has a SparkSession defined. 
If it detects when exists then it assumes this is a Databricks SparkSession and uses that. 
If it doesn't detect a SparkSession then it will use the connection configuration to connect to Databricks over
the [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html). 
See [databricks_sql configuration](#type--databrickssql) for the connection configuration.

###### Type: databricks_spark_session
This connection type assumes that wherever you are running you have access to a Databricks SparkSession. 
This will simplify the required configuration to run since you will not need to provide connection configuration.

###### Type: databricks_sql
This connection type assumes you only need to run SQL queries against Databricks.
If all of your models are SQL models or if Python don't use PySpark DataFrame then this can be used.
Below is the connection configuration for this type:

| Option                  | Description                                                                                                                                                                              |  Type  | Required |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `server_hostname`       | Databricks instance host name                                                                                                                                                            | string |    Y     |
| `http_path`             | HTTP path, either to a DBSQL endpoint (such as `/sql/1.0/endpoints/1234567890abcdef`) or to a DBR interactive cluster (such as `/sql/protocolv1/o/1234567890123456/1234-123456-slid123`) | string |    Y     |
| `access_token`          | HTTP Bearer access token, such as Databricks Personal Access Token                                                                                                                       | string |    Y     |
| `http_headers`          | An optional dictionary of HTTP headers that will be set on every request                                                                                                                 |  dict  |    N     |
| `session_configuration` | An optional dictionary of Spark session parameters.                                                                                                                                      |  dict  |    N     |

#### Airflow Scheduler
[Airflow Configuration Information](../integrations/airflow.md#databricks)

### Bigquery
#### Local/Built-in Scheduler
Currently relies on local configuration of `gcloud` CLI to be authenticated in order to connect. 
[Issue to expand supported methods](https://github.com/TobikoData/sqlmesh/issues/270).

#### Airflow Scheduler
[Airflow Configuration Information](../integrations/airflow.md#bigquery)

### Redshift
#### Local/Built-in Scheduler
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

#### Airflow Scheduler
[Airflow Configuration Information](../integrations/airflow.md#redshift)

## Scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md). By default, the scheduler type is set to `builtin`, which uses the existing SQL engine to store metadata, and that has a simple scheduler. The `airflow` type should be set if you want to integrate with Airflow and is recommended for production deployments.

Below is the list of configuration options specific to each corresponding scheduler type.

### Builtin
```yaml linenums="1"
scheduler:
    type: builtin
```

No additional configuration options are supported by this scheduler type.

### Airflow
```yaml linenums="1"
scheduler:
    type: airflow
    airflow_url: <airflow_url>
    username: <username>
    password: <password>
```

| Option                            | Description                                                                                                                        |  Type  | Required |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `airflow_url`                     | The URL of the Airflow Webserver                                                                                                   | string |    Y     |
| `username`                        | The Airflow username                                                                                                               | string |    Y     |
| `password`                        | The Airflow password                                                                                                               | string |    Y     |
| `dag_run_poll_interval_secs`      | Determines, in seconds, how often a running DAG can be polled (Default: `10`)                                                      |  int   |    N     |
| `dag_creation_poll_interval_secs` | Determines, in seconds, how often SQLMesh should check whether a DAG has been created (Default: `30`)                              |  int   |    N     |
| `dag_creation_max_retry_attempts` | Determines the maximum number of attempts that SQLMesh will make while checking for whether a DAG has been created (Default: `10`) |  int   |    N     |
| `backfill_concurrent_tasks`       | The number of concurrent tasks used for model backfilling during plan application (Default: `4`)                                   |  int   |    N     |
| `ddl_concurrent_tasks`            | The number of concurrent tasks used for DDL operations like table/view creation, deletion, and so forth (Default: `4`)             |  int   |    N     |

See [Airflow Integration Guide](../integrations/airflow.md) for detailed information on how to set up Airflow with SQLMesh.

### Cloud Composer
```yaml linenums="1"
scheduler:
    type: cloud_composer
    airflow_url: <airflow_url>
```
This scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`. 
Cloud Composer relies on `gcloud` authentication, so the `username` and `password` options are not required.

See [Airflow Integration Guide](../integrations/airflow.md) for detailed information on how to set up Airflow with SQLMesh.

## SQLMesh Specific Configurations
| Option                    | Description                                                                                                                                                                                                                                                                                        |         Type         | Required |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------:|:--------:|
| `physical_schema`         | The default schema used to store physical tables for models. (Default: `sqlmesh`)                                                                                                                                                                                                                  |        string        |    N     |
| `snapshot_ttl`            | The period of time that a model snapshot not a part of any environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`) |        string        |    N     |
| `environment_ttl`         | The period of time that a development environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`)                      |        string        |    N     |
| `ignore_patterns`         | Files that match glob patterns specified in this list are ignored when scanning the project folder. (Default: `[]`)                                                                                                                                                                                |     list[string]     |    N     |
| `time_column_format`      | The default format to use for all model time columns. This time format uses [python format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes). (Default: `%Y-%m-%d`)                                                                                       |        string        |    N     |
| `auto_categorize_changes` | Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type. [Additional Details](#auto-categorize-changes)                                                                       | dict[string, string] |    N     |

## Model Configuration
This section contains options that are specific to models, and that are set automatically unless explicitly overriden in the model definition.

```yaml linenums="1"
model_defaults:
    dialect: snowflake
    owner: jen
    start: 2022-01-01
```
| Option           | Description                                                                                                                                                                                                                                                                                                    |      Type      | Required |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------:|:--------:|
| `kind`           | The default model kind. [Additional Details](#kind) (Default: `full`)                                                                                                                                                                                                                                          | string or dict |    N     |
| `dialect`        | The SQL dialect the model's query is written in                                                                                                                                                                                                                                                                |     string     |    N     |
| `cron`           | The default cron expression specifying how often the model should be refreshed                                                                                                                                                                                                                                 |     string     |    N     |
| `owner`          | The owner of a model. May be used for notification purposes.                                                                                                                                                                                                                                                   |     string     |    N     |
| `start`          | The date/time that determines the earliest data interval that should be processed by a model. This value is used to identify missing data intervals during plan application and restatement. The value can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.      | string or int  |    N     |
| `batch_size`     | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. |      int       |    N     |
| `storage_format` | The storage format that should be used to store physical tables. Only applicable to engines such as Spark.                                                                                                                                                                                                     |     string     |    N     |

## Additional Details

### Auto Categorize Changes
Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type.

Default values are set as follows:

```yaml linenums="1"
auto_categorize_changes:
    python: off
    sql: full
    seed: full
```

Supported values:

* `full`: Never prompt the user for input, instead fall back to the most conservative category ([breaking](../concepts/plans.md#breaking-change)) if the category can't be determined automatically.
* `semi`: Prompt the user for input only if the change category can't be determined automatically.
* `off`: Always prompt the user for input; automatic categorization will not be attempted.

### Kind
The default model kind. For more information, refer to [model kinds](../concepts/models/model_kinds.md). 

Example:

```yaml linenums="1"
model_defaults:
    kind: full
```

Alternatively, if a kind requires additional parameters, it can be provided as an object:

```yaml linenums="1"
model_defaults:
    kind:
        name: incremental_by_time_range
        time_column: ds
```
