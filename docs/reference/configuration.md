# Configuration

This page lists SQLMesh configuration options and their parameters. Learn more about SQLMesh configuration in the [configuration guide](../guides/configuration.md).

## Root configurations

A SQLMesh project configuration consists of root level parameters within which other parameters are defined.

Three important root level parameters are [`gateways`](#gateways), [gateway/connection defaults](#gatewayconnection-defaults), and [`model_defaults`](#models), which have their own sections below.

This section describes the other root level configuration parameters.

### Projects

Configuration options for SQLMesh project directories.

| Option                       | Description                                                                                                                                                                                                                                                                                        | Type                 | Required |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------:|:--------:|
| `ignore_patterns`            | Files that match glob patterns specified in this list are ignored when scanning the project folder (Default: `[]`)                                                                                                                                                                                 | list[string]         | N        |

### Environments

Configuration options for SQLMesh environment creation and promotion.

| Option                       | Description                                                                                                                                                                                                                                                                                        | Type                 | Required |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------:|:--------:|
| `snapshot_ttl`               | The period of time that a model snapshot not a part of any environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`) | string               | N        |
| `environment_ttl`            | The period of time that a development environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`)                      | string               | N        |
| `pinned_environments`        | The list of development environments that are exempt from deletion due to expiration                                                                                                                                                                                                               | list[string]         | N        |
| `include_unmodified`         | Indicates whether to create views for all models in the target development environment or only for modified ones                                                                                                                                                                                   | boolean              | N        |
| `time_column_format`         | The default format to use for all model time columns. This time format uses [python format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) (Default: `%Y-%m-%d`)                                                                                        | string               | N        |
| `auto_categorize_changes`    | Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type ([additional details](../guides/configuration.md#auto-categorize-changes))                                                                      | dict[string, string] | N        |
| `default_target_environment` | The name of the environment that will be the default target for the `sqlmesh plan` and `sqlmesh run` commands. (Default: `prod`)                                                                                                                                                                   | string               | N        |
| `physical_schema_override`   | A mapping from model schema names to names of schemas in which physical tables for the corresponding models will be placed - [addition details](../guides/configuration.md#physical-schema-override). (Default physical schema name: `sqlmesh__[model schema]`)                                                                                                                                                                   | string               | N        |
| `environment_suffix_target`   | Whether SQLMesh views should append their environment name to the `schema` or `table` - [additional details](../guides/configuration.md#view-schema-override). (Default: `schema`)                                                                                                                                                                   | string               | N        |


## Gateways

The `gateways` dictionary defines how SQLMesh should connect to the data warehouse, state backend, test backend, and scheduler. A gateway does not need to specify all four components and will use defaults if any are omitted - more information is provided about [gateway defaults](#gatewayconnection-defaults) below.

Find additional information about gateways in the configuration overview page [gateways section](../guides/configuration.md#gateways).

### Gateway

Configuration for a gateway.

#### State schema name

By default, the schema name used to store state tables is `sqlmesh`. This can be changed by providing the `state_schema` config key in the gateway configuration.

| Option             | Description                                                                                                                     | Type | Required |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------|:----:|:--------:|
| `state_schema` | The name of the schema where state information should be stored. (Default: `sqlmesh`) | string  |    N     |


### Connection

Configuration for the data warehouse connection.

Most parameters are specific to the connection engine `type` - see [below](#engine-connection-configuration). The default data warehouse connection type is an in-memory DuckDB database.

#### Concurrent tasks

| Option             | Description                                                                                                                     | Type | Required |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------|:----:|:--------:|
| `concurrent_tasks` | The maximum number of concurrent tasks that will be run by SQLMesh. (Default: 4 for engines that support concurrent tasks.) | int  |    N     |

#### Engine connection configuration

These pages describe the connection configuration options for each execution engine.

* [BigQuery](../integrations/engines/bigquery.md)
* [Databricks](../integrations/engines/databricks.md)
* [DuckDB](../integrations/engines/duckdb.md)
* [MySQL](../integrations/engines/mysql.md)
* [MSSQL](../integrations/engines/mssql.md)
* [Postgres](../integrations/engines/postgres.md)
* [GCP Postgres](../integrations/engines/gcp-postgres.md)
* [Redshift](../integrations/engines/redshift.md)
* [Snowflake](../integrations/engines/snowflake.md)
* [Spark](../integrations/engines/spark.md)

### Scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md).

By default, the scheduler type is set to `builtin` and uses the gateway's connection to store metadata. Use the `airflow` type to integrate with Airflow.

Below is the list of configuration options specific to each corresponding scheduler type. Find additional details in the [configuration overview scheduler section](../guides/configuration.md#scheduler).

#### Builtin

No configuration options are supported by this scheduler type.

#### Airflow

See [Airflow Integration Guide](../integrations/airflow.md) for information about how to integrate Airflow with SQLMesh.

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


#### Cloud Composer

The Google Cloud Composer scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`. Cloud Composer relies on `gcloud` authentication, so the `username` and `password` options are not required.

## Gateway/connection defaults

The default gateway and connection keys specify what should happen when gateways or connections are not explicitly specified. Find additional details in the configuration overview page [gateway/connection defaults section](../guides/configuration.md#gatewayconnection-defaults).

### Default gateway

If a configuration contains multiple gateways, SQLMesh will use the first one in the `gateways` dictionary by default. The `default_gateway` key is used to specify a different gateway name as the SQLMesh default.

| Option                    | Description                                                                                                                                            | Type       | Required |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|:----------:|:--------:|
| `default_gateway`         | The name of a gateway to use if one is not provided explicitly (Default: the gateway defined first in the `gateways` option)                           | string     | N        |

### Default connections/scheduler

The `default_connection`, `default_test_connection`, and `default_scheduler` keys are used to specify shared defaults across multiple gateways.

For example, you might have a specific connection where your tests should run regardless of which gateway is being used. Instead of duplicating the test connection information in each gateway specification, specify it once in the `default_test_connection` key.

| Option                    | Description                                                                                                                                            | Type       | Required |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|:----------:|:--------:|
| `default_connection`      | The default connection to use if one is not specified in a gateway (Default: A DuckDB connection that creates an in-memory database)                   | connection | N        |
| `default_test_connection` | The default connection to use when running tests if one is not specified in a gateway (Default: A DuckDB connection that creates an in-memory database | connection) | N        |
| `default_scheduler`       | The default scheduler configuration to use if one is not specified in a gateway (Default: built-in scheduler)                                          | scheduler  | N        |

## Models

Configuration options for SQLMesh models.

The `model_defaults` configuration is **required** and must contain a value for the `dialect` key. All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py) are allowed.

Other values are set automatically unless explicitly overridden in the model definition. Find additional details in the [configuration overview page models section](../guides/configuration.md#models).

| Option           | Description                                                                                                                                                                                                                                                                                                    |      Type      | Required |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------:|:--------:|
| `kind`           | The default model kind ([Additional Details](#model-kind)) (Default: `VIEW`)                                                                                                                                                                                                                                          | string or dict |    N     |
| `dialect`        | The SQL dialect in which the model's query is written                                                                                                                                                                                                                                                                |     string     |    N     |
| `cron`           | The default cron expression specifying how often the model should be refreshed. (Default: `@daily`.)                                                                                                                                                                                                                                  |     string     |    N     |
| `owner`          | The owner of a model; may be used for notification purposes                                                                                                                                                                                                                                                   |     string     |    N     |
| `start`          | The date/time that determines the earliest date interval that should be processed by a model. This value is used to identify missing data intervals during plan application and restatement. The value can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.      | string or int  |    N     |
| `batch_size`     | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. (Default: `None`) |      int       |    N     |
| `storage_format` | The storage format that should be used to store physical tables; only applicable to engines such as Spark                                                                                                                                                                                                     |     string     |    N     |
| `depends_on` | Models on which this model depends. (Default: dependencies inferred from model code.)                                                                                                                                                                                                     |     array[string]     |    N     |


## Debug mode

To enable debug mode set the `SQLMESH_DEBUG` environment variable to one of the following values: "1", "true", "t", "yes" or "y". Enabling this mode ensures that full backtraces are printed when using CLI. The default log level is set to `DEBUG` when this mode is enabled.

Example enabling debug mode for the CLI command `sqlmesh plan`:

```bash
$ SQLMESH_DEBUG=1 sqlmesh plan
```
