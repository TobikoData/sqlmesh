# Configuration

This page provides information about SQLMesh configuration and lists the available configuration parameters.

Currently, this page only describes how to specify configuration parameters using YAML; details about Python configuration are forthcoming.

## Configuration files

**NOTE:** SQLMesh project configurations have the following two requirements:

1. A `config.yaml` or `config.py` file must be present in the project's folder.
2. That configuration file must contain a default SQL dialect for the project's models in the [`model_defaults` `dialect` key](#models).

SQLMesh configuration parameters can be set as environment variables, in the configuration file within a project folder, or in a configuration file in the `~/.sqlmesh` folder.

The sources have the following order of precedence:

1. Environment variable (e.g., `SQLMESH__MODEL_DEFAULTS__DIALECT`). [HIGHEST PRECEDENCE]
2. `config.yaml` or `config.py` in a project folder.
3. `config.yaml` or `config.py` in the `~/.sqlmesh` folder. [LOWEST PRECEDENCE]

## Root configurations

A SQLMesh project configuration consists of root level parameters within which other parameters are defined.

Three important root level parameters are [`gateways`](#gateways), [gateway/connection defaults](#gatewayconnection-defaults), and [`model_defaults`](#models), which are described in their own sections below.

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
| `auto_categorize_changes`    | Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type ([additional details](#auto-categorize-changes))                                                                      | dict[string, string] | N        |
| `default_target_environment` | The name of the environment that will be the default target for the `sqlmesh plan` and `sqlmesh run` commands. (Default: `prod`)                                                                                                                                                                   | string               | N        |
| `physical_schema_override`   | The name of the schema where SQLMesh should place physical tables.

### Virtual Data Environments

Configuration options for SQLMesh virtual data environment schemas.

#### Physical Schema Override
By default, SQLMesh creates physical tables for a model with a naming convention of `sqlmesh__[model schema]`.

This can be overridden on a per-schema basis using the `physical_schema_override` option, which removes the `sqlmesh__` prefix and uses the name you provide.

This example configuration overrides the default physical schemas for the `db` model schema:

```yaml linenums="1"
physical_schema_override:
  db: my_db
```

If you had a model name of `db.table`, the physical table would be created as `my_db.table_<fingerprint>` instead of the default behavior of `sqlmesh__db.table_<fingerprint>`.

This key only applies to the _physical tables_ that SQLMesh creates - the views are still created in `db` (prod) or `db__<env>`.

#### View Schema Override

By default SQLMesh appends the environment name to the schema name when creating new environments. This can be changed to append a suffix at the end of table instead. This means that new environment views will be created in the same schema as production but be differentiated by having their names end with `__<env>`.

Config example:

```yaml linenums="1"
environment_suffix_target: table
```

If you created a `dev` environment for a project containing a model named `db.users`, the model view would be created as `db.users__dev` instead of the default behavior of `db__dev.users`.

The default behavior of appending the suffix to schemas is recommended because it leaves production with a single clean interface for accessing the views. However, if you are deploying SQLMesh in an environment with tight restrictions on schema creation then this can be a useful way of reducing the number of schemas SQLMesh uses.

### Auto categorize changes
Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type.

Supported values:

* `full`: Never prompt the user for input, instead fall back to the most conservative category ([breaking](../concepts/plans.md#breaking-change)) if the category can't be determined automatically.
* `semi`: Prompt the user for input only if the change category can't be determined automatically.
* `off`: Always prompt the user for input; automatic categorization will not be attempted.

Example showing default values:

```yaml linenums="1"
auto_categorize_changes:
    python: off
    sql: full
    seed: full
```

## Gateways

The `gateways` dictionary defines how SQLMesh should connect to the data warehouse, state backend, and scheduler.

Each gateway key represents a unique gateway name and configures its connections. For example, this configures the `my_gateway` gateway:

```yaml linenums="1"
gateways:
    my_gateway:
        connection:
            ...
        state_connection:
            ...
        test_connection:
            ...
        scheduler:
            ...
```

Gateways do not need to specify all four components in the example above - more information is provided about [gateway defaults](#gatewayconnection-defaults) below.

### Connection

Configuration for the data warehouse connection.

The allowed keys include:

- The optional [`concurrent_tasks`](#concurrent-tasks) key specifies the maximum number of concurrent tasks SQLMesh will run. Default value is 4 for engines that support concurrent tasks.
- Most keys are specific to the connection engine `type` - see [below](#engine-connection-configuration). The default data warehouse connection type is an in-memory DuckDB database.

Example snowflake connection configuration:

```yaml linenums="1"
gateways:
    my_gateway:
        connection:
            type: snowflake
            user: <username>
            password: <password>
            account: <account>
```

#### Concurrent tasks

| Option             | Description                                                                                                                     | Type | Required |
|--------------------|---------------------------------------------------------------------------------------------------------------------------------|:----:|:--------:|
| `concurrent_tasks` | The maximum number of concurrent tasks that will be run by SQLMesh. Default value is 4 for engines that support concurrent tasks. | int  |    N     |

#### Engine connection configuration

These pages describe the connection configuration options for each execution engine.

* [BigQuery](../integrations/engines/bigquery.md)
* [Databricks](../integrations/engines/databricks.md)
* [DuckDB](../integrations/engines/duckdb.md)
* [MySQL](../integrations/engines/mysql.md)
* [Postgres](../integrations/engines/postgres.md)
* [GCP Postgres](../integrations/engines/gcp-postgres.md)
* [Redshift](../integrations/engines/redshift.md)
* [Snowflake](../integrations/engines/snowflake.md)
* [Spark](../integrations/engines/spark.md)

### State connection

Configuration for the state backend connection if different from the data warehouse connection. The data warehouse connection is used if the `state_connection` key is not specified.

Example postgres state connection configuration:

```yaml linenums="1"
gateways:
    my_gateway:
        state_connection:
            type: postgres
            host: <host>
            port: <port>
            user: <username>
            password: <password>
            database: <database>
```

#### State schema name

By default, the schema name used to store state tables is `sqlmesh`. This can be changed by providing the `state_schema` config key in the gateway configuration.

Example configuration to store state information in a postgres database's `custom_name` schema:

```yaml linenums="1"
gateways:
    my_gateway:
        state_connection:
            type: postgres
            host: <host>
            port: <port>
            user: <username>
            password: <password>
            database: <database>
        state_schema: custom_name
```

This would create all state tables in the schema `custom_name`.

### Test connection

Configuration for a connection used to run unit tests. An in-memory DuckDB database is used if the `test_connection` key is not specified.

```yaml linenums="1"
gateways:
    my_gateway:
        test_connection:
            type: duckdb
```


### Scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md). By default, the scheduler type is set to `builtin`, which uses the existing SQL engine to store metadata. Use the `airflow` type integrate with Airflow.

Below is the list of configuration options specific to each corresponding scheduler type.

#### Builtin

Example configuration:

```yaml linenums="1"
gateways:
    my_gateway:
        scheduler:
            type: builtin
```

No additional configuration options are supported by this scheduler type.

#### Airflow

Example configuration:

```yaml linenums="1"
gateways:
    my_gateway:
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

See [Airflow Integration Guide](../integrations/airflow.md) for information about how to integrate Airflow with SQLMesh.

#### Cloud Composer

The Google Cloud Composer scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`. Cloud Composer relies on `gcloud` authentication, so the `username` and `password` options are not required.

Example configuration:

```yaml linenums="1"
gateways:
    my_gateway:
        scheduler:
            type: cloud_composer
            airflow_url: <airflow_url>
```

## Gateway/connection defaults

The default gateway and connection keys specify what gateway/connection should be used if a command does not explicitly specify one or some gateways in the `gateways` dictionaries do not specify every connection.

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

Other values are set automatically unless explicitly overridden in the model definition.

| Option           | Description                                                                                                                                                                                                                                                                                                    |      Type      | Required |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------:|:--------:|
| `kind`           | The default model kind ([Additional Details](#model-kind)) (Default: `VIEW`)                                                                                                                                                                                                                                          | string or dict |    N     |
| `dialect`        | The SQL dialect in which the model's query is written                                                                                                                                                                                                                                                                |     string     |    N     |
| `cron`           | The default cron expression specifying how often the model should be refreshed                                                                                                                                                                                                                                 |     string     |    N     |
| `owner`          | The owner of a model; may be used for notification purposes                                                                                                                                                                                                                                                   |     string     |    N     |
| `start`          | The date/time that determines the earliest date interval that should be processed by a model. This value is used to identify missing data intervals during plan application and restatement. The value can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.      | string or int  |    N     |
| `batch_size`     | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. |      int       |    N     |
| `storage_format` | The storage format that should be used to store physical tables; only applicable to engines such as Spark                                                                                                                                                                                                     |     string     |    N     |
| `depends_on` | Models on which this model depends. Default: dependencies inferred from model code.                                                                                                                                                                                                     |     array     |    N     |

Example configuration:

```yaml linenums="1"
model_defaults:
    dialect: snowflake
    owner: jen
    start: 2022-01-01
```

### Model Kind
The default model kind is 'view' unless overridden with the `kind` key. For more information, refer to [model kinds](../concepts/models/model_kinds.md).

Example:

```yaml linenums="1"
model_defaults:
    kind: full
```

If a kind requires additional parameters it can be provided as an object:

```yaml linenums="1"
model_defaults:
    kind:
        name: incremental_by_time_range
        time_column: ds
```

## Debug mode
To enable debug mode set the `SQLMESH_DEBUG` environment variable to one of the following values: "1", "true", "t", "yes" or "y". Enabling this mode ensures that full backtraces are printed when using CLI. Additionally the default log level is set to `DEBUG` when this mode is enabled.

Example enabling debug mode for the CLI command `sqlmesh plan`:

```bash
$ SQLMESH_DEBUG=1 sqlmesh plan
```
