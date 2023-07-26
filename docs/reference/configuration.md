# Configuration

This page lists the available SQLMesh configurations that can be set either as environment variables in the `config.yaml` file within a project folder, or in the file with the same name in the `~/.sqlmesh` folder.

Configuration options from different sources have the following order of precedence:

1. Set as an environment variable (for example, `SQLMESH__MODEL_DEFAULTS__DIALECT`).
2. Set in `config.yaml` in a project folder.
3. Set in `config.yaml` in the `~/.sqlmesh` folder.

**NOTE**: A SQLMesh project **must** contain a configuration file in its directory - it cannot solely rely on environment variables or a configuration file in `~/.sqlmesh`. The SQLMesh project configuration file **must** contain a default SQL dialect for the project's models in the [`model_defaults` `dialect` key](#model-configuration).

## Debug mode
To enable the debug mode set the `SQLMESH_DEBUG` environment variable to one of the following values: "1", "true", "t", "yes" or "y". Enabling this mode ensures that full backtraces are printed when using CLI. Additionally the default log level is set to `DEBUG` when this mode is enabled.

## Gateways

A dictionary of supported gateways. Each gateway configuration defines how SQLMesh should connect to the data warehouse, the state backend and the scheduler. The key represents a unique gateway name.

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

### Connection

Configuration for the data warehouse connection. Default: a DuckDB connection that creates an in-memory database.

```yaml linenums="1"
gateways:
    my_gateway:
        connection:
            type: snowflake
            user: <username>
            password: <password>
            account: <account>
```

#### Shared connection configuration
| Option             | Description                                                        | Type | Required |
|--------------------|--------------------------------------------------------------------|:----:|:--------:|
| `concurrent_tasks` | The maximum number of concurrent tasks that will be run by SQLMesh | int  |    N     |

#### Engine connection configuration
* [BigQuery](../integrations/engines.md#bigquery-localbuilt-in-scheduler)
* [Databricks](../integrations/engines.md#databricks-localbuilt-in-scheduler)
* [DuckDB](../integrations/engines.md#duckdb-localbuilt-in-scheduler)
* [Postgres](../integrations/engines.md#postgres-localbuilt-in-scheduler)
* [Redshift](../integrations/engines.md#redshift-localbuilt-in-scheduler)
* [Snowflake](../integrations/engines.md#snowflake-localbuilt-in-scheduler)
* [Spark](../integrations/engines.md#spark-localbuilt-in-scheduler)

### State connection

Configuration for the state backend connection if different from the data warehouse connection. Default: the data warehouse connection.

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

### Test connection

Configuration for a connection used when running unit tests. Default: a DuckDB connection that creates an in-memory database.

```yaml linenums="1"
gateways:
    my_gateway:
        test_connection:
            type: duckdb
```


### Scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md). By default, the scheduler type is set to `builtin`, which uses the existing SQL engine to store metadata, and that has a simple scheduler. The `airflow` type should be set if you want to integrate with Airflow and is recommended for production deployments.

Below is the list of configuration options specific to each corresponding scheduler type.

#### Builtin
```yaml linenums="1"
gateways:
    my_gateway:
        scheduler:
            type: builtin
```

No additional configuration options are supported by this scheduler type.

#### Airflow
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
```yaml linenums="1"
scheduler:
    type: cloud_composer
    airflow_url: <airflow_url>
```
This scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`.
Cloud Composer relies on `gcloud` authentication, so the `username` and `password` options are not required.

See [Airflow Integration Guide](../integrations/airflow.md) for information on how to integrate Airflow with SQLMesh.

### Root level gateway configuration
| Option                    | Description                                                                                                                                            | Type       | Required |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|:----------:|:--------:|
| `default_connection`      | The default connection to use if one is not specified in a gateway (Default: A DuckDB connection that creates an in-memory database)                   | connection | N        |
| `default_test_connection` | The default connection to use when running tests if one is not specified in a gateway (Default: A DuckDB connection that creates an in-memory database | connection | N        |
| `default_scheduler`       | The default scheduler configuration to use if one is not specified in a gateway (Default: built-in scheduler)                                          | scheduler  | N        |
| `default_gateway`         | The name of a gateway to use if one is not provided explicitly (Default: the gateway defined first in the `gateways` option)                           | string     | N        |

## SQLMesh-specific configurations
| Option                    | Description                                                                                                                                                                                                                                                                                        | Type                 | Required |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------:|:--------:|
| `snapshot_ttl`            | The period of time that a model snapshot not a part of any environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`) | string               | N        |
| `environment_ttl`         | The period of time that a development environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`. (Default: `in 1 week`)                      | string               | N        |
| `pinned_environments`     | The list of development environments that are exempt from deletion due to expiration                                                                                                                                                                                                               | list[string]         | N        |
| `include_unmodified`      | Indicates whether to create views for all models in the target development environment or only for modified ones                                                                                                                                                                                   | boolean              | N        |
| `ignore_patterns`         | Files that match glob patterns specified in this list are ignored when scanning the project folder (Default: `[]`)                                                                                                                                                                                 | list[string]         | N        |
| `time_column_format`      | The default format to use for all model time columns. This time format uses [python format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) (Default: `%Y-%m-%d`)                                                                                        | string               | N        |
| `auto_categorize_changes` | Indicates whether SQLMesh should attempt to automatically [categorize](../concepts/plans.md#change-categories) model changes during plan creation per each model source type ([Additional Details](#auto-categorize-changes))                                                                      | dict[string, string] | N        |

## Model configuration
This section contains options that are specific to models. 

The `model_defaults` configuration is **required** and must contain a value for the `dialect` key. All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py) are allowed. 

Other values are set automatically unless explicitly overridden in the model definition.

```yaml linenums="1"
model_defaults:
    dialect: snowflake
    owner: jen
    start: 2022-01-01
```

| Option           | Description                                                                                                                                                                                                                                                                                                    |      Type      | Required |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------:|:--------:|
| `kind`           | The default model kind ([Additional Details](#kind)) (Default: `VIEW`)                                                                                                                                                                                                                                          | string or dict |    N     |
| `dialect`        | The SQL dialect in which the model's query is written                                                                                                                                                                                                                                                                |     string     |    N     |
| `cron`           | The default cron expression specifying how often the model should be refreshed                                                                                                                                                                                                                                 |     string     |    N     |
| `owner`          | The owner of a model; may be used for notification purposes                                                                                                                                                                                                                                                   |     string     |    N     |
| `start`          | The date/time that determines the earliest data interval that should be processed by a model. This value is used to identify missing data intervals during plan application and restatement. The value can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.      | string or int  |    N     |
| `batch_size`     | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. |      int       |    N     |
| `storage_format` | The storage format that should be used to store physical tables; only applicable to engines such as Spark                                                                                                                                                                                                     |     string     |    N     |

## Virtual Data Environment configuration

### Physical Schema Override
By default SQLMesh creates physical tables for a model with a naming convention of `sqlmesh__<schema>`. This can be 
overriden on a per-schema basis using the `physical_schema_override` option. This will remove the `sqlmesh__` prefix 
and just use the name you provide.

Config example:

```yaml linenums="1"
physical_schema_override:
  db: my_db
```

If you had a model name of `db.table` then the physical table would be created as `my_db.table_<fingerprint>` instead
of the default behavior of `sqlmesh__db.table_<fingerprint>`.

Keep in mind this applies to just the physical tables that SQLMesh creates. The views are still created in `db` (prod) 
or `db__<env>`. You may want to override this behavior if you have permissions/governance rules you are trying to 
adhere to at your organization and therefore need more control over the schema names used.

### View Schema Override

Coming soon. Please message us on [slack](https://tobikodata.com/slack) if you are interested in this feature so we can better understand your 
use case and make sure the new feature satisfies your needs.

## Additional details

### Auto categorize changes
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
