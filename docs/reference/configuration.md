# Configuration

This page lists the available SQLMesh configurations that can be set either as environment variables in the `config.yaml` file within a project folder, or in the file with the same name in the `~/.sqlmesh` folder.

Configuration options from different sources have the following order of precedence:

1. Set as an environment variable (for example, `SQLMESH__MODEL_DEFAULTS__DIALECT`).
2. Set in `config.yaml` in a project folder.
3. Set in `config.yaml` in the `~/.sqlmesh` folder.

## connections

A dictionary of supported connections and their configurations. The key represents a unique connection name. If there is only one connection, its configuration can be provided directly, omitting the dictionary.

```yaml linenums="1"
connections:
    my_connection:
        type: snowflake
        user: <username>
        password: <password>
        account: <account>
```

All connection configurations share the `concurrent_tasks` setting, which determines the maximum number of tasks that will be run by SQLMesh concurrently when using this connection.

Below is the list of configuration options specific to each corresponding connection type.

### duckdb
#### database

The optional database name. If not specified, the in-memory database is used.

**Type:** string

**Default:** `None`

### snowflake
#### user

The Snowflake username.

**Type:** string

#### password

The Snowflake password.

**Type:** string

#### account

The Snowflake account name.

**Type:** string

#### warehouse

The optional Snowflake warehouse name.

**Type:** string

**Default:** `None`

#### database

The optional Snowflake database name.

**Type:** string

**Default:** `None`

#### role

The optional Snowflake role name.

**Type:** string

**Default:** `None`

### databricks
#### server_hostname

Databricks instance host name.

**Type:** string

#### http_path

HTTP path, either to a DBSQL endpoint (such as `/sql/1.0/endpoints/1234567890abcdef`) or to a DBR interactive cluster (such as `/sql/protocolv1/o/1234567890123456/1234-123456-slid123`).

**Type:** string

#### access_token

HTTP Bearer access token, such as Databricks Personal Access Token.

**Type:** string

#### http_headers

An optional dictionary of HTTP headers that will be set on every request.

**Type:** dictionary

**Default:** `None`

#### session_configuration

An optional dictionary of Spark session parameters.

**Type:** dictionary

**Default:** `None`

### bigquery

TBD

## redshift
### user
The username to use for authentication with the Amazon Redshift cluster.

**Type:** string

**Default:** `None`

### password
The password to use for authentication with the Amazon Redshift cluster.

**Type:** string

**Default:** `None`

### database
The name of the database instance to connect to.

**Type:** string

**Default:** `None`

### host
The hostname of the Amazon Redshift cluster.

**Type:** string

**Default:** `None`

### port
The port number of the Amazon Redshift cluster.

**Type:** int

**Default:** `5439`

### ssl
Is SSL enabled. SSL must be enabled when authenticating using IAM.

**Type:** bool

**Default:** `True`

### sslmode
The security of the connection to the Amazon Redshift cluster. `verify-ca` and `verify-full` are supported.

**Type:** string

**Default:** `None`

### timeout
The number of seconds before the connection to the server will timeout.

**Type:** int

**Default:** `None`

### tcp_keepalive
Is [TCP keepalive](https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive) used.

**Type:** bool

**Default:** `True`

### application_name
The application name.

**Type:** string

**Default:** `None`

### preferred_role
The IAM role preferred for the current connection.

**Type:** string

**Default:** `None`

### principal_arn
The ARN of the IAM entity (user or role) for which you are generating a policy.

**Type:** string

**Default:** `None`

### credentials_provider
The class name of the IdP that will be used for authenticating with the Amazon Redshift cluster.

**Type:** string

**Default:** `None`

### region
The AWS region where the Amazon Redshift cluster is located.

**Type:** string

**Default:** `None`

### cluster_identifier
The cluster identifier of the Amazon Redshift cluster.

**Type:** string

**Default:** `None`

### iam
If IAM authentication is enabled. IAM must be True when authenticating using an IdP.

**Type:** bool

**Default:** `False`

### is_serverless
Whether Redshift endpoint is serverless or provisional.

**Type:** bool

**Default:** `False`

### serverless_acct_id
The account ID of the serverless.

**Type:** string

**Default:** `None`

### serverless_work_group
The name of work group for serverless end point.

**Type:** string

**Default:** `None`

## default_connection

The name of a connection to use by default.

**Type:** string

**Default:** A connection defined first in the `connections` option.

## test_connection

The name of a connection to use when running tests.

**Type:** string

**Default:** A DuckDB connection that creates an in-memory database.

## scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md). By default, the scheduler type is set to `builtin`, which uses the existing SQL engine to store metadata, and that has a simple scheduler. The `airflow` type should be set if you want to integrate with Airflow.

```yaml linenums="1"
scheduler:
    type: builtin
```

Below is the list of configuration options specific to each corresponding scheduler type.

### builtin

No additional configuration options are supported by this scheduler type.

### airflow
#### airflow_url

The URL of the Airflow Webserver.

**Type:** string

#### username

The Airflow username.

**Type:** string

#### password

The Airflow password.

**Type:** string

#### dag_run_poll_interval_secs

Determines how often a running DAG can be polled (in seconds).

**Type:** int

**Default:** `10`

#### dag_creation_poll_interval_secs

Determines how often SQLMesh should check whehter a DAG has been created (in seconds).

**Type:** int

**Default:** `30`

#### dag_creation_max_retry_attempts

Determines the maximum number of attempts that SQLMesh will make while checking for whether a DAG has been created.

**Type:** int

**Default:** `10`

#### backfill_concurrent_tasks

The number of concurrent tasks used for model backfilling during plan application.

**Type:** int

**Default:** `4`

#### ddl_concurrent_tasks

The number of concurrent tasks used for DDL operations like table/view creation, deletion, and so forth.

**Type:** int

**Default:** `4`

### cloud_composer

This scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`.

## physical_schema

The default schema used to store physical tables for models.

**Type:** string

**Default:** `sqlmesh`

## snapshot_ttl

The period of time that a model snapshot not a part of any environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`.

**Type:** string

**Default:** `'in 1 week'`

## environment_ttl

The period of time that a development environment should exist before being deleted. This is defined as a string with the default `in 1 week`. Other [relative dates](https://dateparser.readthedocs.io/en/latest/) can be used, such as `in 30 days`.

**Type:** string

**Default:** `'in 1 week'`

## ignore_patterns

Files that match glob patterns specified in this list are ingored when scanning the project folder.

**Type:** list of strings

**Default:** `[]`

## time_column_format

The default format to use for all model time columns.

This time format uses [python format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes). 

**Type:** string

**Default:** `%Y-%m-%d`

## auto_categorize_changes

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

## model_defaults

This section contains options that are specific to models, and that are set automatically unless explicitly overriden in the model definition.

```yaml linenums="1"
model_defaults:
    dialect: snowflake
    owner: jen
    start: 2022-01-01
```

### kind
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

**Type:** string or object

**Default:** `None`

### dialect

The SQL dialect the model's query is written in.

**Type:** string

**Default:** `None`

### cron

The cron expression specifying how often the model should be refreshed.

**Type:** string

**Default:** `None`

### owner

The owner of a model. May be used for notification purposes.

**Type:** string

**Default:** `None`

### start

The date/time that determines the earliest data interval that should be processed by a model. This value is used to identify missing data intervals during plan application and restatement. The value can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.

**Type:** string or int

**Default:** `None`

### batch_size

The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals.

**Type:** int

**Default:** `None`

### storage_format

The storage format that should be used to store physical tables. Only applicable to engines such as Spark.

**Type:** string

**Default:** `None`
