# Configuration guide

SQLMesh's behavior is determined by three things: a project's files (e.g., models), user actions (e.g., creating a `plan`), and how SQLMesh is configured.

This page describes how SQLMesh configuration works and discusses the aspects of SQLMesh behavior that can be modified via configuration.

The [configuration reference page](../reference/configuration.md) contains concise lists of all configuration parameters and their default values.

## Configuration files

**NOTE:** SQLMesh project configurations have the following two requirements:

1. A `config.yaml` or `config.py` file must be present in the project's folder.
2. That configuration file must contain a default SQL dialect for the project's models in the [`model_defaults` `dialect` key](#models).

SQLMesh configuration parameters can be set as environment variables, in a configuration file in the `~/.sqlmesh` folder, and in the configuration file within a project folder.

The sources have the following order of precedence:

1. Environment variable (e.g., `SQLMESH__MODEL_DEFAULTS__DIALECT`). [HIGHEST PRECEDENCE]
2. `config.yaml` or `config.py` in the `~/.sqlmesh` folder.
3. `config.yaml` or `config.py` in a project folder. [LOWEST PRECEDENCE]

### File type

You can specify a SQLMesh configuration in either YAML or Python.

YAML configuration is simpler, and we recommend it for most projects. Python configuration is more complex, but it enables functionality that YAML does not support.

Because Python configuration files are evaluated by Python when SQLMesh reads them, they support dynamic parameters based on the computational environment in which SQLMesh is running.

For example, Python configuration files enable use of third-party secrets managers for storing passwords and other sensitive information. They also support user-specific parameters such as automatically setting project defaults based on which user account is running SQLMesh.

#### YAML

YAML configuration files consist of configuration keys and values. Strings are not quoted, and some keys are "dictionaries" that contain one or more sub-keys.

For example, the `default_gateway` key specifies the default gateway SQLMesh should use when executing commands. It takes a single, unquoted gateway name as its value:

```yaml linenums="1"
default_gateway: local
```

In contrast, the `gateways` key takes dictionaries as values, and each gateway dictionary contains one or more connection dictionaries. This example specifies the `my_gateway` gateway with a Snowflake `connection`:

```yaml linenums="1"
gateways:
    my_gateway:
        connection:
            type: snowflake
            user: <username>
            password: <password>
            account: <account>
```

Gateway dictionaries can contain multiple connection dictionaries if different SQLMesh components should use different connections (e.g., SQLMesh `test`s should run in a different database than SQLMesh `plan`s). See the [gateways section](#gateways) for more information on gateway configuration.

#### Python

Python configuration files consist of statements that import SQLMesh configuration classes and a configuration specification using those classes.

At minimum, a Python configuration file must:

1. Create an object of the SQLMesh `Config` class named `config`
2. Specify that object's `model_defaults` argument with a `ModelDefaultsConfig()` object specifying the default SQL dialect for the project's models

For example, this minimal configuration specifies a default SQL dialect of `duckdb` and uses the default values for all other configuration parameters:

```python linenums="1"
from sqlmesh.core.config import Config, ModelDefaultsConfig

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
```

Python configuration files may optionally define additional configuration objects and switch between the configurations when issuing `sqlmesh` commands. For example, if a configuration file contained a second configuration object `my_second_config`, you could create a plan using that config with `sqlmesh --config my_second_config plan`.

Different `Config` arguments accept different object types. Some, such as `model_defaults`, take SQLMesh configuration objects. Others, such as `default_gateway`, take strings or other Python object types like dictionaries.

SQLMesh's Python configuration components are documented in the `sqlmesh.core.config` module's [API documentation](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config.html).

The `config` sub-module API documentation describes the individual classes used for the relevant `Config` arguments:

- [Model defaults configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config/model.html): `ModelDefaultsConfig()`
- [Gateway configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config/gateway.html): `GatewayConfig()`
    - [Connection configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config/connection.html) (separate classes for each supported database/engine)
    - [Scheduler configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config/scheduler.html) (separate classes for each supported scheduler)
- [Plan change categorization configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/config/categorizer.html#CategorizerConfig): `CategorizerConfig()`
- [User configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/user.html#User): `User()`
- [Notification configuration](https://sqlmesh.readthedocs.io/en/latest/_readthedocs/html/sqlmesh/core/notification_target.html) (separate classes for each notification target)

See the [notifications guide](../guides/notifications.md) for more information about user and notification specification.

## Environment variables

All software runs within a system environment that stores information as "environment variables."

SQLMesh can access environment variables during configuration, which enables approaches like storing passwords/secrets outside the configuration file and changing configuration parameters dynamically based on which user is running SQLMesh.

You can use environment variables in two ways: specifying them in the configuration file or creating properly named variables to override configuration file values.

### Configuration file

This section demonstrates using environment variables in YAML and Python configuration files.

The examples specify a Snowflake connection whose password is stored in an environment variable `SNOWFLAKE_PW`.

=== "YAML"

    Specify environment variables in a YAML configuration with the syntax `{{ env_var('<ENVIRONMENT VARIABLE NAME>') }}`. Note that the environment variable name is contained in single quotes.

    Access the `SNOWFLAKE_PW` environment variable in a Snowflake connection configuration like this:

    ```yaml linenums="1"
    gateways:
        my_gateway:
            connection:
                type: snowflake
                user: <username>
                password: {{ env_var('SNOWFLAKE_PW') }}
                account: <account>
    ```

=== "Python"

    Python accesses environment variables via the `os` library's `environ` dictionary.

    Access the `SNOWFLAKE_PW` environment variable in a Snowflake connection configuration like this:

    ```python linenums="1"
    import os
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        SnowflakeConnectionConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user=<username>,
                    password=os.environ['SNOWFLAKE_PW'],
                    account=<account>,
                ),
            ),
        }
    )
    ```

### Overrides

Environment variables have the highest precedence among configuration methods, as [noted above](#configuration-files). They will automatically override configuration file specifications if they follow a specific naming structure.

The structure is based on the names of the configuration fields, with double underscores `__` between the field names. The environment variable name must begin with `SQLMESH__`, followed by the YAML field names starting at the root and moving downward in the hierarchy.

For example, we can override the password specified in a Snowflake connection. This is the YAML specification contained in our configuration file, which specifies a password `dummy_pw`:

```yaml linenums="1"
gateways:
    my_gateway:
        connection:
            type: snowflake
            user: <username>
            password: dummy_pw
            account: <account>
```

We can override the `dummy_pw` value with the true password `real_pw` by creating the environment variable. This example demonstrates creating the variable with the bash `export` function:

```bash
$ export SQLMESH__GATEWAYS__MY_GATEWAY__CONNECTION__PASSWORD="real_pw"
```

After the initial string `SQLMESH__`, the environment variable name components move down the key hierarchy in the YAML specification: `GATEWAYS` --> `MY_GATEWAY` --> `CONNECTION` --> `PASSWORD`.

## Configuration types

A SQLMesh project configuration is hierarchical and consists of root level parameters within which other parameters are defined.

Conceptually, we can group the root level parameters into the following types. Each type links to its table of parameters in the [SQLMesh configuration reference page](../reference/configuration.md):

1. [Project](../reference/configuration.md#projects) - configuration options for SQLMesh project directories.
2. [Environment](../reference/configuration.md#environments) - configuration options for SQLMesh environment creation/promotion, physical table schemas, and view schemas.
3. [Gateways](../reference/configuration.md#gateways) - configuration options for how SQLMesh should connect to the data warehouse, state backend, and scheduler.
4. [Gateway/connection defaults](../reference/configuration.md#gatewayconnection-defaults) - configuration options for what should happen when gateways or connections are not all explicitly specified.
5. [Model defaults](../reference/model_configuration.md) - configuration options for what should happen when model-specific configurations are not explicitly specified in a model's file.
6. [Debug mode](../reference/configuration.md#debug-mode) - configuration option for SQLMesh to print and log actions and full backtraces.

## Configuration details

The rest of this page provides additional detail for some of the configuration options and provides brief examples. Comprehensive lists of configuration options are at the [configuration reference page](../reference/configuration.md).

### Environment schemas

SQLMesh creates both physical tables and views in the data warehouse/engine.

The `physical_schema_override` and `environment_suffix_target` configuration options control where SQLMesh creates the tables and views. These options are in the [environments](../reference/configuration.md#environments) section of the configuration reference page.

#### Physical table schemas
By default, SQLMesh creates physical tables for a model with a naming convention of `sqlmesh__[model schema]`.

This can be overridden on a per-schema basis using the `physical_schema_override` option, which removes the `sqlmesh__` prefix and uses the name you provide.

This example configuration overrides the default physical schemas for the `my_schema` model schema:

=== "YAML"

    ```yaml linenums="1"
    physical_schema_override:
        my_schema: my_new_schema
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import Config, ModelDefaultsConfig

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        physical_schema_override={"my_schema":"my_new_schema"},
    )
    ```

If you had a model name of `my_schema.table`, the physical table would be created as `my_new_schema.table_<fingerprint>` instead of the default behavior of `sqlmesh__my_schema.table_<fingerprint>`.

This key only applies to the _physical tables_ that SQLMesh creates - the views are still created in `my_schema` (prod) or `my_schema__<env>`.

#### View schemas

By default, SQLMesh appends the environment name to the schema name when creating new environments. This can be changed to append a suffix at the end of table/view name instead.

Appending the suffix to a table/view means that that new environment views will be created in the same schema as production but be differentiated by having their names end with `__<env>`.

Config example:

=== "YAML"

    ```yaml linenums="1"
    environment_suffix_target: table
    ```

=== "Python"

    The Python `environment_suffix_target` argument takes an `EnvironmentSuffixTarget` enumeration with a value of `EnvironmentSuffixTarget.TABLE` or `EnvironmentSuffixTarget.SCHEMA` (default).

    ```python linenums="1"
    from sqlmesh.core.config import Config, ModelDefaultsConfig, EnvironmentSuffixTarget

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    )
    ```

If you created a `dev` environment for a project containing a model named `my_schema.users`, the model view would be created as `my_schema.users__dev` instead of the default behavior of `my_schema__dev.users`.

The default behavior of appending the suffix to schemas is recommended because it leaves production with a single clean interface for accessing the views. However, if you are deploying SQLMesh in an environment with tight restrictions on schema creation then this can be a useful way of reducing the number of schemas SQLMesh uses.

### Auto-categorize model changes

SQLMesh compares the current state of project files to an environment when `sqlmesh plan` is run. It detects changes to models, which can be classified as breaking or non-breaking.

SQLMesh can  attempt to automatically [categorize](../concepts/plans.md#change-categories) the changes it detects. The `auto_categorize_changes` option determines whether SQLMesh should attempt automatic change categorization. This option is in the [environments](../reference/configuration.md#environments) section of the configuration reference page.

Supported values:

* `full`: Never prompt the user for input, instead fall back to the most conservative category ([breaking](../concepts/plans.md#breaking-change)) if the category can't be determined automatically.
* `semi`: Prompt the user for input only if the change category can't be determined automatically.
* `off`: Always prompt the user for input; automatic categorization will not be attempted.

Example showing default values:

=== "YAML"

    ```yaml linenums="1"
    auto_categorize_changes:
        external: full
        python: off
        sql: full
        seed: full
    ```

=== "Python"

    The Python `auto_categorize_changes` argument takes `CategorizerConfig` object. That object's arguments take an `AutoCategorizationMode` enumeration with values of `AutoCategorizationMode.FULL`, `AutoCategorizationMode.SEMI`, or `AutoCategorizationMode.OFF`.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        AutoCategorizationMode,
        CategorizerConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.OFF,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL,
        ),
    )
    ```

### Gateways

The `gateways` configuration defines how SQLMesh should connect to the data warehouse, state backend, and scheduler. These options are in the [gateway](../reference/configuration.md#gateway) section of the configuration reference page.

Each gateway key represents a unique gateway name and configures its connections. For example, this configures the `my_gateway` gateway:

=== "YAML"

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

=== "Python"

    The Python `gateways` argument takes a dictionary of gateway names and `GatewayConfig` objects. A `GatewayConfig`'s connection-related arguments take an [engine-specific connection config](#engine-connection-configuration) object, and the `scheduler` argument takes a [scheduler config](#scheduler) object.


    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        ...
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                connection=...,
                state_connection=...,
                test_connection=...,
                scheduler=...,
            ),
        }
    )
    ```

Gateways do not need to specify all four components in the example above. The gateway defaults options control what happens if they are not all specified - find more information on [gateway defaults below](#gatewayconnection-defaults).

### Connections

The `connection` configuration controls the data warehouse connection. These options are in the [connection](../reference/configuration.md#connection) section of the configuration reference page.

The allowed keys include:

- The optional [`concurrent_tasks`](#concurrent-tasks) key specifies the maximum number of concurrent tasks SQLMesh will run. Default value is 4 for engines that support concurrent tasks.
- Most keys are specific to the connection engine `type` - see [below](#engine-connection-configuration). The default data warehouse connection type is an in-memory DuckDB database.

Example snowflake connection configuration:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            connection:
                type: snowflake
                user: <username>
                password: <password>
                account: <account>
    ```

=== "Python"

    A Snowflake connection is specified with a `SnowflakeConnectionConfig` object.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        SnowflakeConnectionConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user=<username>,
                    password=<password>,
                    account=<account>,
                ),
            ),
        }
    )
    ```

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

#### State connection

Configuration for the state backend connection if different from the data warehouse connection. The data warehouse connection is used if the `state_connection` key is not specified.

Example postgres state connection configuration:

=== "YAML"

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

=== "Python"

    A Postgres connection is specified with a `PostgresConnectionConfig` object.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        PostgresConnectionConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                state_connection=PostgresConnectionConfig(
                    host=<host>,
                    port=<port>,
                    user=<username>,
                    password=<password>,
                    database=<database>,
                ),
            ),
        }
    )
    ```

#### State schema name

By default, the schema name used to store state tables is `sqlmesh`. This can be changed by providing the `state_schema` config key in the gateway configuration.

Example configuration to store state information in a postgres database's `custom_name` schema:

=== "YAML"

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

=== "Python"


    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        PostgresConnectionConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                state_connection=PostgresConnectionConfig(
                    host=<host>,
                    port=<port>,
                    user=<username>,
                    password=<password>,
                    database=<database>,
                ),
                state_schema="custom_name",
            ),
        }
    )
    ```

This would create all state tables in the schema `custom_name`.

#### Test connection

Configuration for a connection used to run unit tests. An in-memory DuckDB database is used if the `test_connection` key is not specified.

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            test_connection:
                type: duckdb
    ```

=== "Python"

    A DuckDB connection is specified with a `DuckDBConnectionConfig` object. A `DuckDBConnectionConfig` with no arguments specified uses an in-memory DuckDB database.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        DuckDBConnectionConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                test_connection=DuckDBConnectionConfig(),
            ),
        }
    )
    ```

### Scheduler

Identifies which scheduler backend to use. The scheduler backend is used both for storing metadata and for executing [plans](../concepts/plans.md). By default, the scheduler type is set to `builtin`, which uses the existing SQL engine to store metadata. Use the `airflow` type integrate with Airflow.

These options are in the [scheduler](../reference/configuration.md#scheduler) section of the configuration reference page.

#### Builtin

Example configuration:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            scheduler:
                type: builtin
    ```

=== "Python"

    A built-in scheduler is specified with a `BuiltInSchedulerConfig` object.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        BuiltInSchedulerConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                scheduler=BuiltInSchedulerConfig(),
            ),
        }
    )
    ```

No additional configuration options are supported by this scheduler type.

#### Airflow

Example configuration:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            scheduler:
                type: airflow
                airflow_url: <airflow_url>
                username: <username>
                password: <password>
    ```

=== "Python"

    An Airflow scheduler is specified with an `AirflowSchedulerConfig` object.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        AirflowSchedulerConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                scheduler=AirflowSchedulerConfig(
                    airflow_url=<airflow_url>,
                    username=<username>,
                    password=<password>,
                ),
            ),
        }
    )
    ```

See [Airflow Integration Guide](../integrations/airflow.md) for information about how to integrate Airflow with SQLMesh. See the [configuration reference page](../reference/configuration.md#airflow) for a list of all parameters.

#### Cloud Composer

The Google Cloud Composer scheduler type shares the same configuration options as the `airflow` type, except for `username` and `password`. Cloud Composer relies on `gcloud` authentication, so the `username` and `password` options are not required.

Example configuration:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            scheduler:
                type: cloud_composer
                airflow_url: <airflow_url>
    ```

=== "Python"

    An Google Cloud Composer scheduler is specified with an `CloudComposerSchedulerConfig` object.

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        CloudComposerSchedulerConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                scheduler=CloudComposerSchedulerConfig(
                    airflow_url=<airflow_url>,
                ),
            ),
        }
    )
    ```

### Gateway/connection defaults

The default gateway and connection keys specify what should happen when gateways or connections are not explicitly specified. These options are in the [gateway/connection defaults](../reference/configuration.md#gatewayconnection-defaults) section of the configuration reference page.

The gateway specified in `default_gateway` is used when a `sqlmesh` command does not explicitly specify a gateway. All SQLMesh CLI commands [accept a gateway option](../reference/cli.md#cli) after `sqlmesh` and before the command name; for example, `sqlmesh --gateway my_gateway plan`. If the option is not specified in a command call, the `default_gateway` is used.

The three default connection types are used when some gateways in the `gateways` configuration dictionaries do not specify every connection type.

#### Default gateway

If a configuration contains multiple gateways, SQLMesh will use the first one in the `gateways` dictionary by default. The `default_gateway` key is used to specify a different gateway name as the SQLMesh default.

Example configuration:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        my_gateway:
            <gateway specification>
    default_gateway: my_gateway
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                <gateway specification>
            ),
        },
        default_gateway="my_gateway",
    )
    ```

#### Default connections/scheduler

The `default_connection`, `default_test_connection`, and `default_scheduler` keys are used to specify shared defaults across multiple gateways.

For example, you might have a specific connection where your tests should run regardless of which gateway is being used. Instead of duplicating the test connection information in each gateway specification, specify it once in the `default_test_connection` key.

Example configuration specifying a Postgres default connection, in-memory DuckDB default test connection, and builtin default scheduler:

=== "YAML"

    ```yaml linenums="1"
    default_connection:
        type: postgres
        host: <host>
        port: <port>
        user: <username>
        password: <password>
        database: <database>
    default_test_connection:
        type: duckdb
    default_scheduler:
        type: builtin
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        PostgresConnectionConfig,
        DuckDBConnectionConfig,
        BuiltInSchedulerConfig
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        default_connection=PostgresConnectionConfig(
            host=<host>,
            port=<port>,
            user=<username>,
            password=<password>,
            database=<database>,
        ),
        default_test_connection=DuckDBConnectionConfig(),
        default_scheduler=BuiltInSchedulerConfig(),
    )
    ```

### Models

#### Model defaults

The `model_defaults` key is **required** and must contain a value for the `dialect` key. All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py) are allowed. Other values are set automatically unless explicitly overridden in the model definition.

All supported `model_defaults` keys are listed in the [models configuration reference page](../reference/model_configuration.md#model-defaults).

Example configuration:

=== "YAML"

    ```yaml linenums="1"
    model_defaults:
        dialect: snowflake
        owner: jen
        start: 2022-01-01
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import Config, ModelDefaultsConfig

    config = Config(
        model_defaults=ModelDefaultsConfig(
            dialect="snowflake",
            owner="jen",
            start="2022-01-01",
        ),
    )
    ```

The default model kind is `VIEW` unless overridden with the `kind` key. For more information on model kinds, refer to [model concepts page](../concepts/models/model_kinds.md).

#### Model Kinds

Model kinds are required in each model file's `MODEL` DDL statement. They may optionally be used to specify a default kind in the model defaults configuration key.

All model kind specification keys are listed in the [models configuration reference page](../reference/model_configuration.md#model-kind-properties).

The `VIEW`, `FULL`, and `EMBEDDED` model kinds are specified by name only, while other models kinds require additional parameters and are provided with an array of parameters:

=== "YAML"

    `FULL` model only requires a name:

    ```sql linenums="1"
    MODEL(
        name docs_example.full_model,
        kind FULL
    );
    ```

    `INCREMENTAL_BY_TIME_RANGE` requires an array specifying the model's `time_column`:

    ```sql linenums="1"
    MODEL(
        name docs_example.incremental_model,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column ds
        )
    );
    ```

Python model kinds are specified with model kind objects. Python model kind objects have the same arguments as their SQL counterparts, listed in the [models configuration reference page](../reference/model_configuration.md#model-kind-properties).

This example demonstrates how to specify an incremental by time range model kind in Python:

=== "Python"

    ```python linenums="1"
    from sqlmesh import ExecutionContext, model
    from sqlmesh.core.model import IncrementalByTimeRangeKind

    @model(
        "docs_example.incremental_model",
        kind=IncrementalByTimeRangeKind(
            time_column="ds"
        )
    )
    ```

Learn more about specifying Python models at the [Python models concepts page](../concepts/models/python_models.md#model-specification).

### Debug mode

To enable debug mode set the `SQLMESH_DEBUG` environment variable to one of the following values: "1", "true", "t", "yes" or "y".

Enabling this mode ensures that full backtraces are printed when using CLI. The default log level is set to `DEBUG` when this mode is enabled.

Example enabling debug mode for the CLI command `sqlmesh plan`:

=== "Bash"

    ```bash
    $ SQLMESH_DEBUG=1 sqlmesh plan
    ```

=== "MS Powershell"

    ```powershell
    PS> $env:SQLMESH_DEBUG=1
    PS> sqlmesh plan
    ```

=== "MS CMD"

    ```cmd
    C:\> set SQLMESH_DEBUG=1
    C:\> sqlmesh plan
    ```