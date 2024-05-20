# DuckDB

## Local/Built-in Scheduler
**Engine Adapter Type**: `duckdb`

### Connection options

| Option             | Description                                                                                                                                                                                                                                     |  Type  | Required |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`             | Engine type name - must be `duckdb`                                                                                                                                                                                                             | string |    Y     |
| `database`         | The optional database name. If not specified, the in-memory database is used. Cannot be defined if using `catalogs`.                                                                                                                            | string |    N     |
| `catalogs`         | Mapping to define multiple catalogs. Can [attach DuckDB catalogs](#duckdb-catalogs-example) or [catalogs for other connections](#other-connection-catalogs-example). First entry is the default catalog. Cannot be defined if using `database`. |  dict  |    N     |
| `extensions`       | Extension to load into duckdb. Only autoloadable extensions are supported.                                                                                                                                                                      |  list  |    N     |
| `connector_config` | Configuration to pass into the duckdb connector.                                                                                                                                                                                                |  dict  |    N     |

#### DuckDB Catalogs Example

This example specifies two catalogs. The first catalog is named "persistent" and maps to the DuckDB file database `local.duckdb`. The second catalog is named "ephemeral" and maps to the DuckDB in-memory database.

`persistent` is the default catalog since it is the first entry in the dictionary. SQLMesh will place models without an explicit catalog, such as `my_schema.my_model`, into the `persistent` catalog `local.duckdb` DuckDB file database.

SQLMesh will place models with the explicit catalog "ephemeral", such as `ephemeral.other_schema.other_model`, into the `ephemeral` catalog DuckDB in-memory database.

=== "YAML"

    ```yaml linenums="1"
    gateways:
      my_gateway:
        connection:
          type: duckdb
          catalogs:
            persistent: 'local.duckdb'
            ephemeral: ':memory:'
    ```

=== "Python"

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
                connection=DuckDBConnectionConfig(
                    catalogs={
                        "persistent": "local.duckdb"
                        "ephemeral": ":memory:"
                    }
                )
            ),
        }
    )
    ```

#### Other Connection Catalogs Example

Catalogs can also be defined to connect to anything that [DuckDB can be attached to](https://duckdb.org/docs/sql/statements/attach.html).

Below are examples of connecting to a SQLite database and a PostgreSQL database. 
The SQLite database is read-write, while the PostgreSQL database is read-only.

=== "YAML"

    ```yaml linenums="1"
    gateways:
      my_gateway:
        connection:
          type: duckdb
          catalogs:
            memory: ':memory:'
            sqlite:
              type: sqlite
              path: 'test.db'
            postgres:
              type: postgres
              path: 'dbname=postgres user=postgres host=127.0.0.1'
              read_only: true
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        DuckDBConnectionConfig
    )
    from sqlmesh.core.config.connection import DuckDBAttachOptions

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=<dialect>),
        gateways={
            "my_gateway": GatewayConfig(
                connection=DuckDBConnectionConfig(
                    catalogs={
                        "memory": ":memory:",
                        "sqlite": DuckDBAttachOptions(
                            type="sqlite", 
                            path="test.db"
                        ),
                        "postgres": DuckDBAttachOptions(
                            type="postgres", 
                            path="dbname=postgres user=postgres host=127.0.0.1", 
                            read_only=True
                        ),
                    }
                )
            ),
        }
    )
    ```

##### Connectors without schemas

Some connections, like SQLite, do not support schema names and therefore objects will be attached under the default schema name of `main`.

Example: mounting a SQLite database with the name `sqlite` that has a table `example_table` will be accessible as `sqlite.main.example_table`.

##### Sensitive fields in paths

If a connector, like Postgres, requires sensitive information in the path, it might support defining environment variables instead. 
[See DuckDB Documentation for more information](https://duckdb.org/docs/extensions/postgres#configuring-via-environment-variables).

#### Cloud service authentication

DuckDB can read data directly from cloud services via extensions (e.g., [httpfs](https://duckdb.org/docs/extensions/httpfs/s3api), [azure](https://duckdb.org/docs/extensions/azure)).

Loading credentials at runtime using `load_aws_credentials()` or similar functions may fail when using SQLMesh.

Instead, create persistent and automatically used authentication credentials with the [DuckDB secrets manager](https://duckdb.org/docs/configuration/secrets_manager.html) (available in DuckDB v0.10.0 or greater).

## Airflow Scheduler
DuckDB only works when running locally; therefore it does not support Airflow.
