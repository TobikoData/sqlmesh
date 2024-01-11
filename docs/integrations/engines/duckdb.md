# DuckDB

## Local/Built-in Scheduler
**Engine Adapter Type**: `duckdb`

### Connection options

| Option     | Description                                                                                                                                                      |  Type  | Required |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`             | Engine type name - must be `duckdb`                                                                                                                      | string |    Y     |
| `database`         | The optional database name. If not specified, the in-memory database is used. Cannot be defined if using `catalogs`.                                     | string |    N     |
| `catalogs`         | Mapping to define multiple catalogs. Name is the alias and value is the path. First entry is the default catalog. Cannot be defined if using `database`. |  dict  |    N     |
| `extensions`       | Extension to load into duckdb. Only autoloadable extensions are supported.                                                                               |  list  |    N     |
| `connector_config` | Configuration to pass into the duckdb connector.                                                                                                         |  dict  |    N     |

#### Catalogs Example

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

## Airflow Scheduler
DuckDB only works when running locally; therefore it does not support Airflow.
