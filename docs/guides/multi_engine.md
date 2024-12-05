# Multi-Engine guide

Organizations typically connect to a data warehouse through a single engine to ensure data consistency. However, there are cases where the processing capabilities of one engine may be better suited to specific tasks than another.

By decoupling storage from compute and with growing support for open table formats like Apache Iceberg and Hive, different engines can now interact with the same data.

With SQLMesh's new multi-engine feature, users can leverage multiple engine adapters within a single project, offering the flexibility to choose the best engine for each task.

This feature allows you to run each model on a specified engine, provided the data catalog is shared and the engines support read/write operations on it.


## Configuring project with multiple engines

To configure a SQLMesh project with multiple engines, simply include all required gateway [connections](../reference/configuration.md#connection) in your configuration. 

Next, specify the appropriate `gateway` in the `MODEL` DDL for each model. If no gateway is explicitly defined, the default gateway will be used.

The [virtual layer](../concepts/glossary.md#virtual-layer) will be created within the engine corresponding to the default gateway.

### Example

Below is a simple example of setting up a project with connections to both DuckDB and PostgreSQL.

In this setup, the PostgreSQL engine is set as the default, so it will be used to manage views in the virtual layer. 

Meanwhile, the DuckDB's [attach](https://duckdb.org/docs/sql/statements/attach.html) feature enables read-write access to the PostgreSQL catalog's physical tables.

=== "YAML"

    ```yaml linenums="1"
    gateways:
      duckdb:
        connection:
          type: duckdb
          catalogs:
            main_db:
              type: postgres
              path: 'dbname=main_db user=postgres host=127.0.0.1'
          extensions:
            - name: iceberg
      postgres:
        connection:
          type: postgres
          database: main_db
          user: user
          password: password
          host: 127.0.0.1
          port: 5432
    default_gateway: postgres
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        ModelDefaultsConfig,
        GatewayConfig,
        DuckDBConnectionConfig,
        PostgresConnectionConfig
    )
    from sqlmesh.core.config.connection import DuckDBAttachOptions

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="postgres"),
        gateways={
            "duckdb": GatewayConfig(
                connection=DuckDBConnectionConfig(
                    catalogs={
                        "main_db": DuckDBAttachOptions(
                            type="postgres",
                            path="dbname=main_db user=postgres host=127.0.0.1"
                        ),
                    },
                    extensions=["iceberg"],
                )
            ),
            "postgres": GatewayConfig(
                connection=PostgresConnectionConfig(
                    host="127.0.0.1",
                    port=5432,
                    user="postgres",
                    password="password",
                    database="main_db",      
                )
            ),
        },
        default_gateway="postgres",
    )
    ```

Given this configuration, when a model’s gateway is set to duckdb, it will be materialized within the PostgreSQL `main_db` catalog, but it will be evaluated using DuckDB’s engine.


```sql linenums="1"
MODEL (
  name orders.order_ship_date,
  kind FULL,
  gateway duckdb,
);

SELECT
  l_orderkey, 
  l_shipdate
FROM 
  iceberg_scan('data/bucket/lineitem_iceberg', allow_moved_paths = true);
```

In this model, the DuckDB engine can be used to scan and load data from an iceberg table and create the physical table in the PostgreSQL database. 

While the PostgreSQL engine is responsible for creating the model's view for the virtual layer.
