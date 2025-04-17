# Multi-Engine guide

Organizations typically connect to a data warehouse through a single engine to ensure data consistency. However, there are cases where the processing capabilities of one engine may be better suited to specific tasks than another.

Across the industry, companies are increasingly decoupling storage from compute, demanding interoperability across platforms and tools, focusing on cost efficiency and a growing support for open table formats like Apache Iceberg and Hive.

In SQLMesh, you can use multiple engine adapters within a single project, giving you the flexibility to choose the most suitable engine for each task. This allows individual models to run on a specified engine based on their specific requirements.

## Configuring a Project with Multiple Engines

Configuring your project to use multiple engines follows a simple process:

- Include all required [gateway connections](../reference/configuration.md#connection) in your configuration.
- Specify the `gateway` to be used for execution in the `MODEL` DDL.

If no gateway is explicitly defined for a model, the [default_gateway](../reference/configuration.md#default-gateway) of the project is used.

By default, the `default_gateway` is also responsible to create the views of the virtual layer. This assumes that all engines can read from and write to the same shared catalog.

Alternatively, you can configure the model-specific gateway to create the views of the virtual layer by setting [gateway_managed_virtual_layer](#gateway-managed-virtual-layer) flag in your configuration to true.

### Shared Virtual Layer

To dive deeper, in SQLMesh the [physical layer](../concepts/glossary.md#physical-layer) is the concrete data storage layer, where it stores and manages data in database tables and materialized views.

While, the [virtual layer](../concepts/glossary.md#virtual-layer) consists of views, one for each model, each pointing to a snapshot table in the physical layer.

In a multi-engine project with a shared data catalog, the model-specific gateway is responsible for the physical layer, while the default gateway is used for managing the virtual layer.

#### Example: DuckDB + PostgreSQL

Below is a simple example of setting up a project with connections to both DuckDB and PostgreSQL.

In this setup, the PostgreSQL engine is set as the default, so it will be used to manage views in the virtual layer. Meanwhile, the DuckDB's [attach](https://duckdb.org/docs/sql/statements/attach.html) feature enables read-write access to the PostgreSQL catalog's physical tables.

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

In the `order_ship_date` model, the DuckDB engine is set, which will be used to create the physical table in the PostgreSQL database.

This allows you to efficiently scan data from an Iceberg table, or even query tables directly from S3 when used with the [HTTPFS](https://duckdb.org/docs/stable/extensions/httpfs/overview.html) extension.

![PostgreSQL + DuckDB](./multi_engine/postgres_duckdb.png)

In models where no gateway is specified, such as the `customer_orders` model, the default PostgreSQL engine will be used to create the physical table as well as to create and manage the views of the virtual layer.

### Gateway-Managed Virtual Layer

For projects where the engines don’t share a catalog or your raw data is located in different warehouses, you may prefer each gateway to manage its own virtual layer. This ensures isolation and each model’s views being created by its respective gateway.

To enable this, set `gateway_managed_virtual_layer` to `true` in your configuration. By default, this flag is set to false.

#### Example: Redshift + Athena + Snowflake

Consider a scenario where you need to create a project with models in Redshift, Athena and Snowflake. To set this you, add the connections to your configuration and set the `gateway_managed_virtual_layer` flag:

=== "YAML"

```yaml linenums="1"
gateways:
  redshift:
    connection:
      type: redshift
      user: <redshift_user>
      password: <redshift_password>
      host: <redshift_host>
      database: <redshift_database>
    variables:
      gw_var: 'redshift'
  athena:
    connection:
      type: athena
      aws_access_key_id: <athena_aws_access_key_id>
      aws_secret_access_key: <athena_aws_secret_access_key>
      s3_warehouse_location: <athena_s3_warehouse_location>
    variables:
      gw_var: 'athena'
  snowflake:
    connection:
      type: snowflake
      account: <snowflake_account>
      user: <snowflake_user>
      database: <snowflake_database>
      warehouse: <snowflake_warehouse>
    variables:
      gw_var: 'snowflake'

default_gateway: redshift
gateway_managed_virtual_layer: true

variables:
  gw_var: 'global'
  global_var: 5
```

=== "Python"

```python linenums="1"
from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    RedshiftConnectionConfig,
    AthenaConnectionConfig,
    SnowflakeConnectionConfig,
)

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="redshift"),
    gateways={
        "redshift": GatewayConfig(
            connection=RedshiftConnectionConfig(
                user="<redshift_user>",
                password="<redshift_password>",
                host="<redshift_host>",
                database="<redshift_database>",
            ),
            variables={
                "gw_var": "redshift"
            },
        ),
        "athena": GatewayConfig(
            connection=AthenaConnectionConfig(
                aws_access_key_id="<athena_aws_access_key_id>",
                aws_secret_access_key="<athena_aws_secret_access_key>",
                region_name="<athena_region_name>",
                s3_warehouse_location="<athena_s3_warehouse_location>",
            ),
            variables={
                "gw_var": "athena"
            },
        ),
        "snowflake": GatewayConfig(
            connection=SnowflakeConnectionConfig(
                account="<snowflake_account>",
                user="<snowflake_user>",
                database="<snowflake_database>",
                warehouse="<snowflake_warehouse>",
            ),
            variables={
                "gw_var": "snowflake"
            },
        ),
    },
    default_gateway="redshift",
    gateway_managed_virtual_layer=True,
    variables={
        "gw_var": "global",
        "global_var": 5,
    },
)
```

Note that gateway-specific variables take precedence over global ones. In the example above, the `gw_var` used in a model will take the value defined for the respective gateway.

For further customization, you can also enable [gateway-specific model defaults](../guides/configuration.md#gateway-specific-model-defaults). This allows you to define custom behaviors, such as specifying a dialect with case-insensitivity normalization.

```sql linenums="1"
MODEL (
  name redshift_schema.order_dates,
  table_format iceberg,
);

SELECT
  order_date,
  order_id
FROM
  bucket.raw_data;
```

In this setup, since the default gateway is set to redshift, omitting the gateway from a model will default to this, as seen in the `order_dates` model above.

```sql linenums="1"
MODEL (
  name athena_schema.order_status,
  table_format iceberg,
  gateway athena,
);

SELECT
  order_id,
  status
FROM
  bucket.raw_data;
```

While in the case of the `athena_schema.order_status` model above, the gateway is specified to athena explicitly.

```sql linenums="1"
MODEL (
  name snowflake_schema.customer_orders,
  table_format iceberg,
  gateway snowflake
);

SELECT
  customer_id,
  orders
FROM
  bronze_schema.customer_data;
```

Finally, specifying the snowflake gateway for the `customer_orders` model ensures it is isolated from the rest and sources from a table within the snowflake database.

![Athena + Redshift + Snowflake](./multi_engine/athena_redshift_snowflake.png)

When you run the plan, the catalogs for each model will be set automatically based on the gateway’s connection and each corresponding model will be evaluated against the specified engine.

```bash
❯ sqlmesh plan

`prod` environment will be initialized

Models:
└── Added:
    ├── awsdatacatalog.athena_schema.order_status
    ├── redshift_schema.order_dates
    └── silver.snowflake_schema.customers
Models needing backfill:
├── awsdatacatalog.athena_schema.order_status: [full refresh]
├── redshift_schema.order_dates: [full refresh]
└── silver.snowflake_schema.customers: [full refresh]
Apply - Backfill Tables [y/n]: y
```

The views of the virtual layer will also be created by each corresponding engine.

This approach provides isolation between your models, while maintaining centralized control over your project.

This allows users to leverage multiple engines within a single SQLMesh project, particularly as the industry shifts toward data lakes, open table formats, and greater interoperability.