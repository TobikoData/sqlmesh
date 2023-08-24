# Connections guide

## Overview

**Note:** The following guide only applies when using the built-in scheduler. Connections are configured differently when using an external scheduler such as Airflow. See the [Scheduling guide](scheduling.md) for more details.

In order to deploy models and to apply changes to them, you must configure a connection to your Data Warehouse and, optionally, connection to the database where the SQLMesh state is stored. This can be done in either the `config.yaml` file in your project folder, or the one in `~/.sqlmesh`.

Each connection is configured as part of a gateway which has a unique name associated with it. The gateway name can be used to select a specific combination of connection settings  when using the CLI. For example:

```yaml linenums="1"
gateways:
    local_db:
        connection:
            type: duckdb
```

Now the defined connection can be selected in the `sqlmesh plan` CLI command as follows:

```bash
sqlmesh --gateway local_db plan
```

## State connection

By default, the data warehouse connection is also used to store the SQLMesh state. This behavior can be changed by providing different connection settings in the `state_connection` key of the gateway configuration:

```yaml linenums="1"
gateways:
    local_db:
        state_connection:
            type: duckdb
            database: state.db
```

## Default connection

Additionally, you can set a default connection by defining its configuration in the `default_connection` key:

```yaml linenums="1"
default_connection:
    type: duckdb
    database: local.db
```

This connection configuration will be used if one is not provided in the target gateway.

## Test connection

By default, when running [tests](../concepts/tests.md), SQLMesh uses an in-memory DuckDB database connection. You can override this behavior by providing connection settings in the `test_connection` key of the gateway configuration:

```yaml linenums="1"
gateways:
    local_db:
        test_connection:
            type: duckdb
            database: test.db
```

### Default test connection

To configure a default test connection for all gateways use the `default_test_connection` key:

```yaml linenums="1"
default_test_connection:
    type: duckdb
    database: test.db
```

## Default gateway

To change the default gateway used by the CLI when no gateway name is provided, set the desired name in the `default_gateway` key:

```yaml linenums="1"
default_gateway: local_db
```

## Supported engines

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
