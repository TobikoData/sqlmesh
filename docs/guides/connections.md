# Connections guide

## Overview

**Note:** The following guide only applies when using the built-in scheduler. Connections are configured differently when using an external scheduler such as Airflow. See the [Scheduling guide](scheduling.md) for more details.

In order to deploy models and to apply changes to them, you must configure a connection to the Data Warehouse. This can be done in either the `config.yaml` file in your project folder, or the one in `~/.sqlmesh`.

Each configured connection has a unique name associated with it, which can be used to select a specific connection when using the CLI. For example:
```yaml linenums="1"
connections:
    local_db:
        type: duckdb
```

Now the defined connection can be specified in the `sqlmesh plan` CLI command as follows:
```bash
sqlmesh --connection local_db plan
```

## Default connection
If no connection name is provided, then the first connection in the `config.yaml` connections specification will be used.

Additionally, you can set a default connection by specifying the connection name in the `default_connection` key:
```yaml linenums="1"
default_connection: local_db
```

## Test connection
By default, when running [tests](../concepts/tests.md), SQLMesh uses an in-memory DuckDB database connection. You can override this behavior by specifying a connection name in the `test_connection` key:
```yaml linenums="1"
test_connection: local_db
```
Or, you can specify the test connection in the `sqlmesh plan` CLI command:
```bash
sqlmesh --test-connection local_db plan
```

## Supported engines

* [BigQuery](../integrations/engines.md#bigquery---localbuilt-in-scheduler)
* [Databricks](../integrations/engines.md#databricks---localbuilt-in-scheduler)
* [Redshift](../integrations/engines.md#redshift---localbuilt-in-scheduler)
* [Snowflake](../integrations/engines.md#snowflake---localbuilt-in-scheduler)
* [Spark](../integrations/engines.md#spark---localbuilt-in-scheduler)
