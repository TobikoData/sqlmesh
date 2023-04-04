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

### BigQuery
TBD

See the [engine configuration reference](../integrations/engines.md#bigquery---localbuilt-in-scheduler) for more details.

### Databricks

A Databricks connection should be configured as follows:
```yaml linenums="1"
connections:
    my_databricks_connection:
        type: databricks
        server_hostname: [server hostname]
        access_token: [access token]
        http_headers: [optional, list of key-value pairs]
        session_configuration: [optional, key-value mapping]
        concurrent_tasks: [optional, should be greater than 0]
```

See the [engine configuration reference](../integrations/engines.md#databricks---localbuilt-in-scheduler) for more details.

### DuckDB

A DuckDB connection should be configured as follows:
```yaml linenums="1"
connections:
    my_duckdb_connection:
        type: duckdb
        database: [optional, path to the database file]
```

See the [engine configuration reference](../reference/configuration.md#duckdb) for more details.

### Redshift

A Redshift connection should be configured as follows:
```yaml linenums="1"
connections:
    my_redshift_connection:
        type: redshift
        user: [optional, username]
        password: [optional, password]
        database: [optional, database]
        host: [optional, hostname]
        port: [optional, port]
        ssl: [optional, boolean flag which determines whether SSL is enabled]
        sslmode: [optional, the security of the connection to the Amazon Redshift cluster]
        timeout: [optional, connection timeout]
        tcp_keepalive: [optional, boolean flag which determines whether to use TCP Keepalives]
        application_name: [optional, the application name]
        preferred_role: [optional, the IAM role]
        principal_arn: [optional, the ARN for the IAM entity (user or role)]
        credentials_provider: [optional, the class name of the IdP that will be used for authentication]
        region: [optional, the AWS region]
        cluster_identifier: [optional, the cluster identifier]
        iam: [optional, boolean flag which determines whether the IAM authentication should be used]
        is_serverless: [optional, whether the Redshift endpoint is serverless or provisional]
        serverless_acct_id: [optional, serverless account ID]
        serverless_work_group: [optional, serverless work group]
        concurrent_tasks: [optional, should be greater than 0]
```

See the [engine configuration reference](../integrations/engines.md#redshift---localbuilt-in-scheduler) for more details.

### Snowflake

A Snowflake connection should be configured as follows:
```yaml linenums="1"
connections:
    my_snowflake_connection:
        type: snowflake
        user: [required, username]
        password: [required if using password]
        authenticator: [required if using externalbrowser]
        account: [required, account ID]
        warehouse: [optional, warehouse name]
        database: [optional, database name]
        role: [optional, user role]
        concurrent_tasks: [optional, should be greater than 0]
```

See the [engine configuration reference](../integrations/engines.md#snowflake---localbuilt-in-scheduler) for more details.
