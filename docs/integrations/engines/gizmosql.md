# GizmoSQL

This page provides information about how to use SQLMesh with the [GizmoSQL](https://github.com/gizmodata/gizmosql) database server.

!!! info
    The GizmoSQL engine adapter is a community contribution. Due to this, only limited community support is available.

## Overview

GizmoSQL is a database server that uses [DuckDB](./duckdb.md) as its execution engine and exposes an [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) interface for remote connections. This allows you to connect to a GizmoSQL server from anywhere on your network while still benefiting from DuckDB's fast analytical query processing.

The SQLMesh GizmoSQL adapter uses [ADBC (Arrow Database Connectivity)](https://arrow.apache.org/docs/format/ADBC.html) with the Flight SQL driver to communicate with GizmoSQL servers. Data is transferred using the efficient Apache Arrow columnar format.

!!! note
    This adapter only supports the DuckDB backend for GizmoSQL. Attempting to connect to a GizmoSQL server running a different backend will result in an error.

## Local/Built-in Scheduler

**Engine Adapter Type**: `gizmosql`

### Installation

```
pip install "sqlmesh[gizmosql]"
```

This will install the required dependencies:

- `adbc-driver-flightsql` - The ADBC driver for Arrow Flight SQL
- `pyarrow` - Apache Arrow Python bindings

## Connection options

| Option                             | Description                                                                   | Type    | Required |
|------------------------------------|-------------------------------------------------------------------------------|:-------:|:--------:|
| `type`                             | Engine type name - must be `gizmosql`                                         | string  | Y        |
| `host`                             | The hostname of the GizmoSQL server                                           | string  | N        |
| `port`                             | The port number of the GizmoSQL server (default: `31337`)                     | int     | N        |
| `username`                         | The username for authentication with the GizmoSQL server                      | string  | Y        |
| `password`                         | The password for authentication with the GizmoSQL server                      | string  | Y        |
| `use_encryption`                   | Whether to use TLS encryption for the connection (default: `true`)            | bool    | N        |
| `disable_certificate_verification`| Skip TLS certificate verification - useful for self-signed certs (default: `false`) | bool    | N        |
| `database`                         | The default database/catalog to use                                           | string  | N        |

### Example configuration

=== "YAML"

    ```yaml linenums="1"
    gateways:
      gizmosql:
        connection:
          type: gizmosql
          host: gizmosql.example.com
          port: 31337
          username: my_user
          password: my_password
          use_encryption: true
          disable_certificate_verification: false
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import (
        Config,
        GatewayConfig,
        ModelDefaultsConfig,
    )
    from sqlmesh.core.config.connection import GizmoSQLConnectionConfig

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        gateways={
            "gizmosql": GatewayConfig(
                connection=GizmoSQLConnectionConfig(
                    host="gizmosql.example.com",
                    port=31337,
                    username="my_user",
                    password="my_password",
                    use_encryption=True,
                    disable_certificate_verification=False,
                ),
            ),
        },
    )
    ```

## SQL Dialect

GizmoSQL uses the DuckDB SQL dialect. When writing models for GizmoSQL, set your model dialect to `duckdb`:

```yaml
model_defaults:
  dialect: duckdb
```

Or specify the dialect in individual model definitions:

```sql
MODEL (
    name my_schema.my_model,
    dialect duckdb
);

SELECT * FROM my_table;
```

## Docker Setup

For local development and testing, you can run GizmoSQL using Docker:

```bash
docker run -d \
  --name gizmosql \
  -p 31337:31337 \
  -e GIZMOSQL_USERNAME=gizmosql_user \
  -e GIZMOSQL_PASSWORD=gizmosql_password \
  -e TLS_ENABLED=1 \
  gizmodata/gizmosql:latest
```

Then connect with:

```yaml
gateways:
  gizmosql:
    connection:
      type: gizmosql
      host: localhost
      port: 31337
      username: gizmosql_user
      password: gizmosql_password
      use_encryption: true
      disable_certificate_verification: true  # For self-signed certs
```

## Related Integrations

GizmoSQL has adapters available for other popular data tools:

- [Ibis GizmoSQL](https://pypi.org/project/ibis-gizmosql/) - Ibis backend for GizmoSQL
- [dbt-gizmosql](https://pypi.org/search/?q=dbt-gizmosql) - dbt adapter for GizmoSQL
- [SQLFrame GizmoSQL](https://github.com/gizmodata/sqlframe) - SQLFrame (PySpark-like API) support for GizmoSQL
