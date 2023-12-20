# Postgres

## Local/Built-in Scheduler
**Engine Adapter Type**: `postgres`

### Installation
```
pip install "sqlmesh[postgres]"
```

### Connection options

| Option            | Description                                                                     | Type   | Required |
|-------------------|---------------------------------------------------------------------------------|:------:|:--------:|
| `type`            | Engine type name - must be `postgres`                                           | string | Y        |
| `host`            | The hostname of the Postgres server                                             | string | Y        |
| `user`            | The username to use for authentication with the Postgres server                 | string | Y        |
| `password`        | The password to use for authentication with the Postgres server                 | string | Y        |
| `port`            | The port number of the Postgres server                                          | int    | Y        |
| `database`        | The name of the database instance to connect to                                 | string | Y        |
| `keepalives_idle` | The number of seconds between each keepalive packet sent to the server.         | int    | N        |
| `connect_timeout` | The number of seconds to wait for the connection to the server. (Default: `10`) | int    | N        |
| `role`            | The role to use for authentication with the Postgres server                     | string | N        |
| `sslmode`         | The security of the connection to the Postgres server                           | string | N        |

## Airflow Scheduler
**Engine Name:** `postgres`

The SQLMesh Postgres Operator is similar to the [PostgresOperator](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/operators/postgres/index.html), and relies on the same [PostgresHook](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html) implementation.

To enable support for this operator, the Airflow Postgres provider package should be installed on the target Airflow cluster along with SQLMesh with the Postgres extra:
```
pip install "apache-airflow-providers-postgres"
pip install "sqlmesh[postgres]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Postgres account. Refer to [Postgres connection](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html) for more details.

By default, the connection ID is set to `postgres_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "postgres",
    default_catalog="<database name>",
    engine_operator_args={
        "postgres_conn_id": "<Connection ID>"
    },
)
```