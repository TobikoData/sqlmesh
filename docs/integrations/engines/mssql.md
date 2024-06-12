# MSSQL

## Local/Built-in Scheduler
**Engine Adapter Type**: `mssql`

### Installation
```
pip install "sqlmesh[mssql]"
```

### Connection options

| Option            | Description                                                  |     Type     | Required |
| ----------------- | ------------------------------------------------------------ | :----------: | :------: |
| `type`            | Engine type name - must be `mssql`                           |    string    |    Y     |
| `host`            | The hostname of the MSSQL server                             |    string    |    Y     |
| `user`            | The username to use for authentication with the MSSQL server |    string    |    N     |
| `password`        | The password to use for authentication with the MSSQL server |    string    |    N     |
| `port`            | The port number of the MSSQL server                          |     int      |    N     |
| `database`        | The target database                                          |    string    |    N     |
| `charset`         | The character set used for the connection                    |    string    |    N     |
| `timeout`         | The query timeout in seconds. Default: no timeout            |     int      |    N     |
| `login_timeout`   | The timeout for connection and login in seconds. Default: 60 |     int      |    N     |
| `appname`         | The application name to use for the connection               |    string    |    N     |
| `conn_properties` | The list of connection properties                            | list[string] |    N     |
| `autocommit`      | Is autocommit mode enabled. Default: false                   |     bool     |    N     |

## Airflow Scheduler
**Engine Name:** `mssql`

The SQLMesh MsSql Operator is similar to the [MsSqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/_api/airflow/providers/microsoft/mssql/operators/mssql/index.html), and relies on the same [MsSqlHook](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/_api/airflow/providers/microsoft/mssql/hooks/mssql/index.html) implementation.

To enable support for this operator, the Airflow Microsoft MSSQL provider package should be installed on the target Airflow cluster along with SQLMesh with the mssql extra:
```
pip install "apache-airflow-providers-microsoft-mssql"
pip install "sqlmesh[mssql]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target MSSQL account. Refer to [MSSQL connection](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/connections/mssql.html) for more details.

By default, the connection ID is set to `mssql_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "mssql",
    default_catalog="<database name>",
    engine_operator_args={
        "mssql_conn_id": "<Connection ID>"
    },
)
```