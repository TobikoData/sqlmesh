# MySQL

## Local/Built-in Scheduler
**Engine Adapter Type**: `mysql`

### Installation
```
pip install "sqlmesh[mysql]"
```

### Connection options

| Option         | Description                                                  | Type   | Required |
|----------------|--------------------------------------------------------------|:------:|:--------:|
| `type`         | Engine type name - must be `mysql`                           | string | Y        |
| `host`         | The hostname of the MysQL server                             | string | Y        |
| `user`         | The username to use for authentication with the MySQL server | string | Y        |
| `password`     | The password to use for authentication with the MySQL server | string | Y        |
| `port`         | The port number of the MySQL server                          | int    | N        |
| `charset`      | The character set used for the connection                    | string | N        |
| `ssl_disabled` | Is SSL disabled                                              | bool   | N        |

## Airflow Scheduler
**Engine Name:** `mysql`

The SQLMesh MySQL Operator is similar to the [MySQLOperator](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/index.html), and relies on the same [MySqlHook](https://airflow.apache.org/docs/apache-airflow-providers-mysql/1.0.0/_api/airflow/providers/mysql/hooks/mysql/index.html) implementation.

To enable support for this operator, the Airflow MySQL provider package should be installed on the target Airflow cluster along with SQLMesh with the mysql extra:
```
pip install "apache-airflow-providers-mysql"
pip install "sqlmesh[mysql]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target MySQL account. Refer to [MySQL connection](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/connections/mysql.html) for more details.

By default, the connection ID is set to `mysql_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
from sqlmesh.schedulers.airflow import NO_DEFAULT_CATALOG

sqlmesh_airflow = SQLMeshAirflow(
    "mysql",
    default_catalog=NO_DEFAULT_CATALOG,
    engine_operator_args={
        "mysql_conn_id": "<Connection ID>"
    },
)
```

Note: `NO_DEFAULT_CATALOG` is required for MySQL since MySQL doesn't support catalogs. 