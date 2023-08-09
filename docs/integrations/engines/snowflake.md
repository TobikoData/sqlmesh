# Snowflake

## Local/Built-in Scheduler
**Engine Adapter Type**: `snowflake`

### Installation
```
pip install "sqlmesh[snowflake]"
```

### Connection options

| Option          | Description                        |  Type  | Required |
|-----------------|------------------------------------|:------:|:--------:|
| `user`          | The Snowflake username             | string |    N     |
| `password`      | The Snowflake password             | string |    N     |
| `authenticator` | The Snowflake authenticator method | string |    N     |
| `account`       | The Snowflake account name         | string |    Y     |
| `warehouse`     | The Snowflake warehouse name       | string |    N     |
| `database`      | The Snowflake database name        | string |    N     |
| `role`          | The Snowflake role name            | string |    N     |

### Snowflake SSO Authorization

SQLMesh supports Snowflake SSO authorization connections using the `externalbrowser` authenticator method. For example:

```yaml
gateways:
    snowflake:
        connection:
            type: snowflake
            user: ************
            authenticator: externalbrowser
            account: ************
            warehouse: ************
            database: ************
            role: ************
```

## Airflow Scheduler
**Engine Name:** `snowflake`

The SQLMesh Snowflake Operator is similar to the [SnowflakeOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html), and relies on the same [SnowflakeHook](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html) implementation.

To enable support for this operator, the Airflow Snowflake provider package should be installed on the target Airflow cluster along with SQLMesh with the Snowflake extra:
```
pip install "apache-airflow-providers-snowflake[common.sql]"
pip install "sqlmesh[snowflake]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Snowflake account. Refer to [Snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) for more details.

By default, the connection ID is set to `snowflake_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "snowflake",
    engine_operator_args={
        "snowflake_conn_id": "<Connection ID>"
    },
)
```