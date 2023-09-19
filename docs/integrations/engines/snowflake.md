# Snowflake

## Local/Built-in Scheduler
**Engine Adapter Type**: `snowflake`

### Installation
```
pip install "sqlmesh[snowflake]"
```

### Connection options

| Option          | Description                            |  Type  | Required |
|-----------------|----------------------------------------|:------:|:--------:|
| `type`          | Engine type name - must be `snowflake` | string |    Y     |
| `user`          | The Snowflake username                 | string |    N     |
| `password`      | The Snowflake password                 | string |    N     |
| `authenticator` | The Snowflake authenticator method     | string |    N     |
| `account`       | The Snowflake account name             | string |    Y     |
| `warehouse`     | The Snowflake warehouse name           | string |    N     |
| `database`      | The Snowflake database name            | string |    N     |
| `role`          | The Snowflake role name                | string |    N     |
| `private_key`   | Local path to the private key file     | string |    N     |

### Snowflake SSO Authorization

SQLMesh supports Snowflake SSO authorization connections using the `externalbrowser` authenticator method. For example:

```yaml
gateways:
    snowflake:
        connection:
            type: snowflake
            account: ************
            user: ************
            authenticator: externalbrowser
            warehouse: ************
            database: ************
            role: ************
```

### Snowflake Private Key Authorization

SQLMesh supports Snowflake private key authorization connections by providing the private key bytes. Only config.py is supported when using private key authorization. `account` and `user` are required. For example:

```python
from sqlmesh.core.config import (
    Config,
    GatewayConfig,
    ModelDefaultsConfig,
    SnowflakeConnectionConfig,
)

from cryptography.hazmat.primitives import serialization

key = """-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----""".encode()

p_key= serialization.load_pem_private_key(key, password=None)

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="snowflake"),
    gateways={
       "my_gateway": GatewayConfig(
            connection=SnowflakeConnectionConfig(
                user="user",
                account="account",
                private_key=pkb,
            ),
        ),
    }
)
```

The authenticator method is assumed to be `snowflake_jwt` when `private_key` is provided, but it can also be explicitly provided in the connection configuration.

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
