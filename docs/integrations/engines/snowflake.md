# Snowflake

## Local/Built-in Scheduler
**Engine Adapter Type**: `snowflake`

### Installation
```
pip install "sqlmesh[snowflake]"
```

### Connection options

| Option                   | Description                                                                                                                                                                    |  Type  | Required |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                   | Engine type name - must be `snowflake`                                                                                                                                         | string |    Y     |
| `user`                   | The Snowflake username                                                                                                                                                         | string |    N     |
| `password`               | The Snowflake password                                                                                                                                                         | string |    N     |
| `authenticator`          | The Snowflake authenticator method                                                                                                                                             | string |    N     |
| `account`                | The Snowflake account name                                                                                                                                                     | string |    Y     |
| `warehouse`              | The Snowflake warehouse name                                                                                                                                                   | string |    N     |
| `database`               | The Snowflake database name                                                                                                                                                    | string |    N     |
| `role`                   | The Snowflake role name                                                                                                                                                        | string |    N     |
| `token`                  | The Snowflake OAuth 2.0 access token                                                                                                                                           | string |    N     |
| `private_key`            | The optional private key to use for authentication. Key can be Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or bytes (Python config only). | string |    N     |
| `private_key_path`       | The optional path to the private key to use for authentication. This would be used instead of `private_key`.                                                                   | string |    N     |
| `private_key_passphrase` | The optional passphrase to use to decrypt `private_key` (if in PEM format) or `private_key_path`. Keys can be created without encryption so only provide this if needed.       | string |    N     |


#### Lowercase object names

Snowflake object names are case-insensitive by default. If you have intentionally created an object with a case-sensitive lowercase name, specify it with outer single and inner double quotes.

For example, a connection to the database `"my_db"` would include:

``` yaml
connection:
    type: snowflake
    <other connection options>
    database: '"my_db"'
```

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

### Snowflake OAuth Authorization

SQLMesh supports Snowflake OAuth authorization connections using the `oauth` authenticator method. For example:

=== "YAML"

    ```yaml linenums="1"
    gateways:
        snowflake:
            connection:
                type: snowflake
                account: account
                user: user
                authenticator: oauth
                token: eyJhbGciOiJSUzI1NiIsImtpZCI6ImFmZmM...
    ```

=== "Python"

    ```python linenums="1"
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="snowflake"),
        gateways={
           "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user="user",
                    account="account",
                    authenticator="oauth",
                    token="eyJhbGciOiJSUzI1NiIsImtpZCI6ImFmZmM...",
                ),
            ),
        }
    )
    ```

### Snowflake Private Key Authorization

SQLMesh supports Snowflake private key authorization connections by providing the private key as a path, Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or as bytes (Python Only). `account` and `user` are required. For example:

#### Private Key Path

Note: `private_key_passphrase` is only needed if the key was encrypted with a passphrase.

=== "YAML"

    ```yaml linenums="1"
    gateways:
        snowflake:
            connection:
                type: snowflake
                account: account
                user: user
                private_key_path: '/path/to/key.key'
                private_key_passphrase: supersecret
    ```

=== "Python"

    ```python linenums="1"
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="snowflake"),
        gateways={
           "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user="user",
                    account="account",
                    private_key_path="/path/to/key.key",
                    private_key_passphrase="supersecret",
                ),
            ),
        }
    )
    ```


#### Private Key PEM

Note: `private_key_passphrase` is only needed if the key was encrypted with a passphrase.

=== "YAML"

    ```yaml linenums="1"
    gateways:
        snowflake:
            connection:
                type: snowflake
                account: account
                user: user
                private_key: |
                    -----BEGIN PRIVATE KEY-----
                    ...
                    -----END PRIVATE KEY-----
                private_key_passphrase: supersecret
    ```

=== "Python"

    ```python linenums="1"
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="snowflake"),
        gateways={
           "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user="user",
                    account="account",
                    private_key="""
                    -----BEGIN PRIVATE KEY-----
                    ...
                    -----END PRIVATE KEY-----""",
                    private_key_passphrase="supersecret",
                ),
            ),
        }
    )
    ```


#### Private Key Base64 

Note: This is base64 encoding of the bytes of the key itself and not the PEM file contents.

=== "YAML"

    ```yaml linenums="1"
    gateways:
        snowflake:
            connection:
                type: snowflake
                account: account
                user: user
                private_key: 'MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCvMKgsYzoDMnl7QW9nWTzAMMQToyUTslgKlH9MezcEYUvvCv+hYEsY9YGQ5dhI5MSY1vkQ+Wtqc6KsvJQzMaHDA1W+Z5R/yA/IY+Mp2KqJijQxnp8XjZs1t6Unr0ssL2yBjlk2pNOZX3w4A6B6iwpkqUi/HtqI5t2M15FrUMF3rNcH68XMcDa1gAasGuBpzJtBM0bp4/cHa18xWZZfu3d2d+4CCfYUvE3OYXQXMjJunidnU56NZtYlJcKT8Fmlw16fSFsPAG01JOIWBLJmSMi5qhhB2w90AAq5URuupCbwBKB6KvwzPRWn+fZKGAvvlR7P3CGebwBJEJxnq85MljzRAgMBAAECggEAKXaTpwXJGi6dD+35xvUY6sff8GHhiZrhOYfR5TEYYWIBzc7Fl9UpkPuyMbAkk4QJf78JbdoKcURzEP0E+mTZy0UDyy/Ktr+L9LqnbiUIn8rk9YV8U9/BB2KypQTY/tkuji85sDQsnJU72ioJlldIG3DxdcKAqHwznXz7vvF7CK6rcsz37hC5w7MTtguvtzNyHGkvJ1ZBTHI1vvGR/VQJoSSFkv6nLFs2xl197kuM2x+Ss539Xbg7GGXX90/sgJP+QLyNk6kYezekRt5iCK6n3UxNfEqd0GX03AJ1oVtFM9SLx0RMHiLuXVCKlQLJ1LYf8zOT31yOun6hhowNmHvpLQKBgQDzXGQqBLvVNi9gQzQhG6oWXxdtoBILnGnd8DFsb0YZIe4PbiyoFb8b4tJuGz4GVfugeZYL07I8TsQbPKFH3tqFbx69hENMUOo06PZ4H7phucKk8Er/JHW8dhkVQVg1ttTK8J5kOm+uKjirqN5OkLlUNSSJMblaEr9AHGPmTu21MwKBgQC4SeYzJDvq/RTQk5d7AwVEokgFk95aeyv77edFAhnrD3cPIAQnPlfVyG7RgPA94HrSAQ5Hr0PL2hiQ7OxX1HfP+66FMcTVbZwktYULZuj4NMxJqwxKbCmmzzACiPF0sibg8efGMY9sAmcQRw5JRS2s6FQns1MqeksnjzyMf3196wKBgFf8zJ5AjeT9rU1hnuRliy6BfQf+uueFyuUaZdQtuyt1EAx2KiEvk6QycyCqKtfBmLOhojVued/CHrc2SZ2hnmJmFbgxrN9X1gYBQLOXzRxuPEjENGlhNkxIarM7p/frva4OJ0ZXtm9DBrBR4uaG/urKOAZ+euRtKMa2PQxU9y7vAoGAeZWX4MnZFjIe13VojWnywdNnPPbPzlZRMIdG+8plGyY64Km408NX492271XoKoq9vWug5j6FtiqP5p3JWDD/UyKzg4DQYhdM2xM/UcR1k7wRw9Cr7TXrTPiIrkN3OgyHhgVTavkrrJDxOlYG4ORZPCiTzRWMmwvQJatkwTUjsD0CgYEA8nAWBSis9H8n9aCEW30pGHT8LwqlH0XfXwOTPmkxHXOIIkhNFiZRAzc4NKaefyhzdNlc7diSMFVXpyLZ4K0l5dY1Ou2xRh0W+xkRjjKsMib/s9g/crtam+tXddADJDokLELn5PAMhaHBpti+PpOMGqdI3Wub+5yT1XCXT9aj6yU='
    ```

=== "Python"

    ```python linenums="1"
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="snowflake"),
        gateways={
           "my_gateway": GatewayConfig(
                connection=SnowflakeConnectionConfig(
                    user="user",
                    account="account",
                    private_key="MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCvMKgsYzoDMnl7QW9nWTzAMMQToyUTslgKlH9MezcEYUvvCv+hYEsY9YGQ5dhI5MSY1vkQ+Wtqc6KsvJQzMaHDA1W+Z5R/yA/IY+Mp2KqJijQxnp8XjZs1t6Unr0ssL2yBjlk2pNOZX3w4A6B6iwpkqUi/HtqI5t2M15FrUMF3rNcH68XMcDa1gAasGuBpzJtBM0bp4/cHa18xWZZfu3d2d+4CCfYUvE3OYXQXMjJunidnU56NZtYlJcKT8Fmlw16fSFsPAG01JOIWBLJmSMi5qhhB2w90AAq5URuupCbwBKB6KvwzPRWn+fZKGAvvlR7P3CGebwBJEJxnq85MljzRAgMBAAECggEAKXaTpwXJGi6dD+35xvUY6sff8GHhiZrhOYfR5TEYYWIBzc7Fl9UpkPuyMbAkk4QJf78JbdoKcURzEP0E+mTZy0UDyy/Ktr+L9LqnbiUIn8rk9YV8U9/BB2KypQTY/tkuji85sDQsnJU72ioJlldIG3DxdcKAqHwznXz7vvF7CK6rcsz37hC5w7MTtguvtzNyHGkvJ1ZBTHI1vvGR/VQJoSSFkv6nLFs2xl197kuM2x+Ss539Xbg7GGXX90/sgJP+QLyNk6kYezekRt5iCK6n3UxNfEqd0GX03AJ1oVtFM9SLx0RMHiLuXVCKlQLJ1LYf8zOT31yOun6hhowNmHvpLQKBgQDzXGQqBLvVNi9gQzQhG6oWXxdtoBILnGnd8DFsb0YZIe4PbiyoFb8b4tJuGz4GVfugeZYL07I8TsQbPKFH3tqFbx69hENMUOo06PZ4H7phucKk8Er/JHW8dhkVQVg1ttTK8J5kOm+uKjirqN5OkLlUNSSJMblaEr9AHGPmTu21MwKBgQC4SeYzJDvq/RTQk5d7AwVEokgFk95aeyv77edFAhnrD3cPIAQnPlfVyG7RgPA94HrSAQ5Hr0PL2hiQ7OxX1HfP+66FMcTVbZwktYULZuj4NMxJqwxKbCmmzzACiPF0sibg8efGMY9sAmcQRw5JRS2s6FQns1MqeksnjzyMf3196wKBgFf8zJ5AjeT9rU1hnuRliy6BfQf+uueFyuUaZdQtuyt1EAx2KiEvk6QycyCqKtfBmLOhojVued/CHrc2SZ2hnmJmFbgxrN9X1gYBQLOXzRxuPEjENGlhNkxIarM7p/frva4OJ0ZXtm9DBrBR4uaG/urKOAZ+euRtKMa2PQxU9y7vAoGAeZWX4MnZFjIe13VojWnywdNnPPbPzlZRMIdG+8plGyY64Km408NX492271XoKoq9vWug5j6FtiqP5p3JWDD/UyKzg4DQYhdM2xM/UcR1k7wRw9Cr7TXrTPiIrkN3OgyHhgVTavkrrJDxOlYG4ORZPCiTzRWMmwvQJatkwTUjsD0CgYEA8nAWBSis9H8n9aCEW30pGHT8LwqlH0XfXwOTPmkxHXOIIkhNFiZRAzc4NKaefyhzdNlc7diSMFVXpyLZ4K0l5dY1Ou2xRh0W+xkRjjKsMib/s9g/crtam+tXddADJDokLELn5PAMhaHBpti+PpOMGqdI3Wub+5yT1XCXT9aj6yU=",
                ),
            ),
        }
    )
    ```

#### Private Key Bytes

=== "YAML"

    Base64 encode the bytes and follow [Private Key Base64](#private-key-base64) instructions.

=== "Python"

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
    default_catalog="<database name>",
    engine_operator_args={
        "snowflake_conn_id": "<Connection ID>"
    },
)
```

## Configuring Virtual Warehouses

The Snowflake Virtual Warehouse can be specified on a per-model basis using the `session_properties` attribute of the model definition:
```sql
MODEL (
  name model_name,
  session_properties (
    'warehouse' = TEST_WAREHOUSE,
  ),
);
```
