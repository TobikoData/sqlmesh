# Snowflake

This page provides information about how to use SQLMesh with the Snowflake SQL engine.

It begins with a [Connection Quickstart](#connection-quickstart) that demonstrates how to connect to Snowflake, or you can skip directly to information about using Snowflake with the [built-in](#localbuilt-in-scheduler) or [airflow](#airflow-scheduler) schedulers.

## Connection quickstart

Connecting to cloud warehouses involves a few steps, so this connection quickstart provides the info you need to get up and running with Snowflake.

It demonstrates connecting to Snowflake with the `snowflake-connector-python` library bundled with SQLMesh.

Snowflake provides multiple methods of authorizing a connection (e.g., password, SSO, etc.). This quickstart demonstrates authorizing with a password, but configurations for other methods are [described below](#snowflake-authorization-methods).

!!! tip
    This quickstart assumes you are familiar with basic SQLMesh commands and functionality.

    If you're not, work through the [SQLMesh Quickstart](../../quick_start.md) before continuing!

### Prerequisites

Before working through this connection quickstart, ensure that:

1. You have a Snowflake account and know your username and password
2. Your Snowflake account has at least one [warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview) available for running computations
3. Your computer has [SQLMesh installed](../../installation.md) with the [Snowflake extra available](../../installation.md#install-extras)
    - Install from the command line with the command `pip install "sqlmesh[snowflake]"`
4. You have initialized a [SQLMesh example project](../../quickstart/cli#1-create-the-sqlmesh-project) on your computer
    - Open a command line interface and navigate to the directory where the project files should go
    - Initialize the project with the command `sqlmesh init snowflake`

### Access control permissions

SQLMesh must have sufficient permissions to create and access different types of database objects.

SQLMesh's core functionality requires relatively broad permissions, including:

1. Ability to create and delete schemas in a database
2. Ability to create, modify, delete, and query tables and views in the schemas it creates

If your project uses materialized views or dynamic tables, SQLMesh will also need permissions to create, modify, delete, and query those object types.

We now describe how to grant SQLMesh appropriate permissions.

#### Snowflake roles

Snowflake allows you to grant permissions directly to a user, or you can create and assign permissions to a "role" that you then grant to the user.

Roles provide a convenient way to bundle sets of permissions and provide them to multiple users. We create and use a role to grant our user permissions in this quickstart.

The role must be granted `USAGE` on a warehouse so it can execute computations. We describe other permissions below.

#### Database permissions
The top-level object container in Snowflake is a "database" (often called a "catalog" in other engines). SQLMesh does not need permission to create databases; it may use an existing one.

The simplest way to grant SQLMesh sufficient permissions for a database is to give it `OWNERSHIP` of the database, which includes all the necessary permissions.

Alternatively, you may grant SQLMesh granular permissions for all the actions and objects it will work with in the database.

#### Granting the permissions

This section provides example code for creating a `sqlmesh` role, granting it sufficient permissions, and granting it to a user.

The code must be executed by a user with `USERADMIN` level permissions or higher. We provide two versions of the code, one that grants database `OWNERSHIP` to the role and another that does not.

Both examples create a role named `sqlmesh`, grant it usage of the warehouse `compute_wh`, create a database named `demo_db`, and assign the role to the user `demo_user`. The step that creates the database can be omitted if the database already exists.

=== "With database ownership"

    ```sql linenums="1"
    USE ROLE useradmin; -- This code requires USERADMIN privileges or higher

    CREATE ROLE sqlmesh; -- Create role for permissions
    GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sqlmesh; -- Can use warehouse

    CREATE DATABASE demo_db; -- Create database for SQLMesh to use (omit if database already exists)
    GRANT OWNERSHIP ON DATABASE demo_db TO ROLE sqlmesh; -- Role owns database

    GRANT ROLE sqlmesh TO USER demo_user; -- Grant role to user
    ALTER USER demo_user SET DEFAULT ROLE = sqlmesh; -- Make role user's default role
    ```

=== "Without database ownership"

    ```sql linenums="1"
    USE ROLE useradmin; -- This code requires USERADMIN privileges or higher

    CREATE ROLE sqlmesh; -- Create role for permissions
    CREATE DATABASE demo_db; -- Create database for SQLMesh to use (omit if database already exists)

    GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sqlmesh; -- Can use warehouse
    GRANT USAGE ON DATABASE demo_db TO ROLE sqlmesh; -- Can use database

    GRANT CREATE SCHEMA ON DATABASE demo_db TO ROLE sqlmesh; -- Can create SCHEMAs in database
    GRANT USAGE ON FUTURE SCHEMAS IN DATABASE demo_db TO ROLE sqlmesh; -- Can use schemas it creates
    GRANT CREATE TABLE ON FUTURE SCHEMAS IN DATABASE demo_db TO ROLE sqlmesh; -- Can create TABLEs in schemas
    GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE demo_db TO ROLE sqlmesh; -- Can create VIEWs in schemas
    GRANT SELECT, INSERT, TRUNCATE, UPDATE, DELETE ON FUTURE TABLES IN DATABASE demo_db TO ROLE sqlmesh; -- Can SELECT and modify TABLEs in schemas
    GRANT REFERENCES, SELECT ON FUTURE VIEWS IN DATABASE demo_db TO ROLE sqlmesh; -- Can SELECT and modify VIEWs in schemas

    GRANT ROLE sqlmesh TO USER demo_user; -- Grant role to user
    ALTER USER demo_user SET DEFAULT ROLE = sqlmesh; -- Make role user's default role
    ```

### Get connection info

Now that our user has sufficient access permissions, we're ready to gather the information needed to configure the SQLMesh connection.

#### Account name

Snowflake connection configurations require the `account` parameter that identifies the Snowflake account SQLMesh should connect to.

Snowflake account identifiers have two components: your organization name and your account name. Both are embedded in your Snowflake web interface URL, separated by a `/`.

This shows the default view when you log in to your Snowflake account, where we can see the two components of the account identifier:

![Snowflake account info in web URL](./snowflake/snowflake_db-guide_account-url.png){ loading=lazy }

In this example, our organization name is `idapznw`, and our account name is `wq29399`.

We concatenate the two components, separated by a `-`, for the SQLMesh `account` parameter: `idapznw-wq29399`.

#### Warehouse name

Your Snowflake account may have more than one warehouse available - any will work for this quickstart, which runs very few computations.

Some Snowflake user accounts may have a default warehouse they automatically use when connecting.

The connection configuration's `warehouse` parameter is not required, but we recommend specifying the warehouse explicitly in the configuration to ensure SQLMesh's behavior doesn't change if the user's default warehouse changes.

#### Database name

Snowflake user accounts may have a "Default Namespace" that includes a default database they automatically use when connecting.

The connection configuration's `database` parameter is not required, but we recommend specifying the database explicitly in the configuration to ensure SQLMesh's behavior doesn't change if the user's default namespace changes.

### Configure the connection

We now have the information we need to configure SQLMesh's connection to Snowflake.

We start the configuration by adding a gateway named `snowflake` to our example project's config.yaml file and making it our `default_gateway`:

```yaml linenums="1" hl_lines="2-6"
gateways:
  snowflake:
    connection:
      type: snowflake

default_gateway: snowflake

model_defaults:
  dialect: snowflake
  start: 2024-07-24
```

And we specify the `account`, `user`, `password`, `database`, and `warehouse` connection parameters using the information from above:

```yaml linenums="1" hl_lines="5-9"
gateways:
  snowflake:
    connection:
      type: snowflake
      account: idapznw-wq29399
      user: DEMO_USER
      password: << password here >>
      database: DEMO_DB
      warehouse: COMPUTE_WH

default_gateway: snowflake

model_defaults:
  dialect: snowflake
  start: 2024-07-24
```

!!! warning
    Best practice for storing secrets like passwords is placing them in [environment variables that the configuration file loads dynamically](../../guides/configuration.md#environment-variables). For simplicity, this guide instead places the value directly in the configuration file.

    This code demonstrates how to use the environment variable `SNOWFLAKE_PASSWORD` for the configuration's `password` parameter:

    ```yaml linenums="1" hl_lines="5"
    gateways:
      snowflake:
        connection:
          type: snowflake
          password: {{ env_var('SNOWFLAKE_PASSWORD') }}
    ```

### Check connection

We have now specified the `snowflake` gateway connection information, so we can confirm that SQLMesh is able to successfully connect to Snowflake. We will test the connection with the `sqlmesh info` command.

First, open a command line terminal. Now enter the command `sqlmesh info`:

![Run sqlmesh info command in CLI](./snowflake/snowflake_db-guide_sqlmesh-info.png){ loading=lazy }

The output shows that our data warehouse connection succeeded:

![Successful data warehouse connection](./snowflake/snowflake_db-guide_sqlmesh-info-succeeded.png){ loading=lazy }

However, the output includes a `WARNING` about using the Snowflake SQL engine for storing SQLMesh state:

![Snowflake state connection warning](./snowflake/snowflake_db-guide_sqlmesh-info-warning.png){ loading=lazy }

!!! warning
    Snowflake is not designed for transactional workloads and should not be used to store SQLMesh state even in testing deployments.

    Learn more about storing SQLMesh state [here](../../guides/configuration.md#state-connection).

### Specify state connection

We can store SQLMesh state in a different SQL engine by specifying a `state_connection` in our `snowflake` gateway.

This example uses the DuckDB engine to store state in the local `snowflake_state.db` file:

```yaml linenums="1" hl_lines="10-12"
gateways:
  snowflake:
    connection:
      type: snowflake
      account: idapznw-wq29399
      user: DEMO_USER
      password: << your password here >>
      database: DEMO_DB
      warehouse: COMPUTE_WH
    state_connection:
      type: duckdb
      database: snowflake_state.db

default_gateway: snowflake

model_defaults:
  dialect: snowflake
  start: 2024-07-24
```

Now we no longer see the warning when running `sqlmesh info`, and we see a new entry `State backend connection succeeded`:

![No state connection warning](./snowflake/snowflake_db-guide_sqlmesh-info-no-warning.png){ loading=lazy }

### Run a `sqlmesh plan`

Now we're ready to run a `sqlmesh plan` in Snowflake:

![Run sqlmesh plan in snowflake](./snowflake/snowflake_db-guide_sqlmesh-plan.png){ loading=lazy }

And confirm that our schemas and objects exist in the Snowflake catalog:

![Sqlmesh plan objects in snowflake](./snowflake/snowflake_db-guide_sqlmesh-plan-objects.png){ loading=lazy }

Congratulations - your SQLMesh project is up and running on Snowflake!

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
| `account`                | The Snowflake account name                                                                                                                                                     | string |    Y     |
| `user`                   | The Snowflake username                                                                                                                                                         | string |    N     |
| `password`               | The Snowflake password                                                                                                                                                         | string |    N     |
| `authenticator`          | The Snowflake authenticator method                                                                                                                                             | string |    N     |
| `warehouse`              | The Snowflake warehouse name                                                                                                                                                   | string |    N     |
| `database`               | The Snowflake database name                                                                                                                                                    | string |    N     |
| `role`                   | The Snowflake role name                                                                                                                                                        | string |    N     |
| `token`                  | The Snowflake OAuth 2.0 access token                                                                                                                                           | string |    N     |
| `private_key`            | The optional private key to use for authentication. Key can be Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or bytes (Python config only). | string |    N     |
| `private_key_path`       | The optional path to the private key to use for authentication. This would be used instead of `private_key`.                                                                   | string |    N     |
| `private_key_passphrase` | The optional passphrase to use to decrypt `private_key` (if in PEM format) or `private_key_path`. Keys can be created without encryption so only provide this if needed.       | string |    N     |
| `session_parameters`     | The optional session parameters to set for the connection.                                                                                                                     | dict   |    N     |


### Lowercase object names

Snowflake object names are case-insensitive by default, and Snowflake automatically normalizes them to uppercase. For example, the command `CREATE SCHEMA sqlmesh` will generate a schema named `SQLMESH` in Snowflake.

If you need to create an object with a case-sensitive lowercase name, the name must be double-quoted in SQL code. In the SQLMesh configuration file, it also requires outer single quotes.

For example, a connection to the database `"my_db"` would include:

``` yaml
connection:
  type: snowflake
  <other connection options>
  database: '"my_db"' # outer single and inner double quotes
```

### Snowflake authorization methods

The simplest (but arguably least secure) method of authorizing a connection with Snowflake is with a username and password.

This section describes how to configure other authorization methods.

#### Snowflake SSO Authorization

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

#### Snowflake OAuth Authorization

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

#### Snowflake Private Key Authorization

SQLMesh supports Snowflake private key authorization connections by providing the private key as a path, Base64-encoded DER format (representing the key bytes), a plain-text PEM format, or as bytes (Python Only).

The `account` and `user` parameters are required for each of these methods.

__Private Key Path__

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


__Private Key PEM__

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


__Private Key Base64__

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

__Private Key Bytes__

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

The Snowflake Virtual Warehouse a model should use can be specified in the `session_properties` attribute of the model definition:

```sql linenums="1"
MODEL (
  name schema_name.model_name,
  session_properties (
    'warehouse' = TEST_WAREHOUSE,
  ),
);
```
 
## Custom View and Table types

SQLMesh supports custom view and table types for Snowflake models. You can apply these modifiers to either the physical layer or virtual layer of a model using the `physical_properties` and `virtual_properties` attributes respectively. For example:

### Secure Views

A table can be exposed through a `SECURE` view in the virtual layer by specifying the `creatable_type` property and setting it to `SECURE`:

```sql linenums="1"
Model (
  name = schema_name.model_name,
  virtual_properties (
      creatable_type = SECURE
  )
);

SELECT a FROM schema_name.model_b;
```

### Transient Tables

A model can use a `TRANSIENT` table in the physical layer by specifying the `creatable_type` property and setting it to `TRANSIENT`:

```sql linenums="1"
Model (
  name = schema_name.model_name,
  physical_properties (
      creatable_type = TRANSIENT
  )
);

SELECT a FROM schema_name.model_b;
```

## Troubleshooting

### Frequent Authentication Prompts

When using Snowflake with security features like Multi-Factor Authentication (MFA), you may experience repeated prompts for authentication while running SQLMesh commands. This typically occurs when your Snowflake account isn't configured to issue short-lived tokens.

To reduce authentication prompts, you can enable token caching in your Snowflake connection configuration:

- For general authentication, see [Connection Caching Documentation](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use#using-connection-caching-to-minimize-the-number-of-prompts-for-authentication-optional)
- For MFA specifically, see [MFA Token Caching Documentation](https://docs.snowflake.com/en/user-guide/security-mfa#using-mfa-token-caching-to-minimize-the-number-of-prompts-during-authentication-optional).
