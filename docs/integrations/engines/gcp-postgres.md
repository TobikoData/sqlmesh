# GCP Postgres

## Local/Built-in Scheduler
**Engine Adapter Type**: `gcp_postgres`

### Installation
```
pip install "sqlmesh[gcppostgres]"
```

### Connection options

| Option                       | Description                                                                                            |    Type    | Required |
|------------------------------|--------------------------------------------------------------------------------------------------------|:----------:|:--------:|
| `type`                       | Engine type name - must be `gcp_postgres`                                                              |   string   |    Y     |
| `instance_connection_string` | Connection name for the postgres instance                                                              |   string   |    Y     |
| `user`                       | The username (postgres or IAM) to use for authentication                                               |   string   |    Y     |
| `password`                   | The password to use for authentication. Required when connecting as a Postgres user                    |   string   |    N     |
| `enable_iam_auth`            | Enables IAM authentication. Required when connecting as an IAM user                                    |  boolean   |    N     |
| `keyfile`                    | Path to the keyfile to be used with enable_iam_auth instead of ADC                                     |   string   |    N     |
| `keyfile_json`               | Keyfile information provided inline (not recommended)                                                  |    dict    |    N     |
| `db`                         | The name of the database instance to connect to                                                        |   string   |    Y     |
| `ip_type`                    | The IP type to use for the connection. Must be one of `public`, `private`, or `psc`. Default: `public` |   string   |    N     |
| `timeout`                    | The connection timeout in seconds. Default: `30`                                                       |  integer   |    N     |
| `scopes`                     | The scopes to use for the connection. Default: `(https://www.googleapis.com/auth/sqlservice.admin,)`   | tuple[str] |    N     |
| `driver`                     | The driver to use for the connection. Default: `pg8000`. Note: only `pg8000` is tested                 |   string   |    N     |
