# MSSQL

## Local/Built-in Scheduler
**Engine Adapter Type**: `mssql`

### Installation
```
pip install "sqlmesh[mssql]"
```

### Connection options

| Option            | Description                                                  | Type     | Required |
|-------------------|--------------------------------------------------------------|:--------:|:--------:|
| `type`            | Engine type name - must be `mssql`                           | string   | Y        |
| `host`            | The hostname of the MSSQL server                             | string   | Y        |
| `user`            | The username to use for authentication with the MSSQL server | string   | Y        |
| `password`        | The password to use for authentication with the MSSQL server | string   | Y        |
| `port`            | The port number of the MSSQL server                          | int      | N        |
| `database`        | The target database                                          | string   | N        |
| `charset`         | The character set used for the connection                    | string   | N        |
| `timeout`         | The query timeout in seconds. Default: no timeout            | int      | N        |
| `login_timeout`   | The timeout for connection and login in seconds. Default: 60 | int      | N        |
| `appname`         | The application name to use for the connection               | string   | N        |
| `conn_properties` | The list of connection properties                            | [string] | N        |
| `autocommit`      | Is autocommit mode enabled. Default: false                   | bool     | N        |
