# MSSQL

## Local/Built-in Scheduler
**Engine Adapter Type**: `mssql`

### Installation
#### User / Password Authentication:
```
pip install "sqlmesh[mssql]"
```
#### Microsoft Entra ID / Azure Active Directory Authentication:
```
pip install "sqlmesh[mssql-odbc]"
```

### Connection options

| Option            | Description                                                  |     Type     | Required |
| ----------------- | ------------------------------------------------------------ | :----------: | :------: |
| `type`            | Engine type name - must be `mssql`                           |    string    |    Y     |
| `host`            | The hostname of the MSSQL server                             |    string    |    Y     |
| `user`            | The username / client id to use for authentication with the MSSQL server |    string    |    N     |
| `password`        | The password / client secret to use for authentication with the MSSQL server |    string    |    N     |
| `port`            | The port number of the MSSQL server                          |     int      |    N     |
| `database`        | The target database                                          |    string    |    N     |
| `charset`         | The character set used for the connection                    |    string    |    N     |
| `timeout`         | The query timeout in seconds. Default: no timeout            |     int      |    N     |
| `login_timeout`   | The timeout for connection and login in seconds. Default: 60 |     int      |    N     |
| `appname`         | The application name to use for the connection               |    string    |    N     |
| `conn_properties` | The list of connection properties                            | list[string] |    N     |
| `autocommit`      | Is autocommit mode enabled. Default: false                   |     bool     |    N     |
| `driver`         | The driver to use for the connection. Default: pymssql            |    string    |    N     |
| `driver_name`     | The driver name to use for the connection. E.g., *ODBC Driver 18 for SQL Server* |    string    |    N     |
| `odbc_properties` | The dict of ODBC connection properties. E.g., authentication: ActiveDirectoryServicePrincipal. See more [here](https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver16). | dict |    N     |