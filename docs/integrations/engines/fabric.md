# Fabric

## Local/Built-in Scheduler
**Engine Adapter Type**: `fabric`

NOTE: Fabric Warehouse is not recommended to be used for the SQLMesh [state connection](../../reference/configuration.md#connections).

### Installation
#### Microsoft Entra ID / Azure Active Directory Authentication:
```
pip install "sqlmesh[fabric]"
```

### Connection options

| Option            | Description                                                  |     Type     | Required |
| ----------------- | ------------------------------------------------------------ | :----------: | :------: |
| `type`            | Engine type name - must be `fabric`                           |    string    |    Y     |
| `host`            | The hostname of the Fabric Warehouse server                             |    string    |    Y     |
| `user`            | The client id to use for authentication with the Fabric Warehouse server |    string    |    N     |
| `password`        | The client secret to use for authentication with the Fabric Warehouse server |    string    |    N     |
| `port`            | The port number of the Fabric Warehouse server                          |     int      |    N     |
| `database`        | The target database                                          |    string    |    N     |
| `charset`         | The character set used for the connection                    |    string    |    N     |
| `timeout`         | The query timeout in seconds. Default: no timeout            |     int      |    N     |
| `login_timeout`   | The timeout for connection and login in seconds. Default: 60 |     int      |    N     |
| `appname`         | The application name to use for the connection               |    string    |    N     |
| `conn_properties` | The list of connection properties                            | list[string] |    N     |
| `autocommit`      | Is autocommit mode enabled. Default: false                   |     bool     |    N     |
| `driver`          | The driver to use for the connection. Default: pyodbc            |    string    |    N     |
| `driver_name`     | The driver name to use for the connection. E.g., *ODBC Driver 18 for SQL Server* |    string    |    N     |
| `tenant`          | The Fabric tenant UUID                             |    string    |    Y     |
| `workspace`       | The Fabric workspace UUID                             |    string    |    Y     |
| `odbc_properties` | The dict of ODBC connection properties. E.g., authentication: ActiveDirectoryServicePrincipal. See more [here](https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver16). | dict |    N     |
