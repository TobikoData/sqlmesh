# Azure SQL

[Azure SQL](https://azure.microsoft.com/en-us/products/azure-sql) is "a family of managed, secure, and intelligent products that use the SQL Server database engine in the Azure cloud."

The Azure SQL adapter only supports authentication with a username and password. It does not support authentication with Microsoft Entra or Azure Active Directory.

## Local/Built-in Scheduler
**Engine Adapter Type**: `azuresql`

### Installation
```
pip install "sqlmesh[azuresql]"
```

### Connection options

| Option            | Description                                                      |     Type     | Required |
| ----------------- | ---------------------------------------------------------------- | :----------: | :------: |
| `type`            | Engine type name - must be `azuresql`                            |    string    |    Y     |
| `host`            | The hostname of the Azure SQL server                             |    string    |    Y     |
| `user`            | The username to use for authentication with the Azure SQL server |    string    |    N     |
| `password`        | The password to use for authentication with the Azure SQL server |    string    |    N     |
| `port`            | The port number of the Azure SQL server                          |     int      |    N     |
| `database`        | The target database                                              |    string    |    N     |
| `charset`         | The character set used for the connection                        |    string    |    N     |
| `timeout`         | The query timeout in seconds. Default: no timeout                |     int      |    N     |
| `login_timeout`   | The timeout for connection and login in seconds. Default: 60     |     int      |    N     |
| `appname`         | The application name to use for the connection                   |    string    |    N     |
| `conn_properties` | The list of connection properties                                | list[string] |    N     |
| `autocommit`      | Is autocommit mode enabled. Default: false                       |     bool     |    N     |