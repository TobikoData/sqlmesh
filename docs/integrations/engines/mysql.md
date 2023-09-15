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
