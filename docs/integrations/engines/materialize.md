# Materialize

## Local/Built-in Scheduler
**Engine Adapter Type**: `materialize`

### Connection options

| Option            | Description                                                                     | Type   | Required |
|-------------------|---------------------------------------------------------------------------------|:------:|:--------:|
| `type`            | Engine type name - must be `materialize`                                        | string | Y        |
| `host`            | The hostname of the Materialize instance                                        | string | Y        |
| `user`            | The Materialize username                                                        | string | Y        |
| `password`        | The Materialize password                                                        | string | Y        |
| `port`            | The Materialize port number, default is `6875`                                  | int    | N        |
| `database`        | The database name                                                               | string | Y        |
| `sslmode`         | The security of the connection to the Postgres server                           | string | N        |
| `cluster`         | The cluster name to connect to                                                  | string | N        |
