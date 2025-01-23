# RisingWave

This page provides information about how to use SQLMesh with the RisingWave streaming database engine.

## Connection options

| Option         | Description                                                  | Type   | Required |
|----------------|--------------------------------------------------------------|:------:|:--------:|
| `type`         | Engine type name - must be `risingwave`                           | string | Y        |
| `host`         | The hostname of the RisingWave engine                            | string | Y        |
| `user`         | The username to use for authentication with the RisingWave engine | string | Y        |
| `password`     | The password to use for authentication with the RisingWave engine | string | N        |
| `port`         | The port number of the RisingWave engine server                          | int    | Y        |
| `database`        | The name of the database instance to connect to                                 | string | Y        |
| `keepalives_idle` | The number of seconds between each keepalive packet sent to the server.         | int    | N        |
| `connect_timeout` | The number of seconds to wait for the connection to the server. (Default: `10`) | int    | N        |
| `role`            | The role to use for authentication with the Postgres server                     | string | N        |
| `sslmode`         | The security of the connection to the Postgres server                           | string | N        |

## Usage
RisingWave engine has some different features as streaming database. You can create a resource that RisingWave can read data from with `CREATE SOURCE`. You can also create an external target where you can send data processed in RisingWave with `CREATE SINK`.

To use this in SQLMesh, you can refer to optional pre-statements and post-statements as [SQL models doc](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/) here specify.