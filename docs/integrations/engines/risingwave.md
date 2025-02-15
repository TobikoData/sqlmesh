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
| `role`            | The role to use for authentication with the Postgres server                     | string | N        |
| `sslmode`         | The security of the connection to the Postgres server                           | string | N        |

## Usage
RisingWave engine has some different features as streaming database. You can create a resource that RisingWave can read data from with `CREATE SOURCE`. You can also create an external target where you can send data processed in RisingWave with `CREATE SINK`.

To use this in SQLMesh, you can refer to optional pre-statements and post-statements as [SQL models doc](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/) here specify.

Below is an example of creating sink in SQLMesh models as post-statement.

```sql
MODEL (
    name sqlmesh_example.view_model,
    kind VIEW (
      materialized true
    )
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders,
FROM
  sqlmesh_example.incremental_model
GROUP BY item_id;

CREATE
  SINK IF NOT EXISTS kafka_sink
FROM
  @this_model
WITH (
  connector='kafka',
  "properties.bootstrap.server"='localhost:9092',
  topic='test1',
)
FORMAT PLAIN
ENCODE JSON (force_append_only=true);
```

here `@this_model` macro is used to represent "sqlmesh_example.view_model" model.
