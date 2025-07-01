# RisingWave

This page provides information about how to use SQLMesh with the [RisingWave](https://risingwave.com/) streaming database engine.

!!! info
    The RisingWave engine adapter is a community contribution. Due to this, only limited community support is available.

## Local/Built-in Scheduler

**Engine Adapter Type**: `risingwave`

### Installation

```
pip install "sqlmesh[risingwave]"
```

## Connection options

RisingWave is based on Postgres and uses the same `psycopg2` connection library. Therefore, the connection parameters are very similar to [Postgres](./postgres.md).

| Option         | Description                                                       | Type   | Required |
|----------------|-------------------------------------------------------------------|:------:|:--------:|
| `type`         | Engine type name - must be `risingwave`                           | string | Y        |
| `host`         | The hostname of the RisingWave server                             | string | Y        |
| `user`         | The username to use for authentication with the RisingWave server | string | Y        |
| `password`     | The password to use for authentication with the RisingWave server | string | N        |
| `port`         | The port number of the RisingWave engine server                   | int    | Y        |
| `database`     | The name of the database instance to connect to                   | string | Y        |
| `role`         | The role to use for authentication with the RisingWave server     | string | N        |
| `sslmode`      | The security of the connection to the RisingWave server           | string | N        |

## Extra Features

As a streaming database engine, RisingWave contains some extra features tailored specifically to streaming usecases.

Primarily, these are:
 - [Sources](https://docs.risingwave.com/sql/commands/sql-create-source) which are used to stream records into RisingWave from streaming sources like Kafka
 - [Sinks](https://docs.risingwave.com/sql/commands/sql-create-sink) which are used to write the results of data processed by RisingWave to an external target, such as an Apache Iceberg table in object storage.

RisingWave exposes these features via normal SQL statements, namely `CREATE SOURCE` and `CREATE SINK`. To utilize these in SQLMesh, you can use them in [pre / post statements](../../concepts/models/sql_models.md#optional-prepost-statements).

Here is an example of creating a Sink from a SQLMesh model using a post statement:

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

!!! info "@this_model"
    The `@this_model` macro resolves to the physical table for the current version of the model. See [here](../../concepts/macros/macro_variables.md#runtime-variables) for more information.
