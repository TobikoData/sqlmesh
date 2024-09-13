# Clickhouse

This page describes SQLMesh support for the Clickhouse engine, including configuration options specific to Clickhouse.

!!! note
    Clickhouse may not be used for the SQLMesh [state connection](../../reference/configuration.md#connections).

## Background

[Clickhouse](https://clickhouse.com/) is a distributed, column-oriented SQL engine designed to rapidly execute analytical workloads.

It provides users fine-grained control of its behavior, but that control comes at the cost of complex configuration.

This section provides background information about Clickhouse, providing context for how to use SQLMesh with the Clickhouse engine.

### Object naming

Most SQL engines use a three-level hierarchical naming scheme: tables/views are nested within _schemas_, and schemas are nested within _catalogs_. For example, the full name of a table might be `my_catalog.my_schema.my_table`.

Clickhouse instead uses a two-level hierarchical naming scheme that has no counterpart to _catalog_. In addition, it calls the second level in the hierarchy "databases." SQLMesh and its documentation refer to this second level as "schemas."

SQLMesh fully supports Clickhouse's two-level naming scheme without user action.

### Table engines

Every Clickhouse table is created with a ["table engine" that controls how the table's data is stored and queried](https://clickhouse.com/docs/en/engines/table-engines). Clickhouse's (and SQLMesh's) default table engine is `MergeTree`.

The [`MergeTree` engine family](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree) requires that every table be created with an `ORDER BY` clause.

SQLMesh will automatically inject an empty `ORDER BY` clause into every `MergeTree` family table's `CREATE` statement, or you can specify the columns/expressions by which the table should be ordered.

### Clickhouse modes of operation

Conceptually, it may be helpful to view Clickhouse as having three modes of operation: single server, cluster, and Clickhouse Cloud. SQLMesh supports all three modes.

#### Single server mode

Single server mode is similar to other SQL engines: aside from choosing each table's engine, you do not need to worry about how computations are executed. You issue standard SQL commands/queries, and Clickhouse executes them.

#### Cluster mode

Cluster mode allows you to scale your Clickhouse engine to any number of networked servers. This enables massive workloads, but requires that you specify how computations are executed by the networked servers.

Clickhouse coordinates the computations on the networked servers with [Clickhouse Keeper](https://clickhouse.com/docs/en/architecture/horizontal-scaling) (it also supports [Apache ZooKeeper](https://zookeeper.apache.org/)).

You specify named virtual clusters of servers in the Keeper configuration, and those clusters provide namespaces for data objects and computations. For example, you might include all networked servers in the cluster you name `MyCluster`.

In general, you must be connected to a Clickhouse server to execute commands. By default, each command you execute runs in single-server mode on the server you are connected to.

To associate an object with a cluster, DDL commands that create or modify it must include the text `ON CLUSTER [your cluster name]`.

If you provide a cluster name in your SQLMesh connection configuration, SQLMesh will automatically inject the `ON CLUSTER` statement into the DDL commands for all objects created while executing the project. We provide more information about clusters in SQLMesh [below](#cluster-specification).

#### Clickhouse Cloud mode

[Clickhouse Cloud](https://clickhouse.com/cloud) is a managed Clickhouse platform. It allows you to scale Clickhouse without administering a cluster yourself or modifying your SQL commands to run on the cluster.

Clickhouse Cloud automates Clickhouse's cluster controls, which sometimes constrains Clickhouse's flexibility or how you execute SQL commands. For example, creating a table with a `SELECT` command must [occur in two steps on Clickhouse Cloud](https://clickhouse.com/docs/en/sql-reference/statements/create/table#from-select-query). SQLMesh handles this limitation for you.

Aside from those constraints, Clickhouse Cloud mode is similar to single server mode - you run standard SQL commands/queries, and Clickhouse Cloud executes them.

## Cluster specification

A Clickhouse cluster allows multiple networked Clickhouse servers to operate on the same data object. Every cluster must be named in the Clickhouse configuration files, and that name is passed to a table's DDL statements in the `ON CLUSTER` clause.

For example, we could create a table `my_schema.my_table` on cluster `TheCluster` like this: `CREATE TABLE my_schema.my_table ON CLUSTER TheCluster (col1 Int8)`.

To create SQLMesh objects on a cluster, provide the cluster name to the `cluster` key in the SQLMesh connection definition (see all connection parameters [below](#localbuilt-in-scheduler)).

SQLMesh will automatically inject the `ON CLUSTER` clause and cluster name you provide into all project DDL statements.

## Model definition

This section describes how you control a table's engine and other Clickhouse-specific functionality in SQLMesh models.

### Table engine

SQLMesh uses the `MergeTree` table engine with an empty `ORDER BY` clause by default.

Specify a different table engine by passing the table engine definition to the model DDL's `storage_format` parameter. For example, you could specify the `Log` table engine like this:

``` sql linenums="1" hl_lines="4"
MODEL (
    name my_schema.my_log_table,
    kind full,
    storage_format Log,
);

select
    *
from other_schema.other_table;
```

You may also specify more complex table engine definitions. For example:

``` sql linenums="1" hl_lines="4"
MODEL (
    name my_schema.my_rep_table,
    kind full,
    storage_format ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', ver),
);

select
    *
from other_schema.other_table;
```

#### ORDER BY

`MergeTree` family engines require that a table's `CREATE` statement include the `ORDER BY` clause.

SQLMesh will automatically inject an empty `ORDER BY ()` when creating a table with an engine in the `MergeTree` family. This creates the table without any ordering.

You may specify columns/expressions to `ORDER BY` by passing them to the model `physical_properties` dictionary's `order_by` key.

For example, you could order by columns `col1` and `col2` like this:

``` sql linenums="1" hl_lines="4-6"
MODEL (
    name my_schema.my_log_table,
    kind full,
    physical_properties (
        order_by = (col1, col2)
    )
);

select
    *
from other_schema.other_table;
```

Note that there is an `=` between the `order_by` key name and value `(col1, col2)`.

Complex `ORDER BY` expressions may need to be passed in single quotes, with interior single quotes escaped by the `\` character.

#### PRIMARY KEY

Table engines may also accept a `PRIMARY KEY` specification. Similar to `ORDER BY`, specify a primary key in the model DDL's `physical_properties` dictionary. For example:

``` sql linenums="1" hl_lines="6"
MODEL (
    name my_schema.my_log_table,
    kind full,
    physical_properties (
        order_by = (col1, col2),
        primary_key = col1
    )
);

select
    *
from other_schema.other_table;
```

Note that there is an `=` between the `primary_key` key name and value `col1`.

### Partitioning

Some Clickhouse table engines support partitioning. Specify the partitioning columns/expressions in the model DDL's `partitioned_by` key.

For example, you could partition by columns `col1` and `col2` like this:

``` sql linenums="1" hl_lines="4"
MODEL (
    name my_schema.my_log_table,
    kind full,
    partitioned_by (col1, col2),
);

select
    *
from other_schema.other_table;
```


## Settings

Clickhouse supports an [immense number of settings](https://clickhouse.com/docs/en/operations/settings), many of which can be altered in multiple places: Clickhouse configuration files, Python client connection arguments, DDL statements, SQL queries, and others.

This section discusses how to control Clickhouse settings in SQLMesh.

### Connection settings

SQLMesh connects to Python with the [`clickhouse-connect` library](https://clickhouse.com/docs/en/integrations/python). Its connection method accepts a dictionary of arbitrary settings that are passed to Clickhouse.

Specify these settings in the `connection_settings` key. This example demonstrates how to set the `distributed_ddl_task_timeout` setting to `300`:

``` yaml linenums="1" hl_lines="8-9"
clickhouse_gateway:
  connection:
    type: clickhouse
    host: localhost
    port: 8123
    username: user
    password: pw
    connection_settings:
      distributed_ddl_task_timeout: 300
  state_connection:
    type: duckdb
```

### DDL settings

Clickhouse settings may also be specified in DDL commands like `CREATE`.

Specify these settings in a model DDL's [`physical_properties` key](https://sqlmesh.readthedocs.io/en/stable/concepts/models/overview/?h=physical#physical_properties) (where the [`order_by`](#order-by) and [`primary_key`](#primary-key) values are specified, if present).

This example demonstrates how to set the `index_granularity` setting to `128`:

``` sql linenums="1" hl_lines="4-6"
MODEL (
    name my_schema.my_log_table,
    kind full,
    physical_properties (
        index_granularity = 128
    )
);

select
    *
from other_schema.other_table;
```

Note that there is an `=` between the `index_granularity` key name and value `128`.

### Query settings

Clickhouse settings may be specified directly in a model's query with the `SETTINGS` keyword.

This example demonstrates setting the `join_use_nulls` setting to `1`:

``` sql linenums="1" hl_lines="9"
MODEL (
    name my_schema.my_log_table,
    kind full,
);

select
    *
from other_schema.other_table
SETTINGS join_use_nulls = 1;
```

Multiple settings may be specified in a query with repeated use of the `SETTINGS` keyword: `SELECT * FROM other_table SETTINGS first_setting = 1 SETTINGS second_setting = 2;`.

#### Usage by SQLMesh

The Clickhouse setting `join_use_nulls` affects the behavior of SQLMesh SCD models and table diffs. This section describes how SQLMesh uses query settings to control that behavior.

^^Background^^

In general, table `JOIN`s can return empty cells for rows not present in both tables.

For example, consider `LEFT JOIN`ing two tables `left` and `right`, where the column `right_column` is only present in the `right` table. Any rows only present in the `left` table will have no value for `right_column` in the joined table.

In other SQL engines, those empty cells are filled with `NULL`s.

In contrast, Clickhouse fills the empty cells with data type-specific default values (e.g., 0 for integer column types). It will fill the cells with `NULL`s instead if you set `join_use_nulls` to `1`.

^^SQLMesh^^

SQLMesh automatically generates SQL queries for both SCD Type 2 models and table diff comparisons. These queries include table `JOIN`s and calculations based on the presence of `NULL` values.

Because those queries expect `NULL` values in empty cells, SQLMesh automatically adds `SETTINGS join_use_nulls = 1` to the generated SCD and table diff SQL code.

The SCD model definition query is embedded as a CTE in the full SQLMesh-generated query. If run alone, the model definition query would use the Clickhouse server's current `join_use_nulls` value.

If that value is not `1`, the SQLMesh setting on the outer query would override the server value and produce incorrect results.

Therefore, SQLMesh uses the following procedure to ensure the model definition query runs with the correct `join_use_nulls` value:

- If the model query sets `join_use_nulls` itself, do nothing
- If the model query does not set `join_use_nulls` and the current server `join_use_nulls` value is `1`, do nothing
- If the model query does not set `join_use_nulls` and the current server `join_use_nulls` value is `0`, add `SETTINGS join_use_nulls = 0` to the CTE model query
    - All other CTEs and the outer query will still execute with a `join_use_nulls` value of `1`

## Local/Built-in Scheduler
**Engine Adapter Type**: `clickhouse`

### Connection options

| Option               | Description                                                                                                                                                               |  Type  | Required |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`               | Engine type name - must be `clickhouse`                                                                                                                                        | string |    Y     |
| `user`               | The username to log in to your server. | string |    Y     |
| `host`               | The hostname of your server. Do not include the `http://` or `https://` prefix.                                                                                           | string |    Y     |
| `port`               | The port to connect to your server. Default: 8123 for non-encrypted connections, 8443 for encrypted connections and Clickhouse Cloud.                                                                        |  int   |    Y     |
| `cluster`              | Name of the Clickhouse cluster on which SQLMesh should create objects. Should not be specified for standalone Clickhouse servers or Clickhouse Cloud.                                                                                                                                        |  string  |    N     |
| `use_compression`              | Enable compression for ClickHouse HTTP inserts and query results. Default: True                                                                                                                                        |  bool  |    N     |
| `compression_method`              | Use a specific compression method for inserts and query results - allowed values `lz4`, `zstd`, `br`, or `gzip`.                                                                                                                                        |  str  |    N     |
| `query_limit`              | Maximum number of rows to return for any query response. Default: 0 (unlimited rows)                                                                                                                                        |  int  |    N     |
| `connect_timeout`              | HTTP connection timeout in seconds. Default: 10                                                                                                                                        |  int  |    N     |
| `send_receive_timeout`              | Send/receive timeout for the HTTP connection in seconds. Default: 300                                                                                                                                        |  int  |    N     |
| `verify`              | Validate the ClickHouse server TLS/SSL certificate (hostname, expiration, etc.) if using HTTPS/TLS. Default: True                                                                                                                        |  bool  |    N     |
| `connection_settings`              | Arbitrary Clickhouse settings passed to the [`clickhouse-connect` client](https://clickhouse.com/docs/en/integrations/python#client-initialization).                                                                                                                        |  dict[str, any]  |    N     |