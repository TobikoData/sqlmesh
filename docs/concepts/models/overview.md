# Overview

Models are comprised of metadata and queries that create tables and views, which can be used by other models or even outside of SQLMesh. They are defined in the `models/` directory of your SQLMesh project and live in `.sql` files.

SQLMesh will automatically determine the relationships among and lineage of your models by parsing SQL, so you don't have to worry about manually configuring dependencies.

## Example
The following is an example of a model defined in SQL. Note the following aspects:
  - Models can include descriptive information as comments, such as the first line.
  - The first non-comment statement of a `model.sql` file is the `MODEL` DDL.
  - The last non-comment statement should be a `SELECT` statement that defines the logic needed to create the table

```sql linenums="1"
-- Customer revenue computed and stored daily.
MODEL (
  name sushi.customer_total_revenue,
  owner toby,
  cron '@daily',
  grain customer_id
);

SELECT
  o.customer_id::TEXT,
  SUM(o.amount)::DOUBLE AS revenue
FROM sushi.orders AS o
GROUP BY o.customer_id;
```

## Conventions
SQLMesh attempts to infer as much as possible about your pipelines through SQL alone to reduce the cognitive overhead of switching to another format such as YAML.

The `SELECT` expression of a model must follow certain conventions for SQLMesh to detect the necessary metadata to operate.

### Unique column names
The final `SELECT` of a model's query must contain unique column names.

### Explicit types
SQLMesh encourages explicit type casting in the final `SELECT` of a model's query. It is considered a best practice to prevent unexpected types in the schema of a model's table.

SQLMesh uses the postgres `x::int` syntax for casting; the casts are automatically transpiled to the appropriate format for the execution engine.

```sql linenums="1"
WITH cte AS (
SELECT 1 AS foo -- don't need to cast here
)
SELECT foo::int -- need to cast here because it's in the final select statement
```

### Inferrable names
The final `SELECT` of a model's query must have inferrable names or aliases.

Explicit aliases are recommended, but not required. The SQLMesh formatter will automatically add aliases to columns without them when the model SQL is rendered.

This example demonstrates non-inferrable, inferrable, and explicit aliases:

```sql linenums="1"
SELECT
  1, -- not inferrable
  x + 1, -- not inferrable
  SUM(x), -- not inferrable
  x, -- inferrable as x
  x::int, -- inferrable as x
  x + 1 AS x, -- explicitly x
  SUM(x) as x, -- explicitly x
```

## Properties
The `MODEL` DDL statement takes various properties, which are used for both metadata and controlling behavior.

### name
- `name` specifies the name of the model. This name represents the production view name that the model outputs, so it generally takes the form of `"schema"."view_name"`. The name of a model must be unique in a SQLMesh project.<br /><br />
When models are used in non-production environments, SQLMesh automatically prefixes the names. For example, consider a model named `"sushi"."customers"`. In production its view is named `"sushi"."customers"`, and in dev its view is named `"sushi__dev"."customers"`.<br /><br />
Name is ***required*** and must be ***unique***.

### kind
- Kind specifies what [kind](model_kinds.md) a model is. A model's kind determines how it is computed and stored. The default kind is `VIEW`, which means a view is created and your query is run each time that view is accessed. See [below](#incremental-model-properties) for properties that apply to incremental model kinds.

### dialect
- Dialect defines the SQL dialect of the model. By default, this uses the dialect in the [configuration file `model_defaults` `dialect` key](../../reference/configuration.md#model-configuration). All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/__init__.py) are allowed.

### owner
- Owner specifies who the main point of contact is for the model. It is an important field for organizations that have many data collaborators.

### stamp
- An optional arbitrary string sequence used to create new model versions without making changes to any of the functional components of the definition.

### start
- Start is used to determine the earliest time needed to process the model. It can be an absolute date/time (`2022-01-01`), or a relative one (`1 year ago`).

### cron
- Cron is used to schedule your model to process or refresh at a certain interval. It uses [croniter](https://github.com/kiorky/croniter) under the hood, so expressions such as `@daily` can be used.

### interval_unit
- Interval unit determines the granularity of data intervals for this model. By default the interval unit is automatically derived from the `cron` expression. Supported values are: `year`, `month`, `day`, `hour` and `minute`.

### tags
- Tags are one or more labels used to organize your models.

### grain
- A model's grain is the column or combination of columns that uniquely identify a row in the results returned by the model's query. If the grain is set, SQLMesh tools like `table_diff` are simpler to run because they automatically use the model grain for parameters that would otherwise need to be specified manually.

### storage_format
- Storage format is a property for engines such as Spark or Hive that support storage formats such as  `parquet` and `orc`.

### partitioned_by
- Partitioned by is an optional property for engines such as Spark or Hive that support partitioning. Use this to add additional columns to the time column partition key.

### clustered_by
- Clustered by is an optional property for engines such as Bigquery that support clustering.

### table_properties
- A key-value of arbitrary table properties specific to the target engine. For example:

```sql linenums="1"
MODEL (
  ...,
  table_properties (
    partition_expiration_days = 7,
    require_partition_filter = true
  )
);

```

## Incremental Model Properties

For models that are incremental, the following parameters can be specified in the `kind`'s definition.

### time_column
- Time column is a required property for incremental models. It is used to determine which records to overwrite when doing an incremental insert. Engines that support partitioning such as Spark and Hive also use it as the partition key. Additional partition key columns can be specified with the `partitioned_by` property (see below). Time column can have an optional format string. The format should be in the dialect of the model.

### lookback
- Lookback is used for [incremental](model_kinds.md#incremental_by_time_range) models to capture late arriving data. This must be a positive integer and refers to the number of units that late arriving data is expected.

### batch_size
- Batch size is used to optimize backfilling incremental data. It determines the maximum number of intervals to run in a single job. For example, if a model specifies a cron of `@hourly` and a batch_size of `12`, when backfilling 3 days of data, the scheduler will spawn 6 jobs. (3 days * 24 hours/day = 72 hour intervals to fill. 72 intervals / 12 intervals per job = 6 jobs.)

## Macros
Macros can be used for passing in parameterized arguments such as dates, as well as for making SQL less repetitive. By default, SQLMesh provides several predefined macro variables that can be used. Macros are used by prefixing with the `@` symbol. For more information, refer to [macros](../macros/overview.md).

## Statements
Models can have additional statements that run before the main query. This can be useful for loading things such as [UDFs](../glossary.md#user-defined-function-udf).

In general, such statements should only be used for preparing the main query. They should not be used for creating or altering tables, as this could lead to unpredictable behavior.

```sql linenums="1" hl_lines="5-7"
MODEL (
...
);

-- Additional statements preparing for main query
ADD JAR s3://special_udf.jar;
CREATE TEMPORARY FUNCTION UDF AS 'my.jar.udf';

SELECT UDF(x)::int AS x
FROM y
```

Additional statements can also be provided **after** the main query, in which case they will run after each evaluation of the SELECT query.

```sql linenums="1" hl_lines="10-11"
MODEL (
...
);

...

SELECT UDF(x)::int AS x
FROM y;

-- Cleanup statements
DROP TABLE temp_table;
```

## Time column
Models that are loaded incrementally require a time column to partition data.

A time column is a column in a model with an optional format string in the dialect of the model; for example, `'%Y-%m-%d'` for DuckDB or `'yyyy-mm-dd'` for Snowflake. For more information, refer to [time column](./model_kinds.md#time-column).

## Advanced usage
The column used as your model's time column is not limited to a text or date type. In the following example, the time column, `di`, is an integer:

```sql linenums="1" hl_lines="5"
-- Orders are partitioned by the di int column
MODEL (
  name sushi.orders,
  dialect duckdb,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (di, '%Y%m%d')
  ),
);

SELECT
  id::INT AS id, -- Primary key
  customer_id::INT AS customer_id, -- Id of customer who made the order
  waiter_id::INT AS waiter_id, -- Id of waiter who took the order
  start_ts::TEXT AS start_ts, -- Start timestamp
  end_ts::TEXT AS end_ts, -- End timestamp
  di::INT AS di -- Date of order
FROM raw.orders
WHERE
  di BETWEEN @start_ds AND @end_ds
```
SQLMesh will handle casting the start and end dates to the type of your time column. The format is reflected in the time column format string.
