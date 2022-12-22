"""
# Model

Models are comprised of metadata and queries that create tables and views which can be used by other models or even outside of SQLMesh.
They are defined in a the `models/` directory of your SQLMesh product and live in `.sql` files. SQLMesh will automatically understand
the relationships and lineage of your models by parsing your SQL so you don't have to worry about manually setting up dependencies.

# Example
Models can be defined simply in SQL. The first statement of a model.sql file should be the MODEL DDL. The last statement will be a `SELECT` statement
that defines the logic needed to create the table.

```sql
-- Customer revenue computed and stored daily.
MODEL (
  name sushi.customer_revenue_by_day,
  owner toby,
  cron '@daily',
);

SELECT
  c.customer_id::TEXT,
  SUM(o.amount)::DOUBLE AS revenue
  o.ds::TEXT
FROM sushi.orders AS o
JOIN sushi.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.ds BETWEEN @start_ds and @end_ds
```

# Conventions
SQLMesh attempts to infer a lot your pipelines through SQL alone to reduce the cognitive overhead of switching to another format like YAML.
The `SELECT` expression of a model must adhere to certain conventions in order for SQLMesh to get the necessary metadata to operate.

## Unique Column Names
The final selects of a model's query must be unique.

## Explict Types
The final selects of a model's query must be explicitly casted to a type. This way, SQLMesh can automatically create tables with the appropriate schema. SQLMesh uses the
postgres `x::int` syntax for casting because it is elegant. These postgres style casts will be transpiled automatically to the appropriate format of the
execution engine.

```sql
WITH cte AS (
SELECT 1 AS foo -- don't need to cast here
)
SELECT foo::int -- need to cast here because it is in the final select statement
```

## Inferrable Names
The final selects of a model's query must have inferrable names or aliases. An explicit alias is preferable but not necessary. Aliases will be automatically added by the SQLMesh formatter.

```sql
SELECT
  1, -- not inferrable
  x + 1, -- not infererrable
  SUM(x), -- not infererrable
  x, -- inferrable as x
  x::int, -- inferrable as x
  x + 1 AS x, -- explictly x
  SUM(x) as x, -- explicitly x
```

# Properties
The MODEL statement takes various properties which are used for both metadata and controlling behavior.

## name
- Name specifies the name of the model. This name represents the production view name that the model outputs so it generally
takens on the form of `"schema"."view_name"`. The name of a model must be unique in a SQLMesh project. When models are used in development
environments, SQLMesh automatically prefixes the name with a prefix. So given a model named `"sushi"."customers"`, in production, the view is named
 `"sushi"."customers"` and in dev `"dev__sushi"."customers"`.
- Name is ***required***, and must be ***unique***.

## kind
- Kind specifies what [kind](#model-kinds) a model is. A model's kind determines how it is computed and stored. The default kind is `incremental` which means that a model processes and stores data incrementally by minute, hour, or day.

## dialect
- Dialect defines the SQL dialect of the file. By default, this uses the dialect of the SQLMesh `sqlmesh.core.config`.

## owner
- Owner specifies who the main POC is for this particular model. It is an important field for organizations that have many data collaborators.

## start
- Start is used to determine the earliest time needed to process the model. It can be an absolute date/time (2022-01-01) or relative (`1 year ago`).

## cron
- Cron is used to schedule your model to process or refresh at a certain interval. It uses [croniter](https://github.com/kiorky/croniter) under the hood
    so expressions like `@daily` can be used. A model's `IntervalUnit` is determined implicity by the cron expression.

## batch_size
- Batch size is used to optimize backfilling incremental data. It determines the maximum number of intervals to run in a single job. For example, if
    a model specifies a cron of @hourly and a batch_size of 12, when backfilling 3 days of data, the scheduler will spawn 6 jobs.

## storage_format
- Storage format is an optional property for engines like Spark/Hive that support various storage formats like `parquet` and `orc`.

## time_column
- Time column is a required property for incremental models. It is used to determine which records to overwrite when doing an incremental insert.
Engines that support partitioning like Spark and Hive also use it as the partition key. Additional partition key columns can be specified with the
partitioned_by property below. Time column can have an optional format string. The format should be in the dialect of the model.

## partitioned_by
- Partition by is an optional property for engines like Spark/Hive that support partitioning. Use this to add additional columns to the time column partition key.

Models can also have descriptions associated with them in the form of comments, like in the following example:

```sql
/* Customer revenue computed and stored daily. */
MODEL (
  name sushi.customer_revenue_by_day,
  owner toby,
  cron '@daily',
);
```


# Macros
Macros can be used for passing in paramaterized arguments like dates as well as for making SQL less repetitive. By default, SQLMesh provides several predefined macro variables that can be used your SQL. Macros are used by prefixing with the `@` symbol.

- @start_date
```sql
-- The inclusive start interval of an execution casted to a DATETIME SQL object.
@start_date = CAST("2020-01-01 00:00:00.0" AS DATETIME)
```
- @end_date
```sql
-- The inclusive end interval of an execution casted to a DATETIME SQL object.
@end_date = CAST("2020-01-01 23:59:59.999000" AS DATETIME)
```
- @latest_date
```sql
-- The latest datetime or current run date of an execution. Used when you only care about the latest data.
@latest_date = CAST("2020-01-01 00:00:00.0" AS DATETIME)
```
- @start_ds
```sql
-- The inclusive start date string.
@start_ds = '2020-01-01'
```
- @end_ds
```sql
-- The inclusive end date string.
@end_ds = '2020-01-01'
```
- @latest_ds
```sql
-- The date string of the run date.
@end_ds = '2020-01-01'
```

Read more about `sqlmesh.core.macros`.


# Statements
Models can have additional statements the run before the main query. This can be useful for loading things like [UDFs](https://en.wikipedia.org/wiki/User-defined_function). In general, statements should only be used for preparing the main query. They should not be used for creating or altering tables as this could lead to unpredictable behavior.

```SQL
MODEL (
...
);

ADD JAR s3://special_udf.jar;
CREATE TEMPORARY FUNCTION UDF AS 'my.jar.udf';

SELECT UDF(x)::int AS x
FROM y
```


# Time Column
Models that are loaded incrementally require a time column to partition data. A time column is a column in a model with an optional format string in
the dialect of the model, e.g. '%Y-%m-%d' for DuckDB or 'yyyy-mm-dd' for Snowflake.
```sql
-- Orders are partitioned by the ds column
MODEL (
  name sushi.orders,
  dialect duckdb,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (ds, '%Y-%m-%d')
  )
);

SELECT
  id::INT AS id, -- Primary key
  customer_id::INT AS customer_id, -- Id of customer who made the order
  waiter_id::INT AS waiter_id, -- Id of waiter who took the order
  start_ts::TEXT AS start_ts, -- Start timestamp
  end_ts::TEXT AS end_ts, -- End timestamp
  ds::TEXT AS ds -- Date of order
FROM raw.orders
WHERE
  ds BETWEEN @start_ds AND @end_ds
```
When SQLMesh incrementally inserts data for a partition, it will overwrite any existing data in that partition. For engines that support partitions,
it will use an `INSERT OVERWRITE` query. For other engines that do not, it will first delete the data in the partition before inserting.

## Format String Configuration
The format string tells SQLMesh how your dates are formatted so it can compare start and end dates correctly. You can configure a project wide default
format in your project configuration. A time column format string declared in a model will override the project wide default. If the model uses a
different dialect than the rest of your project, the format string will be automatically transpiled to the model dialect with SQLGlot. SQLMesh will use
`%Y-%m-%d` as the default if no default time column format is configured.
See `sqlmesh.core.config`.

## Advanced Usage
The column used as your model's time column is not limited to being a text or date type. In the following example, the time column, `di`, is an integer.
```sql
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
"""
from sqlmesh.core.model.common import parse_model_name
from sqlmesh.core.model.decorator import model
from sqlmesh.core.model.definition import Model
from sqlmesh.core.model.kind import (
    IncrementalByTimeRange,
    IncrementalByUniqueKey,
    ModelKind,
    ModelKindName,
    TimeColumn,
)
from sqlmesh.core.model.meta import ModelMeta
