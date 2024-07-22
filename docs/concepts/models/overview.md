# Overview

Models are made up of metadata and queries that create tables and views, which can be used by other models or even outside of SQLMesh. They are defined in the `models/` directory of your SQLMesh project and live in `.sql` files.

SQLMesh will automatically determine the relationships among and lineage of your models by parsing SQL, so you don't have to worry about manually configuring dependencies.

## Example
The following is an example of a model defined in SQL. Note the following aspects:

  - Models can include descriptive information as comments, such as the first line.
  - The first non-comment statement in the file is the `MODEL` DDL.
  - The last non-comment statement is a `SELECT` query containing the logic that transforms the data.

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

One way it does this is by inferring a model's column names and data types from its SQL query. Disable this behavior for a model by manually specifying its column names and types in the [`columns` model property](#columns).

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

### Model description and comments

Model files may contain SQL comments in a format supported in the model's SQL dialect. (Comments begin with `--` or are gated by `/*` and `*/` in most dialects.)

Some SQL engines support registering comments as metadata associated with a table or view. They may support table-level comments (e.g., "Revenue data for each customer") and/or column-level comments (e.g., "Customer's unique ID").

SQLMesh will automatically register comments if the engine supports it and the [connection's `register_comments` configuration](../../reference/configuration.md#connection) is `true` (`true` by default). Engines vary in their support for comments - see [tables below](#engine-comment-support).

#### Model comment

SQLMesh will register a comment specified before the `MODEL` DDL block as the table comment in the underlying SQL engine. If the [`MODEL` DDL `description` field](#description) is also specified, SQLMesh will register it with the engine instead.

#### Explicit column comments

You may explicitly specify column comments in the `MODEL` DDL `column_descriptions` field.

Specify them as a dictionary of key/value pairs separated by an equals sign `=`, where the column name is the key and the column comment is the value. For example:

```sql linenums="1" hl_lines="4-6"
MODEL (
  name sushi.customer_total_revenue,
  cron '@daily',
  column_descriptions (
    id = 'This is the ID column comment'
  )
);
```

If the `column_descriptions` key is present, SQLMesh will not detect and register inline column comments from the model query.

#### Inline column comments

If the `column_descriptions` key is not present in the `MODEL` definition, SQLMesh will automatically detect comments in a query's column selections and register each column's final comment in the underlying SQL engine.

For example, the physical table created for the following model definition would have:

1. The value of its `MODEL` DDL `description` field, "Revenue data for each customer", registered as a table comment in the SQL engine
2. The comment on the `customer_id` column definition, "Customer's unique ID", registered as a column comment for the table's `customer_id` column
3. The second comment on the `revenue` column definition, "Revenue from customer orders", registered as a column comment for the table's `revenue` column

```sql linenums="1" hl_lines="7 11 13"
-- The MODEL DDL 'description' field is present, so this comment will not be registered with the SQL engine
MODEL (
  name sushi.customer_total_revenue,
  owner toby,
  cron '@daily',
  grain customer_id,
  description 'Revenue data for each customer'
);

SELECT
  o.customer_id::TEXT, -- Customer's unique ID
  -- This comment will not be registered because another `revenue` comment is present
  SUM(o.amount)::DOUBLE AS revenue -- Revenue from customer orders
FROM sushi.orders AS o
GROUP BY o.customer_id;
```

#### Python models

[Python models](./python_models.md) are not parsed like SQL models, so column comments cannot be inferred from the model definition's inline comments.

Instead, specify them in the `@model` decorator's `column_descriptions` key. Specify them in a dictionary whose keys are column names and values are the columns' comments. SQLMesh will error if a column name is present that is not also in the `columns` key.

For example:

```python linenums="1" hl_lines="8-10"
from sqlmesh import ExecutionContext, model

@model(
    "my_model.name",
    columns={
        "column_name": "int",
    },
    column_descriptions={
        "column_name": "The `column_name` column comment",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
```

#### Comment registration by object type

Only some tables/views have comments registered:

- Temporary tables are not registered
- Non-temporary tables and views in the physical layer (i.e., the schema named `sqlmesh__[project schema name]`) are registered
- Views in non-prod environments are not registered
- Views in the `prod` environment are registered

Some engines automatically pass comments from physical tables through to views that select from them. In those engines, views may display comments even if SQLMesh did not explicitly register them.

#### Engine comment support

Engines vary in their support for comments and their method(s) of registering comments. Engines may support one or both registration methods: in the `CREATE` command that creates the object or with specific post-creation commands.

In the former method, column comments are embedded in the `CREATE` schema definition - for example: `CREATE TABLE my_table (my_col INTEGER COMMENT 'comment on my_col') COMMENT 'comment on my_table'`. This means that all table and column comments can be registered in a single command.

In the latter method, separate commands are required for every comment. This may result in many commands: one for the table comment and one for each column comment. In some scenarios, SQLMesh is not able to use the former `CREATE` method and must issue separate commands. Because SQLMesh must use different methods in different situations and engines vary in their support of the methods, comments may not be registered for all objects.

This table lists each engine's support for `TABLE` and `VIEW` object comments:

| Engine        | `TABLE` comments | `VIEW` comments |
| ------------- | ---------------- | --------------- |
| BigQuery      | Y                | Y               |
| Databricks    | Y                | Y               |
| DuckDB <=0.9  | N                | N               |
| DuckDB >=0.10 | Y                | Y               |
| MySQL         | Y                | Y               |
| MSSQL         | N                | N               |
| Postgres      | Y                | Y               |
| GCP Postgres  | Y                | Y               |
| Redshift      | Y                | N               |
| Snowflake     | Y                | Y               |
| Spark         | Y                | Y               |
| Trino         | Y                | Y               |


## Model properties
The `MODEL` DDL statement takes various properties, which are used for both metadata and controlling behavior.

Learn more about these properties and their default values in the [model configuration reference](../../reference/model_configuration.md#general-model-properties).

### name
:   Name specifies the name of the model. This name represents the production view name that the model outputs, so it generally takes the form of `"schema"."view_name"`. The name of a model must be unique in a SQLMesh project.

    When models are used in non-production environments, SQLMesh automatically prefixes the names. For example, consider a model named `"sushi"."customers"`. In production its view is named `"sushi"."customers"`, and in dev its view is named `"sushi__dev"."customers"`.

    Name is ***required*** and must be ***unique***, unless [name inference](../../reference/model_configuration.md#model-naming) is enabled.

### project
:   Project specifies the name of the project the model belongs to. Used in multi-repo SQLMesh deployments.

### kind
:   Kind specifies what [kind](model_kinds.md) a model is. A model's kind determines how it is computed and stored. The default kind is `VIEW`, which means a view is created and your query is run each time that view is accessed. See [below](#incremental-model-properties) for properties that apply to incremental model kinds.

### audits
:   Audits specifies which [audits](../audits.md) should run after the model is evaluated.

### dialect
:   Dialect defines the SQL dialect of the model. By default, this uses the dialect in the [configuration file `model_defaults` `dialect` key](../../reference/configuration.md#model-configuration). All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/__init__.py) are allowed.

### owner
:   Owner specifies who the main point of contact is for the model. It is an important field for organizations that have many data collaborators.

### stamp
:   An optional arbitrary string sequence used to create a new model version without changing the functional components of the definition.

### tags
:   Tags are one or more labels used to organize your models.

### cron
:   Cron is used to schedule your model to process or refresh at a certain interval. It accepts a [cron expression](https://en.wikipedia.org/wiki/Cron) or any of `@hourly`, `@daily`, `@weekly`, or `@monthly`.

### interval_unit
:   Interval unit determines the granularity of data intervals for this model. By default the interval unit is automatically derived from the `cron` expression. Supported values are: `year`, `month`, `day`, `hour`, `half_hour`, `quarter_hour`, and `five_minute`.

### start
:   Start is used to determine the earliest time needed to process the model. It can be an absolute date/time (`2022-01-01`), or a relative one (`1 year ago`).

### end
:   End is used to determine the latest time needed to process the model. It can be an absolute date/time (`2022-01-01`), or a relative one (`1 year ago`).

### description
:   Optional description of the model. Automatically registered as a table description/comment with the underlying SQL engine (if supported by the engine).

### column_descriptions
:   Optional dictionary of [key/value pairs](#explicit-column-comments). Automatically registered as column descriptions/comments with the underlying SQL engine (if supported by the engine). If not present, [inline comments](#inline-column-comments) will automatically be registered.

### grain
:   A model's grain is the column or combination of columns that uniquely identify a row in the results returned by the model's query. If the grain is set, SQLMesh tools like `table_diff` are simpler to run because they automatically use the model grain for parameters that would otherwise need to be specified manually.

### grains
:   A model can define multiple grains if it has more than one unique key or combination of keys.

### references
:   References are non-unique columns or combinations of columns that identify a join relationship to another model.

    For example, a model could define a reference `account_id`, which would indicate that it can now automatically join to any model with an `account_id` grain. It cannot safely join to a table with an `account_id` reference because references are not unique and doing so would constitute a many-to-many join.

    Sometimes columns are named differently, in that case you can alias column names to a common entity name. For example `guest_id AS account_id` would allow a model with the column guest\_id to join to a model with the grain account\_id.

### depends_on
:   Depends on explicitly specifies the models on which the model depends, in addition to the ones automatically inferred by from the model code.

### storage_format
:   Storage format is a property for engines such as Spark or Hive that support storage formats such as  `parquet` and `orc`.

### partitioned_by
:   Partitioned by plays two roles. For most model kinds, it is an optional property for engines that support table partitioning such as Spark or BigQuery.

    For the [`INCREMENTAL_BY_PARTITION` model kind](./model_kinds.md#incremental_by_partition), it defines the partition key used to incrementally load data.

    It can specify a multi-column partition key or modify a date column for partitioning. For example, in BigQuery you could partition by day by extracting the day component of a timestamp column `event_ts` with `partitioned_by TIMESTAMP_TRUNC(event_ts, DAY)`.

### clustered_by
:   Clustered by is an optional property for engines such as Bigquery that support clustering.

### columns
:   By default, SQLMesh [infers a model's column names and types](#conventions) from its SQL query. Disable that behavior by manually specifying all column names and data types in the model's `columns` property.

    **WARNING**: SQLMesh may exhibit unexpected behavior if the `columns` property includes columns not returned by the query, omits columns returned by the query, or specifies data types other than the ones returned by the query.

    For example, this shows a seed model definition that includes the `columns` key. It specifies the data types for all columns in the file: the `holiday_name` column is data type `VARCHAR` and the `holiday_date` column is data type `DATE`.

    ```sql linenums="1" hl_lines="6-9"
    MODEL (
      name test_db.national_holidays,
      kind SEED (
        path 'national_holidays.csv'
      ),
      columns (
        holiday_name VARCHAR,
        holiday_date DATE
      )
    );
    ```

    NOTE: Specifying column names and data types is required for [Python models](../models/python_models.md) that return DataFrames.

### physical_properties
:   Previously named `table_properties`

    Physical properties is a key-value mapping of arbitrary properties specific to the target engine that are applied to the model table / view in the physical layer. For example:

    ```sql linenums="1"
    MODEL (
      ...,
      physical_properties (
        partition_expiration_days = 7,
        require_partition_filter = true
      )
    );

    ```

### virtual_properties
:   Virtual properties is a key-value mapping of arbitrary properties specific to the target engine that are applied to the model view in the virtual layer. For example:

    ```sql linenums="1"
    MODEL (
      ...,
      virtual_properties (
        labels = [('test-label', 'label-value')]
      )
    );

    ```

### session_properties
:   Session properties is a key-value mapping of arbitrary properties specific to the target engine that are applied to the engine session.

### allow_partials
:   Indicates that this model can be executed for partial (incomplete) data intervals.

    By default, each model processes only complete intervals to prevent common errors caused by partial data. The size of the interval is determined by the model's [interval_unit](#interval_unit).

    Setting `allow_partials` to `true` overrides this behavior, indicating that the model may process a segment of input data that is missing some of the data points.

    NOTE: setting this attribute to `true` disregards the [cron](#cron) property.

### enabled
:   Whether the model is enabled. This attribute is `true` by default. Setting it to `false` causes SQLMesh to ignore this model when loading the project.

## Incremental Model Properties

These properties can be specified in an incremental model's `kind` definition.

Some properties are only available in specific model kinds - see the [model configuration reference](../../reference/model_configuration.md#incremental-models) for more information and a complete list of each `kind`'s properties.

### time_column
:   Time column is a required property for incremental models. It is used to determine which records to overwrite when doing an incremental insert. Time column can have an optional format string specified in the SQL dialect of the model.

    Engines that support partitioning, such as Spark and BigQuery, use the time column as the model's partition key. Multi-column partitions or modifications to columns can be specified with the [`partitioned_by` property](#partitioned_by).

### batch_size
:   Batch size is used to optimize backfilling incremental data. It determines the maximum number of intervals to run in a single job.

    For example, if a model specifies a cron of `@hourly` and a batch_size of `12`, when backfilling 3 days of data, the scheduler will spawn 6 jobs. (3 days * 24 hours/day = 72 hour intervals to fill. 72 intervals / 12 intervals per job = 6 jobs.)

### batch_concurrency
:   The maximum number of [batches](#batch_size) that can run concurrently for this model. If not specified, the concurrency is only constrained by the number of concurrent tasks set in the connection settings.

### lookback
:   Lookback is used with [incremental by time range](model_kinds.md#incremental_by_time_range) models to capture late-arriving data. It must be a positive integer and specifies the number of interval time units prior to the current interval the model should include.

    For example, a model with cron `@daily` and `lookback` of 7 would include the previous 7 days each time it ran, while a model with cron `@weekly` and `lookback` of 7 would include the previous 7 weeks each time it ran.

### forward_only
:   Set this to true to indicate that all changes to this model should be [forward-only](../plans.md#forward-only-plans).

### on_destructive_change
:   What should happen when a change to a [forward-only model](../../guides/incremental_time.md#forward-only-models) or incremental model in a [forward-only plan](../plans.md#forward-only-plans) causes a destructive modification to the table schema (i.e., requires dropping an existing column).

    SQLMesh checks for destructive changes at plan time based on the model definition and run time based on the model's underlying physical tables.

    Must be one of the following values: `allow`, `warn`, or `error` (default).

### disable_restatement
:   Set this to true to indicate that [data restatement](../plans.md#restatement-plans) is disabled for this model.

## Macros
Macros can be used for passing in parameterized arguments such as dates, as well as for making SQL less repetitive. By default, SQLMesh provides several predefined macro variables that can be used. Macros are used by prefixing with the `@` symbol. For more information, refer to [macros](../macros/overview.md).

## Statements
Models can have additional statements that run before and/or after the main query. They can be useful for loading things such as [UDFs](../glossary.md#user-defined-function-udf) or cleaning up after a model query has run.

In general, pre-statements statements should only be used for preparing the main query. They should not be used for creating or altering tables, as this could lead to unpredictable behavior if multiple models are running simultaneously.

```sql linenums="1" hl_lines="5-7"
MODEL (
...
);

-- Additional statements preparing for main query
ADD JAR s3://special_udf.jar;
CREATE TEMPORARY FUNCTION UDF AS 'my.jar.udf';

SELECT UDF(x)::int AS x
FROM y;
```

Additional statements can also be provided after the main query, in which case they will run after each evaluation of the SELECT query. Note that the model query must end with a semi-colon prior to the post-statements.

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

A time column is a column in a model with an optional format string in the dialect of the model; for example, `'%Y-%m-%d'` for DuckDB or `'yyyy-mm-dd'` for Snowflake.

For more information, refer to [time column](./model_kinds.md#time-column).

### Advanced usage
The column used as your model's time column is not limited to a text or date type. In the following example, the time column, `di`, is an integer:

```sql linenums="1" hl_lines="5"
-- Orders are partitioned by the di int column
MODEL (
  name sushi.orders,
  dialect duckdb,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (order_date_int, '%Y%m%d')
  ),
);

SELECT
  id::INT AS id, -- Primary key
  customer_id::INT AS customer_id, -- Id of customer who made the order
  waiter_id::INT AS waiter_id, -- Id of waiter who took the order
  start_ts::TEXT AS start_ts, -- Start timestamp
  end_ts::TEXT AS end_ts, -- End timestamp
  di::INT AS order_date_int -- Date of order
FROM raw.orders
WHERE
  order_date_int BETWEEN @start_ds AND @end_ds
```
SQLMesh will handle casting the start and end dates to the type of your time column. The format is reflected in the time column format string.
