# Model kinds

This page describes the kinds of [models](./overview.md) SQLMesh supports, which determine how the data for a model is loaded.

## INCREMENTAL_BY_TIME_RANGE

Models of the `INCREMENTAL_BY_TIME_RANGE` kind are computed incrementally based on a time range. This is an optimal choice for datasets in which records are captured over time and represent immutable facts such as events, logs, or transactions. Using this kind for appropriate datasets typically results in significant cost and time savings.

Only missing time intervals are processed during each execution for `INCREMENTAL_BY_TIME_RANGE` models. This is in contrast to the [FULL](#full) model kind, where the entire dataset is recomputed every time the model is executed.

An `INCREMENTAL_BY_TIME_RANGE` model query must contain an expression in its SQL `WHERE` clause that filters the upstream records by time range. SQLMesh provides special macros that represent the start and end of the time range being processed: `@start_date` / `@end_date` and `@start_ds` / `@end_ds`.

Refer to [Macros](../macros/macro_variables.md) for more information.

This example implements an `INCREMENTAL_BY_TIME_RANGE` model by specifying the `kind` in the `MODEL` ddl and including a SQL `WHERE` clause to filter records by time range:
```sql linenums="1" hl_lines="3-5 12-13"
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds
  )
);

SELECT
  event_date::TEXT as ds,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  event_date BETWEEN @start_ds AND @end_ds;
```

### Time column
SQLMesh needs to know which column in the model's output represents the timestamp or date associated with each record.

The `time_column` is used to determine which records will be overridden during data [restatement](../plans.md#restatement-plans) and provides a partition key for engines that support partitioning (such as Apache Spark):

```sql linenums="1" hl_lines="4"
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date_column
  )
);
```

By default, SQLMesh assumes the time column is in the `%Y-%m-%d` format. For other formats, the default can be overridden as follows:
```sql linenums="1" hl_lines="4"
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (date_column, '%Y-%m-%d')
  )
);
```
**Note:** The time format should be defined using the same SQL dialect as the one used to define the model's query.

SQLMesh also uses the time column to automatically append a time range filter to the model's query at runtime, which prevents records that are not part of the target interval from being stored. This is a safety mechanism that prevents unintentionally overriding unrelated records when handling late-arriving data.

Consider the following model definition:
```sql linenums="1"
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  )
);

SELECT
  event_date::TEXT as event_date,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  receipt_date BETWEEN @start_ds AND @end_ds;
```

At runtime, SQLMesh will automatically modify the model's query to look like this:
```sql linenums="1" hl_lines="7"
SELECT
  event_date::TEXT as event_date,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  receipt_date BETWEEN @start_ds AND @end_ds
  AND event_date BETWEEN @start_ds AND @end_ds;
```

### Idempotency
It is recommended that queries of models of this kind are [idempotent](../glossary.md#idempotency) to prevent unexpected results during data [restatement](../plans.md#restatement-plans).

Note, however, that upstream models and tables can impact a model's idempotency. For example, referencing an upstream model of kind [FULL](#full) in the model query automatically causes the model to be non-idempotent.

### Materialization strategy
Depending on the target engine, models of the `INCREMENTAL_BY_TIME_RANGE` kind are materialized using the following strategies:

| Engine     | Strategy                                  |
|------------|-------------------------------------------|
| Spark      | INSERT OVERWRITE by time column partition |
| Databricks | INSERT OVERWRITE by time column partition |
| Snowflake  | DELETE by time range, then INSERT         |
| BigQuery   | DELETE by time range, then INSERT         |
| Redshift   | DELETE by time range, then INSERT         |
| Postgres   | DELETE by time range, then INSERT         |
| DuckDB     | DELETE by time range, then INSERT         |

## INCREMENTAL_BY_UNIQUE_KEY

Models of the `INCREMENTAL_BY_UNIQUE_KEY` kind are computed incrementally based on a unique key.

If a key is missing in the model's table, the new data row is inserted; otherwise, the existing row associated with this key is updated with the new one. This kind is a good fit for datasets that have the following traits:

* Each record has a unique key associated with it.
* There is at most one record associated with each unique key.
* It is appropriate to upsert records, so existing records can be overridden by new arrivals when their keys match.

A [Slowly Changing Dimension](../glossary.md#slowly-changing-dimension-scd) (SCD) is one approach that fits this description well.

The name of the unique key column must be provided as part of the `MODEL` DDL, as in this example:
```sql linenums="1" hl_lines="3-5"
MODEL (
  name db.employees,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key name
  )
);

SELECT
  name::TEXT as name,
  title::TEXT as title,
  salary::INT as salary
FROM raw_employees;
```

Composite keys are also supported:
```sql linenums="1" hl_lines="4"
MODEL (
  name db.employees,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (first_name, last_name)
  )
);
```

`INCREMENTAL_BY_UNIQUE_KEY` model kinds can also filter upstream records by time range using a SQL `WHERE` clause and the `@start_date`, `@end_date` or other macros (similar to the [INCREMENTAL_BY_TIME_RANGE](#incremental_by_time_range) kind):
```sql linenums="1" hl_lines="6-7"
SELECT
  name::TEXT as name,
  title::TEXT as title,
  salary::INT as salary
FROM raw_employee_events
WHERE
  event_date BETWEEN @start_date AND @end_date;
```

**Note:** Models of the `INCREMENTAL_BY_UNIQUE_KEY` kind are inherently [non-idempotent](../glossary.md#idempotency), which should be taken into consideration during data [restatement](../plans.md#restatement-plans).

### Materialization strategy
Depending on the target engine, models of the `INCREMENTAL_BY_UNIQUE_KEY` kind are materialized using the following strategies:

| Engine     | Strategy            |
|------------|---------------------|
| Spark      | not supported       |
| Databricks | MERGE ON unique key |
| Snowflake  | MERGE ON unique key |
| BigQuery   | MERGE ON unique key |
| Redshift   | MERGE ON unique key |
| Postgres   | MERGE ON unique key |
| DuckDB     | not supported       |

## FULL
Models of the `FULL` kind cause the dataset associated with a model to be fully refreshed (rewritten) upon each model evaluation.

The `FULL` model kind is somewhat easier to use than incremental kinds due to the lack of special settings or additional query considerations. This makes it suitable for smaller datasets, where recomputing data from scratch is relatively cheap and doesn't require preservation of processing history. However, using this kind with datasets containing a large volume of records will result in significant runtime and compute costs.

This kind can be a good fit for aggregate tables that lack a temporal dimension. For aggregate tables with a temporal dimension, consider the [INCREMENTAL_BY_TIME_RANGE](#incremental_by_time_range) kind instead.

This example specifies a `FULL` model kind:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.salary_by_title_agg,
  kind FULL
);

SELECT
  title,
  AVG(salary)
FROM db.employees
GROUP BY title;
```

### Materialization strategy
Depending on the target engine, models of the `FULL` kind are materialized using the following strategies:

| Engine     | Strategy                         |
|------------|----------------------------------|
| Spark      | INSERT OVERWRITE                 |
| Databricks | INSERT OVERWRITE                 |
| Snowflake  | CREATE OR REPLACE TABLE          |
| BigQuery   | CREATE OR REPLACE TABLE          |
| Redshift   | DROP TABLE, CREATE TABLE, INSERT |
| Postgres   | DROP TABLE, CREATE TABLE, INSERT |
| DuckDB     | CREATE OR REPLACE TABLE          |

## VIEW
The model kinds described so far cause the output of a model query to be materialized and stored in a physical table.

The `VIEW` kind is different, because no data is actually written during model execution. Instead, a non-materialized view (or "virtual table") is created or replaced based on the model's query.

**Note:** View is the default model kind if kind is not specified.

**Note:** With this kind, the model's query is evaluated every time the model is referenced in a downstream query. This may incur undesirable compute cost and time in cases where the model's query is compute-intensive, or when the model is referenced in many downstream queries.

This example specifies a `VIEW` model kind:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.highest_salary,
  kind VIEW
);

SELECT
  MAX(salary)
FROM db.employees;
```

### Materialized Views
The `VIEW` model kind can be configured to represent a materialized view by setting the `materialized` flag to `true`:
```sql linenums="1" hl_lines="4"
MODEL (
  name db.highest_salary,
  kind VIEW (
    materialized true
  )
);
```

**Note:** This flag only applies to engines that support materialized views and is ignored by other engines. Supported engines include:

* Snowflake
* BigQuery
* Redshift
* Postgres

During the evaluation of a model of this kind, the view will be replaced or recreated only if the model's query rendered during evaluation does not match the query used during the previous view creation for this model, or if the target view does not exist. Thus, views are recreated only when necessary in order to realize all the benefits provided by materialized views.

## EMBEDDED
Embedded models are a way to share common logic between different models of other kinds.

There are no data assets (tables or views) associated with `EMBEDDED` models in the data warehouse. Instead, an `EMBEDDED` model's query is injected directly into the query of each downstream model that references it.

This example specifies a `EMBEDDED` model kind:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.unique_employees,
  kind EMBEDDED
);

SELECT DISTINCT
  name
FROM db.employees;
```

## SEED
The `SEED` model kind is used to specify [seed models](./seed_models.md) for using static CSV datasets in your SQLMesh project.

## EXTERNAL
The EXTERNAL model kind is used to specify [external models](./external_models.md) that store metadata about external tables. External models are special; they are not specified in .sql files like the other model kinds. They are optional but useful for propagating column and type information for external tables queried in your SQLMesh project.
