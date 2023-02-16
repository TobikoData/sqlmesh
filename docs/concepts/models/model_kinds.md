# Model kinds

This page describes supported kinds of [models](overview.md) which ultimately determine how the data for a model gets loaded.

## INCREMENTAL_BY_TIME_RANGE

Specifies that the model should be computed incrementally based on a time range. This is a good choice for datasets in which records are of temporal nature and represent immutable facts like events, logs or transactions. Using this kind for datasets that fit the described traits usually results in significant cost and time savings.

As the name suggests a model of this kind is computed incrementally, meaning only missing data intervals are processed during each evaluation. This is in contrast to the [FULL](#full) model kind, which causes the recomputation of the entire dataset every time the model is evaluated.

In order to take advantage of the incremental evaluation, the model query must contain an expression in its `WHERE` clause which filters the upstream records by time range. SQLMesh provides special macros which represent the start and the end of the time range that is being processed: `@start_date` / `@end_date` and `@start_ds` / `@end_ds`. Please refer to [Macros](../macros.md#predefined-variables) to find more information on these.

Below is an example of a definition which takes full advantage of the model's incremental nature:
```sql
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE
);
SELECT
  event_date::TEXT as ds,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  event_date BETWEEN @start_ds AND @end_ds;
```

### Time column
SQLMesh needs to know which column in the model's output represents a timestamp or a date associated with each record. This column is used to determine which records will be overridden during data [restatement](../plans.md#restatement-plans) as well as a partition key for engines that support partitioning (eg. Apache Spark). By default the `ds` column name is used but it can be overridden in the model definition:
```sql
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date_column
  )
);
```

Additionally, the format in which the timestamp/date is stored is required. By default SQLMesh uses the `%Y-%m-%d` format but it can be overridden as follows:
```sql
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (date_column, '%Y-%m-%d')
  )
);
```
Please note that the time format should be defined using the same dialect as the one used to define the model's query.

SQLMesh also uses the time column to automatically append a time range filter to the model's query at runtime which prevents records that are not a part of the target interval from being stored. This is a safety mechanism which prevents the unintended overriding of unrelated records when handling late arriving data.

Consider the following model definition:
```sql
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

At runtime, SQLMesh will automatically modify the model's query to look like following:
```sql
SELECT
  event_date::TEXT as event_date,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  receipt_date BETWEEN @start_ds AND @end_ds
  AND event_date BETWEEN @start_ds AND @end_ds;
```

### Idempotency
It's recommended to ensure that queries of models of this kind are [idempotent](../../glossary/#idempotency) to prevent unexpected results during data [restatement](../plans.md#restatement-plans). Please note, however, that upstream models and tables can impact the extent to which the idempotency property can be guaranteed. For example, referencing an upstream model of kind [FULL](#full) in the model query automatically renders such a model as non-idempotent.

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

This kind signifies that a model should be computed incrementally based on a unique key. If a key is missing in the model's table, the new row is inserted, otherwise the existing row associated with this key is updated with the new one. This kind is a good fit for datasets which have the following traits:

* Each record has a key associated with it.
* There should be at most one record associated with each unique key.
* It's appropriate to upsert records, meaning existing records can be overridden by newly arrived ones when their keys match.

[SCD](https://en.wikipedia.org/wiki/Slowly_changing_dimension) (Slowly Changing Dimensions) is one example that fits this description well.

The name of the unique key column must be provided as part of the model definition as in the following example:
```sql
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
```sql
MODEL (
  name db.employees,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (first_name, last_name)
  )
);
```

Similarly to the [INCREMENTAL_BY_TIME_RANGE](#incremental_by_time_range) kind, the upstream records can be filtered by time range using the `@start_date`, `@end_date`, etc. [macros](../macros.md#predefined-variables) in order to process the input data incrementally:
```sql
SELECT
  name::TEXT as name,
  title::TEXT as title,
  salary::INT as salary
FROM raw_employee_events
WHERE
  event_date BETWEEN @start_date AND @end_date;
```

Note, however, that models of this kind don't support data [restatement](../plans.md#restatement-plans).

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
As the name suggests, this kind causes the dataset associated with a model to be fully refreshed (rewritten) on each model evaluation. It's somewhat easier to use than incremental kinds due to lack of any special settings or additional query considerations. This makes it suitable for smaller datasets, for which recomputing data from scratch is relatively cheap and which don't require preservation of processing history. However, using this kind with datasets which have a high volume of records will result in significant runtime and compute costs.

This kind can be a good fit for aggregate tables that lack temporal dimension. For aggregate tables with temporal dimension consider the [INCREMENTAL_BY_TIME_RANGE](#incremental_by_time_range) kind instead.

Example:
```sql
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
Up until now each model kind caused the output of a model query to be materialized and stored in a physical table. The `VIEW` kind is different because no data actually gets written during model evaluation. Instead a non-materialized view (aka "virtual table") is created or replaced based on the model's query.

Please note that with this kind the model's query is evaluated every time the model gets referenced in downstream queries. This may incur undesirable compute cost in case when the model's query is compute intensive or when the model is referenced in many downstream queries.

Example:
```sql
MODEL (
  name db.highest_salary,
  kind VIEW
);
SELECT
  MAX(salary)
FROM db.employees;
```

## EMBEDDED
This kind is similar to [VIEW](#view), except models of this kind are never evaluated, and therefore, there are no data assets (tables or views) associated with them in the data warehouse. Instead the embedded model's query gets injected directly into a query of each downstream model that references this model in its own query.

Embedded models are a way to share common logic between different models of other kinds.

Example:
```sql
MODEL (
  name db.unique_employees,
  kind EMBEDDED
);
SELECT DISTINCT
  name
FROM db.employees;
```

## SEED
This is a special kind reserved for [seed models](seed_models.md).
