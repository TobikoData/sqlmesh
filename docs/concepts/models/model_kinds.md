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

SQLMesh also uses the time column to automatically append a time range filter to the model's query at runtime, which prevents records that are not part of the target interval from being stored. This is a safety mechanism that prevents unintentionally overwriting unrelated records when handling late-arriving data.

The required filter you write in the model query's `WHERE` clause filters the **input** data as it is read from upstream tables, reducing the amount of data processed by the model. The automatically appended time range filter is applied to the model query's **output** data to prevent data leakage.

Consider the following model definition, which specifies a `WHERE` clause filter with the `receipt_date` column. The model's `time_column` is a different column `event_date`, whose filter is automatically added to the model query. This approach is useful when an upstream model's time column is different from the model's time column:

```sql linenums="1"
MODEL (
  name db.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date -- `event_date` is model's time column
  )
);

SELECT
  event_date::TEXT as event_date,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  receipt_date BETWEEN @start_ds AND @end_ds; -- User-supplied `receipt_date` filter
```

At runtime, SQLMesh will automatically modify the model's query to look like this:
```sql linenums="1" hl_lines="7"
SELECT
  event_date::TEXT as event_date,
  event_payload::TEXT as payload
FROM raw_events
WHERE
  receipt_date BETWEEN @start_ds AND @end_ds
  AND event_date BETWEEN @start_ds AND @end_ds; -- `event_date` time column filter automatically added
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

**Note:** `VIEW` is the default model kind if kind is not specified.

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

* BigQuery
* Databricks
* Postgres
* Snowflake

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

## SCD Type 2

SCD Type 2 is a model kind that supports [slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) (SCDs) in your SQLMesh project. SCDs are a common pattern in data warehousing that allow you to track changes to records over time.

SQLMesh achieves this by adding a `valid_from` and `valid_to` column to your model. The `valid_from` column is the timestamp that the record became valid (inclusive) and the `valid_to` column is the timestamp that the record became invalid (exclusive). The `valid_to` column is set to `NULL` for the latest record.

Therefore you can use these models to not only tell you what the latest value is for a given record but also what the values were anytime in the past. Note that maintaining this history does come at a cost of increased storage and compute and this may not be a good fit for sources that change frequently since the history could get very large.

Currently SCD Type 2 only supports sourcing from tables that have an "Updated At" timestamp defined in the table that tells you when a given was last updated. Soon we will also be supporting checking column values in cases where an update column is not available.

This example specifies a `SCD_TYPE_2` model kind:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.menu_items,
  kind SCD_TYPE_2 (
    unique_key id,
  )
);

SELECT
  id::INT,
  name::STRING,
  price::DOUBLE,
  updated_at::TIMESTAMP
FROM
  stg.current_menu_items;
```

SQLMesh will materialize this table with the following structure:
```sql linenums="1" hl_lines="3"
TABLE db.menu_items (
  id INT,
  name STRING,
  price DOUBLE,
  updated_at TIMESTAMP,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP
);
```

### Column Names
SQLMesh will automatically add the `valid_from` and `valid_to` columns to your table. If you would like to specify the names of these columns you can do so by adding the following to your model definition:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.menu_items,
  kind SCD_TYPE_2 (
    unique_key id,
    valid_from_name my_valid_from,
    valid_to_name my_valid_to
  )
);
```

SQLMesh will materialize this table with the following structure:
```sql linenums="1" hl_lines="3"
TABLE db.menu_items (
  id INT,
  name STRING,
  price DOUBLE,
  updated_at TIMESTAMP,
  my_valid_from TIMESTAMP,
  my_valid_to TIMESTAMP
);
```

The `updated_at` column name can also be changed by adding the following to your model definition:
```sql linenums="1" hl_lines="3"
MODEL (
  name db.menu_items,
  kind SCD_TYPE_2 (
    unique_key id,
    updated_at_name my_updated_at
  )
);

SELECT
    id,
    name,
    price,
    my_updated_at
FROM
    stg.current_menu_items;
```

SQLMesh will materialize this table with the following structure:
```sql linenums="1" hl_lines="3"
TABLE db.menu_items (
  id INT,
  name STRING,
  price DOUBLE,
  my_updated_at TIMESTAMP,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP
);
```

### Deletes

Deletes are supported and when an item is detected as deleted then the `valid_to` column will be set to the time when SQLMesh started running (called `execution_time`). If a deleted column is added back in the source then it will be inserted back into the table with `valid_from` set to the largest of either the `updated_at` timestamp of the new record or the `valid_from` timestamp of the deleted record in the SCD Type 2 table.

### Example of SCD Type 2 in Action

Lets say that you started with the following data in your source table:

| ID | Name             | Price |     Updated At      |
|----|------------------|:-----:|:-------------------:|
| 1  | Chicken Sandwich | 10.99 | 2020-01-01 00:00:00 |
| 2  | Cheeseburger     | 8.99  | 2020-01-01 00:00:00 |
| 3  | French Fries     | 4.99  | 2020-01-01 00:00:00 |

The target table, which is currently empty, will be materialized with the following data:

| ID | Name             | Price |     Updated At      |     Valid From      | Valid To |
|----|------------------|:-----:|:-------------------:|:-------------------:|:--------:|
| 1  | Chicken Sandwich | 10.99 | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 |   NULL   |
| 2  | Cheeseburger     | 8.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 |   NULL   |
| 3  | French Fries     | 4.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 |   NULL   |

Now lets say that you update the source table with the following data:

| ID | Name             | Price |     Updated At      |
|----|------------------|:-----:|:-------------------:|
| 1  | Chicken Sandwich | 12.99 | 2020-01-02 00:00:00 |
| 3  | French Fries     | 4.99  | 2020-01-01 00:00:00 |
| 4  | Milkshake        | 3.99  | 2020-01-02 00:00:00 |

Summary of Changes:

* The price of the Chicken Sandwich was increased from $10.99 to $12.99.
* Cheeseburger was removed from the menu.
* Milkshakes were added to the menu.

Assuming your pipeline ran at `2020-01-02 02:00:00`, target table will be updated with the following data:

| ID | Name             | Price |     Updated At      |     Valid From      |      Valid To       |
|----|------------------|:-----:|:-------------------:|:-------------------:|:-------------------:|
| 1  | Chicken Sandwich | 10.99 | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 | 2020-01-02 00:00:00 |
| 1  | Chicken Sandwich | 12.99 | 2020-01-02 00:00:00 | 2020-01-02 00:00:00 |        NULL         |
| 2  | Cheeseburger     | 8.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 | 2020-01-02 02:00:00 |
| 3  | French Fries     | 4.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 |        NULL         |
| 4  | Milkshake        | 3.99  | 2020-01-02 00:00:00 | 2020-01-02 00:00:00 |        NULL         |

For our final pass, lets say that you update the source table with the following data:

| ID | Name                | Price |     Updated At      |
|----|---------------------|:-----:|:-------------------:|
| 1  | Chicken Sandwich    | 14.99 | 2020-01-03 00:00:00 |
| 2  | Cheeseburger        | 8.99  | 2020-01-03 00:00:00 |
| 3  | French Fries        | 4.99  | 2020-01-01 00:00:00 |
| 4  | Chocolate Milkshake | 3.99  | 2020-01-02 00:00:00 |

Summary of changes:

* The price of the Chicken Sandwich was increased from $12.99 to $14.99 (must be good!)
* Cheeseburger was added back to the menu with original name and price.
* Milkshake name was updated to be "Chocolate Milkshake".

Target table will be updated with the following data:

| ID | Name                | Price |     Updated At      |     Valid From      |      Valid To       |
|----|---------------------|:-----:|:-------------------:|:-------------------:|:-------------------:|
| 1  | Chicken Sandwich    | 10.99 | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 | 2020-01-02 00:00:00 |
| 1  | Chicken Sandwich    | 12.99 | 2020-01-02 00:00:00 | 2020-01-02 00:00:00 | 2020-01-03 00:00:00 |
| 1  | Chicken Sandwich    | 14.99 | 2020-01-03 00:00:00 | 2020-01-03 00:00:00 |        NULL         |
| 2  | Cheeseburger        | 8.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 | 2020-01-02 02:00:00 |
| 2  | Cheeseburger        | 8.99  | 2020-01-03 00:00:00 | 2020-01-03 00:00:00 |        NULL         |
| 3  | French Fries        | 4.99  | 2020-01-01 00:00:00 | 1970-01-01 00:00:00 |        NULL         |
| 4  | Milkshake           | 3.99  | 2020-01-02 00:00:00 | 2020-01-02 00:00:00 | 2020-01-03 00:00:00 |
| 4  | Chocolate Milkshake | 3.99  | 2020-01-03 00:00:00 | 2020-01-03 00:00:00 |        NULL         |

**Note:** `Cheeseburger` was deleted from `2020-01-02 02:00:00` to `2020-01-03 00:00:00` meaning if you queried the table during that time range then you would not see `Cheeseburger` in the menu. This is the most accurate representation of the menu based on the source data provided. If `Cheeseburger` were added back to the menu with it's original updated at timestamp of `2020-01-01 00:00:00` then the `valid_from` timestamp of the new record would have been `2020-01-02 02:00:00` resulting in no period of time where the item was deleted. Since in this case the updated at timestamp did not change it is likely the item was removed in error and this again most accurately represents the menu based on the source data.

### Querying SCD Type 2 Models

#### Querying the current version of a record

Although SCD Type 2 models support history, it is still very easy to query for just the latest version of a record. Simply query the model as you would any other table. For example, if you wanted to query the latest version of the `menu_items` table you would simply run:

```sql linenums="1" hl_lines="3"
SELECT
    *
FROM
    menu_items
WHERE
    valid_to IS NULL;
```

One could also create a view on top of the SCD Type 2 model that creates a new `is_current` column to make it easy for consumers to identify the current record.

```sql linenums="1" hl_lines="3"
SELECT
    *,
    valid_to IS NULL AS is_current
FROM
    menu_items;
```

#### Querying for a specific version of a record at a give point in time

If you wanted to query the `menu_items` table as it was on `2020-01-02 01:00:00` you would simply run:

```sql linenums="1" hl_lines="3"
SELECT
    *
FROM
    menu_items
WHERE
    id = 1
    AND '2020-01-02 01:00:00' >= valid_from
    AND '2020-01-02 01:00:00' < COALESCE(valid_to, CAST('2199-12-31 23:59:59+00:00' AS TIMESTAMP));
```

Example in a join:

```sql linenums="1" hl_lines="3"
SELECT
    *
FROM
    orders
    INNER JOIN
    menu_items
    ON orders.menu_item_id = menu_items.id
    AND orders.created_at >= menu_items.valid_from
    AND orders.created_at < COALESCE(menu_items.valid_to, CAST('2199-12-31 23:59:59+00:00' AS TIMESTAMP));
```

A view can be created to do the `COALESCE` automatically. This, combined with the `is_current` flag, makes it easier to query for a specific version of a record.

```sql linenums="1" hl_lines="3"
SELECT
    id,
    name,
    price,
    updated_at,
    valid_from,
    COALESCE(valid_to, CAST('2199-12-31 23:59:59+00:00' AS TIMESTAMP)) AS valid_to
    valid_to IS NULL AS is_current,
FROM
    menu_items;
```

Furthermore if you want to make it so users can use `BETWEEN` when querying by making `valid_to` inclusive you can do the following:
```sql linenums="1" hl_lines="3"
SELECT
    id,
    name,
    price,
    updated_at,
    valid_from,
    COALESCE(valid_to, CAST('2200-01-01 00:00:00+00:00' AS TIMESTAMP)) - INTERVAL 1 SECOND AS valid_to
    valid_to IS NULL AS is_current,
```

Note: The precision of the timestamps in this example is second so I subtract 1 second. Make sure to subtract a value equal to the precision of your timestamps.

#### Querying for deleted records

One way to identify deleted records is to query for records that do not have a `valid_to` record of `NULL`. For example, if you wanted to query for all deleted ids in the `menu_items` table you would simply run:

```sql linenums="1" hl_lines="3"
SELECT
    id,
    MAX(CASE WHEN valid_to IS NULL THEN 0 ELSE 1 END) AS is_deleted
FROM
    menu_items
GROUP BY
    id
```

### Limitations
* Currently SCD Type 2 requires an update column to be defined on the table you are sourcing your data from. Soon we will also be supporting checking column values in cases where an update column is not available.

## EXTERNAL
The EXTERNAL model kind is used to specify [external models](./external_models.md) that store metadata about external tables. External models are special; they are not specified in .sql files like the other model kinds. They are optional but useful for propagating column and type information for external tables queried in your SQLMesh project.
