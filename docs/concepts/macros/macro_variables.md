# Macro variables

The most common use case for macros is variable substitution. For example, you might have a SQL query that filters by date in the `WHERE` clause.

Instead of manually changing the date each time the model is run, you can use a macro variable to make the date dynamic. With the dynamic approach, the date changes automatically based on when the query is run.

Consider this query that filters for rows where column `my_date` is after '2023-01-01':

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > '2023-01-01'
```

To make this query's date dynamic you could use the predefined SQLMesh macro variable `@execution_ds`:

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > @execution_ds
```

The `@` symbol tells SQLMesh that `@execution_ds` is a macro variable that requires substitution before the SQL is executed.

The macro variable `@execution_ds` is predefined, so its value will be automatically set by SQLMesh based on when the execution started. If the model was executed on February 1, 2023 the rendered query would be:

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > '2023-02-01'
```

This example used one of SQLMesh's predefined variables, but you can also define your own macro variables.

We describe SQLMesh's predefined variables below; user-defined macro variables are discussed in the [SQLMesh macros](./sqlmesh_macros.md#user-defined-variables) and [Jinja macros](./jinja_macros.md#user-defined-variables) pages.

## Predefined Variables
SQLMesh comes with predefined variables that can be used in your queries. They are automatically set by the SQLMesh runtime.

These variables are related to time and comprise a combination of prefixes (start, end, execution) and postfixes (date, ds, ts, epoch, millis).

SQLMesh uses the python [datetime module](https://docs.python.org/3/library/datetime.html) for handling dates and times. It uses the standard [Unix epoch](https://en.wikipedia.org/wiki/Unix_time) start of 1970-01-01. *All predefined variables with a time component use the [UTC time zone](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).*

Prefixes:

* start - The inclusive starting interval of a model run.
* end - The inclusive end interval of a model run.
* execution - The timestamp of when the execution started.

Postfixes:

* date - A python date object that converts into a native SQL Date.
* ds - A date string with the format: '%Y-%m-%d'
* ts - An ISO 8601 datetime formatted string: '%Y-%m-%d %H:%M:%S'.
* epoch - An integer representing seconds since Unix epoch.
* millis - An integer representing milliseconds since Unix epoch.

All predefined macro variables:

* date
    * @start_date
    * @end_date
    * @execution_date

* datetime
    * @start_dt
    * @end_dt
    * @execution_dt

* ds
    * @start_ds
    * @end_ds
    * @execution_ds

* ts
    * @start_ts
    * @end_ts
    * @execution_ts

* epoch
    * @start_epoch
    * @end_epoch
    * @execution_epoch

* millis
    * @start_millis
    * @end_millis
    * @execution_millis
