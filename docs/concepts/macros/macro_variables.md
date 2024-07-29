# Macro variables

Macro variables are placeholders whose values are substituted in when the macro is rendered.

They enable dynamic macro behavior - for example, a date parameter's value might be based on when the macro was run.

!!! note

    This page discusses SQLMesh's built-in macro variables. Learn more about custom, user-defined macro variables on the [SQLMesh macros page](./sqlmesh_macros.md#user-defined-variables).

## Example

Consider a SQL query that filters by date in the `WHERE` clause.

Instead of manually changing the date each time the model is run, you can use a macro variable to make the date dynamic. With the dynamic approach, the date changes automatically based on when the query is run.

This query filters for rows where column `my_date` is after '2023-01-01':

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

Most predefined variables are related to time and use a combination of prefixes (start, end, etc.) and postfixes (date, ds, ts, etc.). They are described in the next section; [other predefined variables](#runtime-variables) are discussed in the following section.

### Temporal variables

SQLMesh uses the python [datetime module](https://docs.python.org/3/library/datetime.html) for handling dates and times. It uses the standard [Unix epoch](https://en.wikipedia.org/wiki/Unix_time) start of 1970-01-01.

*All predefined variables with a time component use the [UTC time zone](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).*

Prefixes:

* start - The inclusive starting interval of a model run
* end - The inclusive end interval of a model run
* execution - The timestamp of when the execution started

Postfixes:

* dt - A python datetime object that converts into a native SQL `TIMESTAMP` (or SQL engine equivalent)
* date - A python date object that converts into a native SQL `DATE`
* ds - A date string with the format: '%Y-%m-%d'
* ts - An ISO 8601 datetime formatted string: '%Y-%m-%d %H:%M:%S'
* tstz - An ISO 8601 datetime formatted string with timezone: '%Y-%m-%d %H:%M:%S%z'
* hour - An integer representing the hour of the day, with values 0-23
* epoch - An integer representing seconds since Unix epoch
* millis - An integer representing milliseconds since Unix epoch

All predefined temporal macro variables:

* dt
    * @start_dt
    * @end_dt
    * @execution_dt

* date
    * @start_date
    * @end_date
    * @execution_date

* ds
    * @start_ds
    * @end_ds
    * @execution_ds

* ts
    * @start_ts
    * @end_ts
    * @execution_ts

* tstz
    * @start_tstz
    * @end_tstz
    * @execution_tstz

* hour
    * @start_hour
    * @end_hour
    * @execution_hour

* epoch
    * @start_epoch
    * @end_epoch
    * @execution_epoch

* millis
    * @start_millis
    * @end_millis
    * @execution_millis

### Runtime variables

SQLMesh provides two other predefined variables used to modify model behavior based on information available at runtime.

* @runtime_stage - A string value denoting the current stage of the SQLMesh runtime. Typically used in models to conditionally execute pre/post-statements (learn more [here](../models/sql_models.md#optional-prepost-statements)). It returns one of these values:
    * 'loading' - The project is being loaded into SQLMesh's runtime context.
    * 'creating' - The model tables are being created.
    * 'evaluating' - The model query logic is being evaluated.
    * 'testing' - The model query logic is being evaluated in the context of a unit test.
* @gateway - A string value containing the name of the current [gateway](../../guides/connections.md).

### Audit-only variables

Some predefined variables are only supported in [SQLMesh audit definitions](../audits.md).

* @this_model - used to create [generic audits](../audits.md#generic-audits)

The `{{ this_model }}` Jinja macro variable may be used in model definitions for the rare cases when SQLGlot cannot fully parse a statement and you need to reference the model's underlying physical table directly. We recommend against using it unless absolutely required.
