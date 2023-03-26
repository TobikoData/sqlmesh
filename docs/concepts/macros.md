# Macros

Although SQL is not dynamic, data pipelines need some form of dynamicism in order to be useful. For example, you may want a SQL query that runs the same logic except for a filter on dates that should change with every invocation.

```sql linenums="1"
SELECT *
FROM table
WHERE ds BETWEEN @start_ds and @end_ds
```

The syntax for the parameters `@start_ds` and `@end_ds` are passed in at run time, allowing a SQL query to take arguments.

## Variables
The most common use case for dynamicism is simple variable substitution. You may want to swap out a constant in a filter at runtime. Variables can be referenced in Models by using the `@` symbol. They are type aware and can be used naturally in your SQL.

```sql linenums="1"
SELECT *
FROM table
WHERE ds BETWEEN @start_ds and @end_ds
    AND epoch BETWEEN @start_epoch and @end_epoch
-- with the variables
-- start_ds: '2022-01-01, end_ds: '2022-01-02'
-- start_epoch: 1640995200.0, end_epoch: 1641081600.0
-- translates into
WHERE ds BETWEEN '2022-01-01' and end_ds '2022-01-02'
    AND epoch BETWEEN 1640995200.0 AND 1641081600.0
```

## Predefined Variables
SQLMesh comes with predefined variables that can be used in your queries. They are automatically set by the SQLMesh runtime. These variables are related to time and are comprised of a combination of prefixes (start, end, latest) and postfixes (date, ds, ts, epoch, millis).

Prefixes:

* start - The inclusive starting interval of a model run.
* end - The inclusive end interval of a model run.
* latest - The latest date that SQLMesh has run for.

Postfixes:

* date - A python date object that converts into a native SQL Date.
* ds - A date string with the format: '%Y-%m-%d'
* ts - An ISO 8601 datetime formatted string: '%Y-%m-%d %H:%M:%S'.
* epoch - An integer representing seconds since epoch.
* millis - An integer representing milliseconds since epoch.

Variables:

* date
    * @start_date
    * @end_date
    * @latest_date

* ds
    * @start_ds
    * @end_ds
    * @latest_ds

* ts
    * @start_ts
    * @end_ts
    * @latest_ts

* epoch
    * @start_epoch
    * @end_epoch
    * @latest_epoch

* millis
    * @start_millis
    * @end_millis
    * @latest_millis

## Jinja
[Jinja](https://jinja.palletsprojects.com/en/3.1.x/) is a popular templating tool for creating dynamic SQL and is supported by SQLMesh, but there are some drawbacks which lead for us to create our own macro system.

* Jinja is not valid SQL and not parseable.
```sql linenums="1"
-- templating allows for arbitrary string replacements which is not feasible to parse
SE{{ 'lect' }} x {{ 'AS ' + var }}
FROM {{ 'table CROSS JOIN z' }}
```

* Jinja is verbose and difficult to debug.
```sql linenums="1"
TBD example with multiple for loops with trailing or leading comma
```
* No concepts of types. Easy to miss quotes.

```sql linenums="1"
SELECT *
FROM table
WHERE ds BETWEEN '{{ start_ds }}' and '{{ end_ds }}'  -- quotes are needed
WHERE ds BETWEEN {{ start_ds }} and {{ end_ds }}  -- error because ds is a string
```
