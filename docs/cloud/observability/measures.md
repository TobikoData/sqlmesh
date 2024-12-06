# Measures

Tobiko Cloud automatically captures and stores information about all SQLMesh actions. It summarizes that information into "measures" that are displayed in charts and tables.

We now briefly review the SQLMesh workflow before describing the different measures Tobiko Cloud captures.

## SQLMesh workflow

The core of a SQLMesh project is its **models**. Roughly, each model consists of one SQL query and metadata that tells SQLMesh how the model should be processed.

Each model may have **audits** that validate the data returned by a model (e.g., verifying that a column contains no `NULL` values). By default, SQLMesh will stop running a project if an audit fails.

When you run a project on a SQL engine, you must choose an **environment** in which to run it. Environments allow people to modify projects in an isolated space that won't interfere with anyone else (or the version of the project running in production).

SQLMesh stores a unique fingerprint of the project's contents on each run. THat way it can determine if any of that content has changed the next time you run it in that environment.

When a project's content has changed, an environment is updated to reflect those changes with a SQLMesh **plan**. The plan identifies all the changes and determines which data will be affected by them so it only has to re-run the relevant models.

After changes have been applied with a plan, the project is **run** on a schedule to process new data that has arrived since the previous run.

The five entities in bold - **models, audits, environments, runs, and plans** - provide the information Tobiko Cloud captures to help you efficiently identify and remediate problems with your transformation pipeline.

## Built-in measures

We now describe the built-in measures SQLMesh automatically captures about each entity.

SQLMesh performs its primary actions during **plans** and **runs**, so most measures are generated when they occur. Both plans and runs are executed in a specific **environment**, so all of their measures are environment-specific.

These measures are recorded and stored for each plan or run in a specific environment:

- When it began and ended
- Total run time
- Whether it failed
- Whether and how any model audits failed
- The model versions evaluated during the plan/run
- Each model's run time

These measures are displayed in the relevant Tobiko Cloud interface for the underlying SQLMesh action (e.g., **run** measures are in the **run** interfaces).

## Custom measures

Tobiko Cloud allows you to calculate and track custom measures in addition to the ones it [automatically calculates](#built-in-measures).

### Definition

Each custom measure is associated with a model and is defined by a SQL query in that model's file.

The `@measure` macro is used to define custom measures. The body of the `@measure` macro is the query, and each column in the query defines a separate measure.

A measure's name is the name of the column that defined it. Measure names must be unique within a model, but a name may be used in multiple models.

A model may contain more than one `@measure` macro specification. The `@measure` macros must be specified after the model's primary query. They will be executed during a SQLMesh `plan` or `run` after the primary model query is executed.

This example shows a model definition that includes a measure query defining two measures: `row_count` (the total number of rows in the table) and `num_col_avg` (the average value of the model's `numeric_col` column).

```sql
MODEL (
  name custom_measure.example,
  kind FULL
);

SELECT
  numeric_col
FROM
  custom_measure.upstream;

@measure( -- Measure query specified in the `@measure` macro
  SELECT
    COUNT(*) AS row_count, -- Table's row count
    AVG(numeric_col) AS num_col_avg -- Average value of `numeric_col`
  FROM custom_measure.example -- Select FROM the name of the model
);
```

Every time the `custom_measure.example` model is executed, Tobiko Cloud will execute the measure query and store the value it returns.

By default, the measure's timestamp will be the execution time of the `plan`/`run` that captured it. [Incremental by time range](../../concepts/models/model_kinds.md#incremental_by_time_range) models may specify [custom timestamps](#incremental-by-time-models) other than execution time.

#### TODO: is this still true?
An Tobiko Cloud chart allows you to select which measure to display. The chart displays the value of the selected measure on the y-axis and the execution time of the associated `plan`/`run` on the x-axis, allowing you to monitor whether the value has meaningfully changed since the previous execution.

### Incremental by time models

In the previous example, Tobiko Cloud automatically associated each measure value with the execution time of the `plan` or `run` that executed it.

For [incremental by time range models](../../concepts/models/model_kinds.md#incremental_by_time_range), you can customize how measures are timestamped by including your own time column in the measure query.

In the measure definition, the time column must be named `ts`. It may be of any datetime data type (e.g., date string, `DATE`, `TIMESTAMP`, etc.).

Custom measure times are typically derived from a datetime column in the model and are most useful when the measure groups by the datetime.

For example, this incremental model stores the date of each data point in the `event_datestring` column. We could measure each day's row count and numeric column average with this measure query:

```sql
MODEL (
  name custom_measure.incremental_example
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_datestring
  )
);

SELECT
  event_datestring,
  numeric_col
FROM
  custom_measure.upstream
WHERE
  event_datestring BETWEEN @start_ds AND @end_ds;

@measure(
  SELECT
    event_datestring AS ts, -- Custom measure time column `ts`
    COUNT(*) AS daily_row_count, -- Daily row count
    AVG(numeric_col) AS daily_num_col_avg -- Daily average value of `numeric_col`
  FROM custom_measure.incremental_example
  WHERE event_datestring BETWEEN @start_ds AND @end_ds -- Filter measure on time
  GROUP BY event_datestring -- Group measure by time
);
```

The measure query both filters and groups the data based on the model's time column `event_datestring`. The filtering and grouping ensures that only one measure value is ever calculated for a specific day of data.

NOTE: the custom time column approach will not work correctly if the model's [`lookback` argument](../../concepts/models/overview.md#lookback) is specified because a given day's data will be processed every time it is in the lookback window.

### Execution and custom times

A model may contain multiple measure queries, so both execution time and custom time measures may be specified for the same model.

These two measure types help answer different questions:

1. Execution time: has something meaningfully changed **on this `plan`/`run`** compared to previous plans/runs?
2. Custom time: has something meaningfully changed **in a specific time point's data** compared to other time points?

If multiple time points of data are processed during each model execution, an anomaly at a specific time may not be detectable from an execution time measure alone.

Custom time measures enable monitoring at the temporal granularity of the data itself.
