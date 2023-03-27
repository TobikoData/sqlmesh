# Comparisons

**This documentation is a work in progress.**

There are many tools and frameworks in the data ecosystem. This page tries to make sense of it all.

## dbt
[dbt](https://www.getdbt.com/) is a tool for data transformations. It is a pioneer in this space and has shown how valuable transformation frameworks can be. Although dbt is a fanstastic tool, it has trouble scaling with data and organizational size.

dbt initially built their product focused on smaller companies with small data. These companies are able to fully refresh their entire warehouse every run, which usually ensures correctness. Fully refreshing your entire warehouse is not cost effective or feasible at many companies. Incremental models in dbt are considered an "advanced" feature and leave a lot of room for error. SQLMesh actively encourages incremental modeling and aims to make it as easy as full refresh.

dbt has been trying to grow beyond its small scale and simple data transformation foundation but these attempts result in many “advanced” features that are difficult to implement and pushes the burden of accuracy and efficiency onto the users.

SQLMesh aims to be dbt format-compatible. Importing existing dbt projects with minor changes is in development.

### Feature comparisons
| Feature                           | dbt | SQLMesh
| -------                           | --- | -------
| `SQL models`                      | ✅ | ✅
| `Python models`                   | ✅ | ✅
| `Seed models`                     | ✅ | ✅
| `Jinja support`                   | ✅ | ✅
| `Views / Embedded models`         | ✅ | ✅
| `Incremental models`              | ✅ | ✅
| `Seed models`                     | ✅ | ✅
| `Snapshot models`                 | ✅ | ❌
| `Documentation generation`        | ✅ | ❌
| `Package manager`                 | ✅ | ❌
| `Semantic validation`             | ❌ | ✅
| `Transpilation`                   | ❌ | ✅
| `Unit tests`                      | ❌ | ✅
| `Column level lineage`            | ❌ | ✅
| `Accessible incremental models`   | ❌ | ✅
| `Downstream impact planner`       | ❌ | ✅
| `Change categorization`           | ❌ | ✅
| `Native Airflow integration`      | ❌ | ✅
| `Date leakage protection`         | ❌ | ✅
| `Data gap detection/repair`       | ❌ | ✅
| `Batched backfills`               | ❌ | ✅
| `Table reuse across environments` | ❌ | ✅
| `Local Python execution`          | ❌ | ✅
| `Open-source CI/CD Bot`           | ❌ | ✅
| `Open-source IDE (UI)`            | ❌ | ✅
| `CLI`                             | ✅ | ✅
| `Notebook Support`                | ❌ | ✅
| `Comprehensive Python API`        | ❌ | ✅

### Environments
Development and staging environments in DBT are expensive to make and not fully representative of what will go into production.

The usual flow for creating a new environment in dbt is to re-run your entire warehouse in a new environment. This may work at small scales, but even if it does, it's a waste of time and money. SQLMesh is able to provide efficient isolated environments with [Virtual Data Marts](concepts/plans.md#plan-application). Creating a development environment in SQLMesh is free -- you can quickly get a full replica of any other environment with a simple command. Environments in dbt cost compute and storage.

Additionally, SQLMesh ensures that promotion of staging environments to produciton is predictable and consistent. Promotions are simple pointer swaps meaning there is again no wasted compute. There is no concept of promotion in dbt, and queries are all rerun when it's time to deploy something.

### Incremental models
Implementing an incremental model is difficult and error-prone in dbt, because dbt does not keep track of state. Since there is no state in dbt, the user must write subqueries to find missing date boundaries.

#### Complexity
```sql
-- dbt incremental
SELECT *
FROM raw.events e
JOIN raw.event_dims d
  ON e.id = d.id
-- must specify the is_incremental flag because this predicate will fail if the model has never run before
{% if is_incremental() %}
    -- this filter dynamically scans the current model to find the date boundary
    AND d.ds >= (SELECT MAX(ds) FROM {{ this }})
{% endif %}
{% if is_incremental() %}
WHERE e.ds >= (SELECT MAX(ds) FROM {{ this }})
{% endif %}
```

Having to manually specify macros to find date boundaries is repetitive and error-prone. As incremental models become more complex, the cognitive burden of having two run times, "first time full refresh" vs. "subsequent incremental", increases.

SQLMesh keeps track of which date ranges exist so that the query can be simplified as follows:

```sql
-- sqlmesh incremental
SELECT *
FROM raw.events
JOIN raw.event_dims d
  -- date ranges are handled automatically by sqlmesh
  ON e.id = d.id AND d.ds BETWEEN @start_ds AND @end_ds
WHERE d.ds BETWEEN @start_ds AND @end_ds
```

#### Data leakage
dbt does not enforce that the data inserted into the incremental table should be there. This can lead to problems or consistency issues, such as late-arriving data overriding past partitions. SQLMesh wraps all queries under the hood in a subquery with a time filter to enforce that the data inserted for a particular batch is as expected.

dbt also only supports the 'insert/overwrite' incremental load pattern for systems that natively support it. SQLMesh enables 'insert/overwrite' on any system, because it is the most robust way to do incremental pipelines. 'Append' pipelines risk data accuracy in the variety of scenarios where your pipelines may run more than once for a given date.


```sql
-- original query
SELECT *
FROM raw.events
JOIN raw.event_dims d
  ON e.id = d.id AND d.ds BETWEEN @start_ds AND @end_ds
WHERE d.ds BETWEEN @start_ds AND @end_ds

-- with data leakage guard
SELECT *
FROM (
  SELECT *
  FROM raw.events
  JOIN raw.event_dims d
    ON e.id = d.id AND d.ds BETWEEN @start_ds AND @end_ds
  WHERE d.ds BETWEEN @start_ds AND @end_ds
)
WHERE ds BETWEEN @start_ds AND @end_ds
```

#### Data gaps
The main pattern used in incremental models checks for MAX(ds). This pattern does not catch missing data from the past, or data gaps.

```
Expected dates: 2022-01-01, 2022-01-02, 2022-01-03
Missing past data: ?, 2022-01-02, 2022-01-03
Data gap: 2022-01-01, ?, 2022-01-03
```

SQLMesh stores each date interval a model has been run with, so it knows exactly what dates are missing.

#### Performance
The subqueries that look for MAX(date) could have a performance impact on the query. SQLMesh is able to avoid these extra subqueries.

Additionally, dbt expects an incremental model to be able to fully refresh the first time it runs. For some large scale data sets, this is cost prohibitive or infeasible. SQLMesh is able to [batch](../concepts/models/overview#batch_size) up backfills into more manageable chunks.

### SQL understanding
dbt heavily relies on [Jinja](https://jinja.palletsprojects.com/en/3.1.x/). It has no understanding of SQL and treats all queries as raw strings with no context. This means that simple syntax errors (like a trailing comma) are difficult to debug and require a full run to detect.

Although SQLMesh supports Jinja, it does not rely on it and parses/understands SQL through [SQLGlot](https://github.com/tobymao/sqlglot). Simple errors can be detected at compile time. You no longer have to wait minutes to see that you've referenced a column incorrectly or missed a comma.

Additionally, having a first-class understanding of SQL allows for SQLMesh to do some interesting things, like transpilation, column-level lineage, and automatic change categorization.

### Testing
dbt calls data quality checks testing. Although data quality checks are extremely valuable, they are not sufficient for creating robust data pipelines. Data qualiy checks are great for detecting upstream data issues and large scale problems like nulls and duplicates. But they are not meant for testing edge cases or business logic.

[Unit and integration tests](concepts/tests.md) are the tools to use to validate business logic. SQLMesh encourages users to add unit tests to all of their models to ensure changes don't unexpectedly break assumptions. Unit tests are designed to be fast and self contained so that they can run in CI.
