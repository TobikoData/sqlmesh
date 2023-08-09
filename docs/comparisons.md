# Comparisons

**This documentation is a work in progress.**

There are many tools and frameworks in the data ecosystem. This page tries to make sense of it all.

If you are not familiar with SQLMesh, it will be helpful to first read [Why SQLMesh](../#why-sqlmesh) and [What is SQLMesh](../#what-is-sqlmesh) to better understand the comparisons.

## dbt
[dbt](https://www.getdbt.com/) is a tool for data transformations. It is a pioneer in this space and has shown how valuable transformation frameworks can be. Although dbt is a fantastic tool, it has trouble scaling with data and organizational size.

dbt built their product focused on simple data transformations. By default, it fully refreshes data warehouses by executing templated SQL in the correct order.

Over time dbt has seen that data transformations are not enough to operate a scalable and robust data product. As a result, advanced features are patched in, such as state management (defer) and incremental loads, to try to address these needs while pushing the burden of correctness onto users with increased complexity. These "advanced" features make up some of the fundamental building blocks of a DataOps framework.

In other words, the challenge of implementing these features in dbt falls primarily on **you**: more jinja macro blocks, more manual configuration, more custom tooling, and more opportunities for error. We needed an easier, more reliable way, so we designed SQLMesh from the ground up to be a robust DataOps framework.

SQLMesh aims to be dbt format-compatible. Importing existing dbt projects with minor changes is in development.

### Feature comparisons
| Feature                           | dbt | SQLMesh
| -------                           | --- | -------
| Modeling
| `SQL models`                      | ✅ | [✅](../concepts/models/overview)
| `Python models`                   | ✅ | [✅✅](../concepts/models/python_models)
| `Jinja support`                   | ✅ | ✅
| `Jinja macros`                    | ✅ | [✅](../concepts/macros/jinja_macros)
| `Python macros`                   | ❌ | [✅](../concepts/macros/sqlmesh_macros)
| Validation
| `SQL semantic validation`         | ❌ | [✅](../concepts/glossary/#semantic-understanding)
| `Unit tests`                      | ❌ | [✅](../concepts/tests)
| `Table diff`                      | ❌ | ✅
| `Data audits`                     | ✅ | [✅](../concepts/audits)
| `Schema contracts`                | ✅ | [✅](../concepts/plans)
| `Data contracts`                  | ❌ | [✅](../concepts/plans)
| Deployment
| `Virtual Data Environments`       | ❌ | [✅](../concepts/environments)
| `Open-source CI/CD bot`           | ❌ | [✅](../integrations/github)
| `Data consistency enforcement`    | ❌ | ✅
| `Native Airflow integration`      | ❌ | [✅](../integrations/airflow)
| Interfaces
| `CLI`                             | ✅ | [✅](../reference/cli)
| `Paid UI`                         | ✅ | ❌
| `Open-source UI`                  | ❌ | ✅
| `Native Notebook Support`         | ❌ | [✅](../reference/notebook)
| Visualization
| `Documentation generation`        | ✅ | ✅
| `Column-level lineage`            | ❌ | ✅
| Miscellaneous
| `Package manager`                 | ✅ | ❌
| `Multi-repository support`        | ❌ | [✅](../guides/multi_repo)
| `SQL transpilation`               | ❌ | [✅](../concepts/models/sql_models/#transpilation)

### Environments
Development and staging environments in dbt are costly to make and not fully representative of what will go into production.

The standard approach to creating a new environment in dbt is to rerun your entire warehouse in a new environment. This may work at small scales, but even then it wastes time and money. Here's why:

The first part of running a data transformation system is repeatedly iterating through three steps: create or modify model code, execute the models, evaluate the outputs. Practitioners may repeat these steps many times in a day's work.

These steps incur costs to organizations: compute costs to run the models and staff time spent waiting on them to run. Inefficiencies compound rapidly because the steps are repeated so frequently.
dbt's default full refresh approach leads to the most costly version of this loop: recomputing every model every time.

SQLMesh takes another approach. It examines the code modifications and the dependency structure among the models to determine which models are affected -- and executes only those models. This results in the least costly version of the loop: computing only what is required every time through.

This enables SQLMesh to provide efficient isolated [Virtual Environments](./concepts/plans.md#plan-application). Environments in dbt cost compute and storage, but creating a development environment in SQLMesh is free -- you can quickly access a full replica of any other environment with a single command.

Additionally, SQLMesh ensures that promotion of staging environments to production is predictable and consistent. There is no concept of promotion in dbt, so queries are all rerun when it's time to deploy something. In SQLMesh, promotions are simple pointer swaps so there is no wasted compute.

### Incremental models
Implementing incremental models is difficult and error-prone in dbt because it does not keep track of state.

#### Complexity
Since there is no state of which incremental intervals have already run in dbt, users must write and maintain subqueries to find missing date boundaries themselves:

```sql
-- dbt incremental
SELECT *
FROM {{ ref(raw.events) }} e
JOIN {{ ref(raw.event_dims) }} d
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

Manually specifying macros to find date boundaries is repetitive and error-prone.

The example above shows how incremental models behave differently in dbt depending on whether they have been run before. As models become more complex, the cognitive burden of having two run times, "first time full refresh" vs. "subsequent incremental", increases.

SQLMesh keeps track of which date ranges exist, producing a simplified and efficient query as follows:

```sql
-- SQLMesh incremental
SELECT *
FROM raw.events e
JOIN raw.event_dims d
  -- date ranges are handled automatically by SQLMesh
  ON e.id = d.id AND d.ds BETWEEN @start_ds AND @end_ds
WHERE d.ds BETWEEN @start_ds AND @end_ds
```

#### Data leakage
dbt does not check whether the data inserted into an incremental table should be there or not. This can lead to problems and consistency issues, such as late-arriving data overriding past partitions. These problems are called "data leakage."

SQLMesh wraps all queries in a subquery with a time filter under the hood to enforce that the data inserted for a particular batch is as expected and reproducible every time.

In addition, dbt only supports the 'insert/overwrite' incremental load pattern for systems that natively support it. SQLMesh enables 'insert/overwrite' on any system, because it is the most robust approach to incremental loading, while 'Append' pipelines risk data inaccuracy in the variety of scenarios where your pipelines may run more than once for a given date.

This example shows the time filtering subquery SQLMesh applies to all queries as a guard against data leakage:
```sql
-- original query
SELECT *
FROM raw.events
JOIN raw.event_dims d
  ON e.id = d.id AND d.ds BETWEEN @start_ds AND @end_ds
WHERE d.ds BETWEEN @start_ds AND @end_ds

-- with automated data leakage guard
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
The main pattern used to implement incremental models in dbt is checking for the most recent data with MAX(date). This pattern does not catch missing data from the past, or "data gaps."

SQLMesh stores each date interval a model has been run with, so it knows exactly what dates are missing:
```
Expected dates: 2022-01-01, 2022-01-02, 2022-01-03
Missing past data: ?, 2022-01-02, 2022-01-03
Data gap: 2022-01-01, ?, 2022-01-03
```

SQLMesh will automatically fill these data gaps on the next run.

#### Performance
Subqueries that look for MAX(date) could have a performance impact on the primary query. SQLMesh is able to avoid these extra subqueries.

Additionally, dbt expects an incremental model to be able to fully refresh the first time it runs. For some large data sets, this is cost-prohibitive or infeasible.

SQLMesh is able to [batch](../concepts/models/overview#batch_size) up backfills into more manageable chunks.

### SQL understanding
dbt heavily relies on [Jinja](https://jinja.palletsprojects.com/en/3.1.x/). It has no understanding of SQL and treats all queries as raw strings without context. This means that simple syntax errors like trailing commas are difficult to debug and require a full run to detect.

SQLMesh supports Jinja, but it does not rely on it - instead, it parses/understands SQL through [SQLGlot](https://github.com/tobymao/sqlglot). Simple errors can be detected at compile time, so you no longer have to wait minutes or even longer to see that you've referenced a column incorrectly or missed a comma.

Additionally, having a first-class understanding of SQL supercharges SQLMesh with features such as transpilation, column-level lineage, and automatic change categorization.

### Testing
Data quality checks such as detecting NULL values and duplicated rows are extremely valuable for detecting upstream data issues and large scale problems. However, they are not meant for testing edge cases or business logic, and they are not sufficient for creating robust data pipelines.

[Unit and integration tests](./concepts/tests.md) are the tools to use to validate business logic. SQLMesh encourages users to add unit tests to all of their models to ensure changes don't unexpectedly break assumptions. Unit tests are designed to be fast and self contained so that they can run in continuous integration (CI) frameworks.

### Python models
dbt's Python models only run remotely on adapters of data platforms that have a full Python runtime, limiting the number of users that can take advantage of them and making the models difficult to debug.

SQLMesh's [Python models](../concepts/models/python_models) run locally and can be used with any data warehouse. Breakpoints can be added to debug the model.

### Data contracts
dbt offers manually configured schema contracts that will check the model's schema against the yaml schema at runtime. Models can be versioned to allow downstream teams time to migrate to the latest version, at the risk of a fragmented source of truth during the migration period.

SQLMesh provides automatic schema contracts and data contracts via [`sqlmesh plan`](../concepts/plans), which checks the model's schema and query logic for changes that affect downstream users. `sqlmesh plan` will show which models have breaking changes and which downstream models are affected.

While breaking changes can be rolled out as separate models to allow for a migration period, SQLMesh's [Virtual Preview](../concepts/glossary#virtual-preview) empowers teams to collaborate on migrations before the changes are deployed to prod, maintaining a single source of truth across the business.