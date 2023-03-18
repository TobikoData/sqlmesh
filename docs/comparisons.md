# Comparisons

There are many tools and frameworks in the data ecosystem. This page tries to make sense of it all.

## dbt
[dbt](https://www.getdbt.com/) is a tool for data transformations. It is a pioneer in this space and has shown how valuable transformation frameworks can be. Although dbt is a fanstastic tool, it has trouble scaling with data and organizational size.

SQLMesh aims to be dbt format compatible. Importing existing dbt projects with minor changes is currently supported in alpha status.

### Feature Comparisons
| Feature                           | dbt | SQLMesh
| -------                           | --- | -------
| `SQL models`                      | ✅ | ✅
| `Python models`                   | ✅ | ✅
| `Seed models`                     | ✅ | ✅
| `Jinja support`                   | ✅ | ✅
| `Views / Embedded Models`         | ✅ | ✅
| `Incremental Models`              | ✅ | ✅
| `Seed Models`                     | ✅ | ✅
| `Snapshot Models`                 | ✅ | ❌
| `Documentation generation`        | ✅ | ❌
| `Package Manager`                 | ✅ | ❌
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


### Incremental Models
Implementing an incremental model is difficult and error-prone in dbt because it does not keep track of state. Since there is no state in dbt, the user must write subqueries to find missing date boundaries.

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

Having to manually specify macros to find date boundaries is repetitive and error-prone. As incremental models become more complex, the cognitive burden of having two run times, "first time full refresh" vs "subsequent incremental", increases.

SQLMesh keeps track of which date ranges exist so the query can be simplified as follows.

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
dbt does not enforce that the data inserted into the incremental table should be there. This can lead to problems or consistency issues such as late arriving data overriding past partitions. SQLMesh wraps all queries under the hood in a subquery with a time filter to enforce that the data inserted for a particular batch is as expected.

dbt also only supports the 'insert/overwrite' incremental load pattern for systems that natively support it. SQLMesh enables 'insert/overwrite' on any system because it is the most robust way to do incremental pipelines. 'Append' pipelines are extremely dangerous due data leakage / duplicates.


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
The main pattern used in incremental models checks for MAX(ds). This pattern does not catch missing data from the past or data gaps.

```
Expected dates: 2022-01-01, 2022-01-02, 2022-01-03
Missing past data: ?, 2022-01-02, 2022-01-03
Data gap: 2022-01-01, ?, 2022-01-03
```

SQLMesh stores each date interval a model has been run with so that it knows exactly what dates are missing.

#### Performance
The subqueries that look for MAX(date) could have a performance impact on the query. SQLMesh is able to avoid these extra subqueries.

Additionally, dbt expects an incremental model to be able to fully refresh the first time it runs. For some large scale data sets, this is cost prohibitive or infeasible. SQLMesh is able to [batch](../concepts/models/overview#batch_size) up backfills into more manageable chunks.

### SQL unaware
dbt does not parse or understand SQL. It relies heavily on Jinja which is basically just string manipulation. Syntax errors are difficult to debug and only discovered at runtime.
