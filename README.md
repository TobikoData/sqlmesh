# SQLMesh
## About
SQLMesh is a next generation SQL transformation platform. It allows you to focus on **simply writing SQL** while providing powerful automation for versioning, backfilling, deployment, and testing.

SQLMesh heavily optimizes for **incremental compute** eg. computing one day or hour at a time. Unlike other transformation frameworks that default to full refresh, SQLMesh defaults to incremental because it is more cost effective and scalable. SQLMesh allows you to take advantage of the cost and time savings of incrementality while automating away the complexity.

SQLMesh is able to achieve all of this with minimal set up. There are no additional services or dependencies to install to get started using SQLMesh except for a connection to your existing data warehouse or engine.

# Why SQLMesh?
## Cost & Efficiency
Incremental compute is significantly cheaper than full refresh compute. If you have a 1 year of history and only receive new data daily, only processing the new data is ~365x cheaper than reprocessing 1 year every day. As your data grows and grows, it is possible that refreshing your tables takes longer than a day, which means you would never catch up. Some tables may also not be able to be refreshed in one shot and need to be batched up into smaller intervals.

The cost of your data pipelines compound as more dependent pipelines get created. Writing your data pipelines mostly incrementally can have exponential savings.

SQLMesh also safely reuses physical tables across isolated environments. Some databases like snowflake have [zero-copy cloning](https://docs.snowflake.com/en/user-guide/tables-storage-considerations.html#label-cloning-tables) but this is a manual process and not widely supported. SQLMesh is able to automatically reuse tables regardless of which engine / warehouse you're using. It achieves this by storing fingerprints of your models and using [views](https://en.wikipedia.org/wiki/View_(SQL)) like pointers to physical locations. Thus, spinning up a new development environment is fast and cheap. Only models with incompatible changes need to be materialized, saving even more time and money.

## Automation
SQLMesh democratizes the ability to write **safe** and **scaleable** data pipelines to **all data practitioners** regardless of technical ability.

Creating maintainable and scaleable data pipelines is extremely difficult and a task usually reserved for data engineers. As your data grows, the need for incremental compute becomes mandatory due to cost and time constaints. Incremental models have inherent state of which partitions have been computed or not. This makes managing a consistent and accurate (no data leakages / gaps) challenging. Although a seasoned engineer may have the expertise or tooling to operate one of these tables, an analyst would not. In these organizations, analysts either need to file a ticket and wait on data engineering resources, or bypass core data models by running their own custom jobs which inevitably leads to an ungoverned data swamp.

As more models and users depend on core tables, the complexity of making changes increases. You need to ensure that all downstream consumers are compatible and updated with new changes. Propogating a change throughout a complex graph of dependencies is difficult to communicate and also a challenge to do correctly. Additionally, the introduction of other schedulers like [Airflow](https://airflow.apache.org/) adds even more complexity. SQLMesh integrates directly with your existing scheduler so that your whole data pipeline, including jobs outside of SQLMesh, can be unified.

Other frameworks categorize incrementality as an 'advanced' use case and should be avoided unless necessary. Additionally, they don't treat incrementality as first class citizens, so configuring models is tricky because of complex macros that require understanding the context of an execution eg. is this run incremental or a full refresh?. With SQLMesh, not only is incrementality easy, but it's the default. Writing your data pipelines incrementally with SQLMesh not only saves you money and time, but also keeps systems maintainable and consistent.

## Collaboration
SQLMesh focuses on making data pipelines a collaborative experience. While it empowers less technical data users to contribute, it allows them to collaborate with others who may be more familiar with data engineering. Development and changes can be done in a fully isolated environment that can be accessed and validated by others. Additionally, SQLMesh provides information about changes and how they may affect downstream consumers. This transparency along with the ability to categorize changes makes it more feasible for a less saavy user to make changes to core data pipelines.

By integrating with our CI/CD flows, changes can be required to have approval before affecting production to ensure that the relevant data owners or experts can validate changes.

SQLMesh supports both [audits](#audits) and [tests](#tests). Although unit testing has been commonplace in the world of software engineering, they are relatively unknown in the data world. SQLMesh's data unit tests allows for greater collaboration because data pipeline owners can ensure that changes to models don't change underlying logic. These tests can run quickly in CI or locally without having to actually create full scale tables.

# Overview
SQLMesh allows analysts, scientists, and engineers to comfortably unify around common tooling while guaranteeing scalable modern data best practices. SQLMesh is built around two main commands, [plan](#plan) and [apply](#apply) which makes it easy to iterate, test, and deploy code/data changes.

Changing SQL queries [(models)](#model) can have dramatic effects downstream when working with complex pipelines. SQLMesh's [plan](#plan) lets developers understand the full scope of directly and indirectly impacted downstream wokrflows automatically, giving them a holistic view of  their changes.

Deploying new pipelines can be time consuming, expensive, and error-prone. SQLMesh's [apply](#apply) lets developers deploy their changes to isolated develop environments for testing and validation, seamlessly handling backfilling and reusing existing tables. When development is done, `promoting` an environment to production is quick and has no downtime. SQLMesh is able to accomplish all of this regardless of your SQL engine / warehouse's capabilities.

SQLMesh leverages [SQLGlot](https://github.com/tobymao/sqlglot) allowing it to have a first-class understanding of queries. The lineage of your SQL queries is automatically determined without having to manually define dependencies/ references. Additionally, SQLMesh seamlessly integrates with workflow orchestrations like [Airflow](https://airflow.apache.org/) so that your queries can run robustly in production.


# Getting Started
To get started and up and running, just install SQLMesh.

```
pip install sqlmesh
```

SQLMesh supports a variety of deployment models and use cases because we understand that everyone's data stack is different.


You can interact with SQLMesh through the [cli](...) or through [notebooks](...).

## Local DuckDB
SQLMesh can be used on your laptop without any other infrastructure. If you'd like to use SQLMesh against local files using DuckDB, you can get started in just a couple of minutes.

## Airflow
See `sqlmesh.schedulers.airflow`.

# Get in Touch
We'd love for you to join our community and help you along your data journey.

- File issues on our [GitHub](https://github.com/TobikoData/sqlmesh/issues/new)
- Join our [Slack](https://join.slack.com/t/tobiko-data/shared_invite/zt-1je7o3xhd-C7~GuZTj0a8xz_uQbTJjHg) to ask questions or just say hi
- Or send us an email at [hello@tobikodata.com](hello@tobikodata.com)

# Model
Date pipelines are made up of many connected queries and jobs that produce tables and data assets. SQLMesh represents each asset as a model. A model is simply a SQL file with a Model definition and a query. The model results in either a single table/view or a logical query which can be used by other queries.


```sql
-- Customer revenue computed and stored daily.
MODEL (
  name sushi.customer_revenue_by_day,
  owner toby,
  cron '@daily',
);

SELECT
  c.customer_id::TEXT,
  SUM(o.amount)::DOUBLE AS revenue
  o.ds::TEXT
FROM sushi.orders AS o
JOIN sushi.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.ds BETWEEN @start_ds and @end_ds
```

With this model definition, SQLMesh automatically understands that ***customer revenue*** depends on 2 upstream models, ***orders*** and ***customers***. It will also compute this model incrementally every day after first computing its dependencies. Instead of recomputing all of history every day which can be time consuming and expensive, SQLMesh can handle incremental models with ease. However, sometimes models do need to be recomputed from scratch every day. This can be handled by setting `sqlmesh.core.model.ModelKind` to `full`.

Read more about `sqlmesh.core.model`.

## Model Kinds

# Lineage
Lineage provides a way to visualize how data flows through your organization. SQLMesh can automatically build a DAG that represents your models and their relationships with each other. This lineage graph is a powerful tool for understanding and debugging your organization's data and pipelines.

# Deployment
Deploying SQLMesh models is easy and efficient. SQLMesh allows you to spin up zero-copy development environments and iterate without affecting production. It achieves this in an engine / warehouse agnostic way by leveraging views. Although some warehouses like Snowflake have the ability to create [zero-copy clones](https://docs.snowflake.com/en/user-guide/tables-storage-considerations.html#label-cloning-tables), it is a manual process and not applicable in other engines. SQLMesh automatically figures out when an existing table can be reused or needs backfilling based on fingerprinting a model's SQL query.

Execution, backfilling, and promoting views is all handled automatically through SQLMesh's plan and apply commands which is similar to the paradigm popularized by [Terraform](https://developer.hashicorp.com/terraform/cli/commands/plan).

## Plan
Plan allows you to see how your changes compare to an existing environment. It will show you all the differences and prompt you with choices that need to be made such as whether or not your changes are breaking and what date range should be backfilled.

Read more about `sqlmesh.core.plan`.

## Apply
Apply evaluates a Plan and orchestrates all of the necessary steps in order to reflect a plan in a given environment.

# Tests
SQLMesh tests are unit tests for models. They compare expected outputs with actual outputs for models. This is done with user provided input and output data fixtures.
```yaml
test_customer_revenue_by_day:
  model: sushi.customer_revenue_by_day
  inputs:
    sushi.orders:
      rows:
        - id: 1
          customer_id: 1
          waiter_id: 1
          start_ts: 2022-01-01 01:59:00
          end_ts: 2022-01-01 02:29:00
          ds: 2022-01-01
    sushi.order_items:
      rows:
        - id: 1
          order_id: 1
          item_id: 1
          quantity: 2
          ds: 2022-01-01
        - id: 2
          order_id: 1
          item_id: 2
          quantity: 3
          ds: 2022-01-01
    sushi.items:
      rows:
        - id: 1
          name: maguro
          price: 1.23
          ds: 2022-01-01
        - id: 2
          name: ika
          price: 2.34
          ds: 2022-01-01
  outputs:
    vars:
      start: 2022-01-01
      end: 2022-01-01
      latest: 2022-01-01
    ctes:
      order_total:
        rows:
        - order_id: 1
          total: 9.48
          ds: 2022-01-01
    query:
      rows:
      - customer_id: 1
        revenue: 9.48
        ds: '2022-01-01'
```
This test provides the upstream data for the ***customer revenue*** model as well as the expected output data for a CTE used by the model as well as the model's query. SQLMesh will load the input, execute the model query, and compare it with the expected output. Like in software engineering, a good SQLMesh test suite can catch issues with your queries and give data engineers and analysts confidence when making changes to models.

Read more about `sqlmesh.core.test`

# Audits
Audits are SQL queries that make assertions on the validity of your data. SQLMesh will automatically audit your data, halt your pipeline, and alert you if any of your audits fail. This way, bad data can be caught early and will not propagate downstream to other models and to end users. Audits are defined in SQL files with an Audit definition and a query.
```sql
AUDIT (
  name assert_item_price_is_not_null,
  model sushi.items,
  dialect spark,
)
SELECT * from sushi.items
WHERE ds BETWEEN @start_ds AND @end_ds AND
   price IS NULL
```
This audit will run after loading ***items*** and detect if there are missing prices.

Read more about `sqlmesh.core.audit`

# Dates
SQLMesh uses dates and datetimes to indicate what date intervals should be processed. All time periods should be treated as inclusive. When a model uses dates, eg '2022-01-01' or DATE('2022-01-01'), the date variables are treated categorically so that you can write queries like

```sql
SELECT *
FROM x
WHERE ds BETWEEN '2022-01-01' -- @start_ds
  AND '2022-01-01' -- @end_ds
```

However, when using datetimes, variables are treated as numeric and get converted to 1 millisecond before the next date. So if you care about all the data on '2022-01-01' as a datetime, then start and end queries will look like this

```sql
SELECT *
FROM x
WHERE ts BETWEEN '2020-01-01 00:00:00.000000' -- @start_ts
  AND '2020-01-01 23:59:59.999000' -- @end_ts
```

# Macros
SQLMesh allows you to make SQL queries dynamic and [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) through the use of Macros. SQLMesh macros
use the SQL syntax you're used to except with the prefix `@`.

## Variables
The main use case for macros are variables for dates. These variables will be substituted at runtime with the correct values.

```sql
SELECT @ds
```
evaulates to become
```sql
SELECT '2022-01-01'
```

A more advanced feature with macros is using functions. SQLMesh macro functions have two modes of operation, literal SQL mode and eval mode. If a function references columns, then it will be evaluated as if you're dynamically creating SQL, otherwise it will be evaluated in the python interpreter.

## Literal SQL Mode
```sql
SELECT @EACH(['a', 'b'], x -> z.x + 1 AS @SQL(x))
FROM z
```

will evaluate to
```sql
SELECT
  z.x + 1 AS a,
  z.x + 1 AS b,
FROM z
```
because the macro references the column z.x. It treats the lambda function as generating new sql literals.

## Eval Mode
```sql
SELECT @REDUCE(@EACH(['a', 'b'], (x, y) -> CONCAT(x, y))) AS c
FROM z
```

will evaluate to
```sql
SELECT 'ab' AS c
FROM z
```
because the macro does not reference any columns.

will evaluate to
```sql
SELECT
  z.x + 1 AS a,
  z.x + 1 AS b,
FROM z
```



Read more about `sqlmesh.core.macros`.

# Examples

# Development
Install dev dependencies:
```
make install-dev
```
Run linters and formatters:
```
make style
```
Run tests:
```
make test
```
Run docs server:
```
make docs-serve
```
Run ide:
```
make web-serve
```
Optional: Use pre-commit to automatically run linters/formatters
```
make install-pre-commit
```
