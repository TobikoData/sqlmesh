# Getting started with SQLMesh

## How it works
SQLMesh allows data analysts, scientists, and engineers to unify around common tooling while guaranteeing scalable modern data best practices.

SQLMesh also makes it easy to iterate, test, and deploy code and data changes, and is built around two main commands: plan and apply.

### The plan command
Changing SQL query models can have dramatic effects downstream when working with complex pipelines. SQLMesh's plan command allows developers to understand the full scope of directly and indirectly-impacted workflows automatically, giving them a holistic view of the changes.

### The apply command
Deploying new pipelines can be time-consuming, expensive, and error-prone. SQLMesh's apply command allows developers to deploy their changes to isolated environments for testing and validation, seamlessly handling backfilling and reuse of existing tables. When development is complete, promoting an environment to production is quick and has no downtime. SQLMesh is able to accomplish all of this regardless of your data warehouse or SQL engine's capabilities.

## Installing SQLMesh
To get up and running, first install SQLMesh:

```
pip install sqlmesh
```

Because we understand that everyone's data stack is different, SQLMesh supports a variety of deployment models and use cases. You can interact with SQLMesh via the cli or notebooks.

### Local DuckDB
SQLMesh can be run on your laptop without any other infrastructure. If you'd like to run SQLMesh against local files using DuckDB, you can get started in just a few minutes.

### Airflow
Refer to `sqlmesh.schedulers.airflow`.

## Concepts

### Models
Date pipelines are made up of many connected queries and jobs that produce tables and data assets. SQLMesh represents each asset as a model, which is a SQL file with a model definition and a query. The model results in either a single table/view or a logical query, which can be used by other queries.

In the following example, SQLMesh automatically understands that ***customer revenue*** depends on two upstream models: ***orders*** and ***customers***. SQLMesh will compute this model incrementally every day after first computing its dependencies rather than re-computing the entire history every day, which can be time-consuming and expensive.

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

Note that sometimes, models do need to be re-computed from scratch every day. This can be handled by setting `sqlmesh.core.model.ModelKind` to `full`. Refer to the API for `sqlmesh.core.model`.

### Model types

### Lineage
Lineage provides a way to visualize how data flows through your organization. SQLMesh can automatically build a Directed Acyclic Graph (DAG) that represents your models and their relationships with each other. This lineage graph is a powerful tool for understanding and troubleshooting your organization's data and pipelines.

### Deployment
SQLMesh allows you to spin up zero-copy development environments and iterate without affecting production, making deploying SQLMesh models easy and efficient. This is achieved in a data warehouse and engine-agnostic way by leveraging views.

Although some data warehouses, such as Snowflake, have the ability to create [zero-copy clones](https://docs.snowflake.com/en/user-guide/tables-storage-considerations.html#label-cloning-tables), it is a manual process and not applicable to other engines. SQLMesh automatically determines when an existing table can be reused or whether it needs backfilling based on fingerprinting a model's SQL query. Execution, backfilling, and promoting views are all handled automatically through SQLMesh's [plan](#the-plan-command) and [apply](#the-apply-command) commands, similar to the paradigm popularized by [Terraform](https://developer.hashicorp.com/terraform/cli/commands/plan).

**Plan**
<br>
The plan command allows you to see how your changes compare to an existing environment. The plan command will display the differences and prompt you with areas that need attention, such as whether or not your changes are breaking, or which date range should be backfilled. Refer to the API for `sqlmesh.core.plan`.

**Apply**
<br>
The apply command evaluates a plan and orchestrates all of the necessary steps to reflect a plan in a given environment.

### Tests
SQLMesh tests are unit tests for models, comparing expected outputs with actual outputs. This is done with user-provided input and output data fixtures. SQLMesh will load the input, execute the model query, and compare it with the expected output. Similar to the software engineering test process, a good SQLMesh test suite can catch issues with your queries and give data engineers and analysts confidence when making changes to models.

The following test provides the upstream data for the ***customer revenue*** model, the expected output data for a Common Table Expression (CTE) used by the model, and the model's query:

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

Refer to the API for `sqlmesh.core.test`.

### Audits
Audits are SQL queries that make assertions on the validity of your data. SQLMesh will automatically audit your data, halt your pipeline, and alert you if any of your audits fail. This way, bad data will be caught early and won't propagate downstream to other models and end users. Audits are defined in SQL files with an audit definition and a query, as in the following example:

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
This audit will run after loading ***items***, and will detect if there are missing prices.

Refer to the API for `sqlmesh.core.audit`.

### Dates
SQLMesh uses dates and datetimes to indicate which date intervals should be processed. All time periods should be treated as inclusive. When a model uses dates, i.e. '2022-01-01' or DATE('2022-01-01'), the date variables are treated categorically so that you can write queries such as the following:

```sql
SELECT *
FROM x
WHERE ds BETWEEN '2022-01-01' -- @start_ds
  AND '2022-01-01' -- @end_ds
```

When using datetimes, variables are treated as numeric and will be converted to one millisecond before the next date. So, if you require all the data on '2022-01-01' as a datetime, start and end queries will look like this:

```sql
SELECT *
FROM x
WHERE ts BETWEEN '2020-01-01 00:00:00.000000' -- @start_ts
  AND '2020-01-01 23:59:59.999000' -- @end_ts
```

### Macros
SQLMesh allows you to make SQL queries dynamic and [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) through the use of macros. SQLMesh macros use the SQL syntax you're used to, but with the prefix `@`.

### Variables
The main use case for macros are variables for dates. These variables will be substituted at runtime with the correct values, such as in the following example:

```sql
SELECT @ds
```
evaulates to become:
```sql
SELECT '2022-01-01'
```

A more advanced macro feature is using functions. SQLMesh macro functions have two modes of operation: literal SQL mode, and eval mode. If a function references columns, it will be evaluated as if you're dynamically creating SQL; otherwise it will be evaluated in the python interpreter.

### Literal SQL Mode
```sql
SELECT @EACH(['a', 'b'], x -> z.x + 1 AS @SQL(x))
FROM z
```

will evaluate to:
```sql
SELECT
  z.x + 1 AS a,
  z.x + 1 AS b,
FROM z
```
because the macro references the column z.x, it treats the lambda function as generating new SQL literals.

### Eval Mode
```sql
SELECT @REDUCE(@EACH(['a', 'b'], (x, y) -> CONCAT(x, y))) AS c
FROM z
```

will evaluate to:
```sql
SELECT 'ab' AS c
FROM z
```
because the macro does not reference any columns, and:

```sql
SELECT
  z.x + 1 AS a,
  z.x + 1 AS b,
FROM z
```

Refer to the API for `sqlmesh.core.macros`.

### SQLGlot
SQLMesh leverages [SQLGlot](https://github.com/tobymao/sqlglot), which provides a first-class understanding of queries. The lineage of your SQL queries is automatically determined without having to manually define dependencies or references.
