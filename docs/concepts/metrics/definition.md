# Definition

Metrics are defined in SQL files in the `metrics/` directory of your SQLMesh project. A single file can contain multiple metric definitions in the form of

```sql linenums="1"
METRIC (
  name total_orders_from_active_customers,
  owner jen,
  expression COUNT(IF(sushi.customers.status = 'ACTIVE', sushi.orders.id, NULL))
);
```

## SQL expressions
The expression field can be any SQL statement that contains an aggregation.

```sql linenums="1"
METRIC (
  name unique_account_ids,
  expression COUNT(DISTINCT silver.accounts.account_id)
);
```

All columns referenced in the expression should be fully qualified with the model name. For example, if a model name is `a.b`, a metric referencing column `c` should point to `a.b.c`.

### Automatic joins
Metrics can refer to multiple tables, in these cases, [grains](../models/overview.md#grain) and [references](../models/overview.md#references) are used to automatically join these tables together.


```sql linenums="1"
-- Example models defining grain/references for automatic joins.
MODEL (
  name prod.users,
  grain user_id
);

MODEL (
  name prod.searches,
  references user_id,
);

METRIC (
  name canadian_searchers,
  expression SUM(IF(prod.users.country = 'CAD', prod.searches.num_searches, 0)),
);
```

In this example, `canadian_searchers` sums searches from users in Canada. Due to the use of grains/references, SQLMesh can automatically do the correct join.

### Derived metrics
Metrics that perform additional operations on other metrics are also possible.

```sql linenums="1"
-- Example models defining grain/references for automatic joins.
METRIC (
  name total_searches,
  expression SUM(prod.searchers.num_searches)
);

METRIC (
  name total_clicks,
  expression SUM(prod.clickers.num_clicks)
);

METRIC (
  name clicks_per_search,
  expression total_clicks / total_searches
);
```

## Properties
### name
- The name of the metric. This name is case insensitive and must be unique in a project.
### expression
- A SQL expression consisting of an aggregation, a formula consisting of other metrics, or a combination of both.
### description
- The description of the metric.
### owner
- The owner of the metric. Used for organizational and governance purposes.
### dialect
- The dialect that the expression is written in. It is recommended to leave this empty and rely on the project's default dialect.
