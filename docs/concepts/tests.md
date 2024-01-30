# Testing

Testing allows you to protect your project from regression by continuously verifying the output of each model matches your expectations. Unlike [audits](audits.md), tests are executed either on demand (for example, as part of a CI/CD job) or every time a new [plan](plans.md) is created.

Similar to unit testing in software development, SQLMesh evaluates the model's logic against predefined inputs and then compares the output to expected outcomes provided as part of each test.

A comprehensive suite of tests can empower data practitioners to work with confidence, as it allows them to ensure models behave as expected after changes have been applied to them.

## Creating tests

Test suites are defined using YAML format within `.yaml` files in the `tests/` folder of your SQLMesh project. Each test within a suite file contains the following attributes:

* The unique name of a test
* The name of the model targeted by this test
* Test inputs, which are defined per external table or upstream model referenced by the target model. Each test input consists of the following:
    * The name of an upstream model or external table
    * The list of rows defined as a mapping from a column name to a value associated with it
* Expected outputs, which are defined as follows:
    * The list of rows that are expected to be returned by the model's query defined as a mapping from a column name to a value associated with it
    * [Optional] The list of expected rows per each individual [Common Table Expression](glossary.md#cte) (CTE) defined in the model's query
* [Optional] The dictionary of values for macro variables that will be set during model testing
    * There are three special macros that can be overridden, `start`, `end`, and `execution_time`. Overriding each will allow you to override the date macros in your SQL queries. For example, setting execution_time: 2022-01-01 -> execution_ds in your queries.

A column may be omitted from a row (either input or output), in which case it will be implicitly added with the value `NULL`. For example, this can be useful when specifying input data for wide tables where some columns may not be required to define a test.

The YAML format is defined as follows:

```yaml linenums="1"
<unique_test_name>:
  model: <target_model_name>
  inputs:
    <upstream_model_or_external_table_name>:
      rows:
        - <column_name>: <column_value>
  outputs:
    query:
      rows:
        - <column_name>: <column_value>
    ctes:  # Optional
      <cte_alias>:
        rows:
          - <column_name>: <column_value>
  vars:  # Optional
    start: 2022-01-01
    end: 2022-01-01
    execution_time: 2022-01-01
    <macro_variable_name>: <macro_variable_value>
```

Note: the `rows` key is optional in the above format, so the following would also be valid:

```
<unique_test_name>:
  model: <target_model_name>
  inputs:
    <upstream_model_or_external_table_name>:
      - <column_name>: <column_value>
...
```

### Example

In this example, we'll use the `sqlmesh_example.full_model` model, which is provided as part of the `sqlmesh init` command and defined as follows:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  COUNT(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
ORDER BY item_id
```

Notice how the query of the model definition above references one upstream model: `sqlmesh_example.incremental_model`.

The test definition for this model may look like the following:

```yaml linenums="1"
test_example_full_model:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
        rows:
        - id: 1
          item_id: 1
          ds: '2020-01-01'
        - id: 2
          item_id: 1
          ds: '2020-01-02'
        - id: 3
          item_id: 2
          ds: '2020-01-03'
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
```

Note that `ds` is redundant in the above test, since it is not referenced in `full_model`, so it may be omitted.

Let's also assume that we are only interested in testing the `num_orders` output column, i.e. we only care about the `id` input column of `sqlmesh_example.incremental_model`. Then, we could rewrite the above test more compactly as follows:

```yaml linenums="1"
test_example_full_model:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
        rows:
        - id: 1
        - id: 2
        - id: 3
  outputs:
    query:
      rows:
      - num_orders: 3
```

Leaving out the input column `item_id` means that it will be implicitly added in all input rows with a `NULL` value. Thus, we expect the corresponding output column to only contain `NULL` values, which is indeed reflected in the above test since the `item_id` column is also omitted from `query`'s rows.

### Testing CTEs

Individual CTEs within the model's query can also be tested. Let's slightly modify the query of the model used in the previous example:

```sql linenums="1"
WITH filtered_orders_cte AS (
    SELECT
      id,
      item_id
    FROM
        sqlmesh_example.incremental_model
    WHERE
        item_id = 1
)
SELECT
  item_id,
  COUNT(distinct id) AS num_orders,
FROM
    filtered_orders_cte
GROUP BY item_id
ORDER BY item_id
```

Below is the example of a test that verifies individual rows returned by the `filtered_orders_cte` CTE before aggregation takes place:

```yaml linenums="1" hl_lines="16-22"
test_example_full_model:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
        rows:
        - id: 1
          item_id: 1
          ds: '2020-01-01'
        - id: 2
          item_id: 1
          ds: '2020-01-02'
        - id: 3
          item_id: 2
          ds: '2020-01-03'
  outputs:
    ctes:
      filtered_orders_cte:
        rows:
          - id: 1
            item_id: 1
          - id: 2
            item_id: 1
    query:
      rows:
      - item_id: 1
        num_orders: 2
```

## Automatic test generation

Creating tests manually is a cumbersome and, ironically, error-prone process, especially as the number of rows and columns of the involved models grows. To address this, SQLMesh provides the [`create_test` command](../reference/cli.md#create_test), which can be used to automatically create tests for a given model.

### Example

Since we already have a test for `sqlmesh_example.full_model`, in this example we'll show how to generate a test for `sqlmesh_example.incremental_model`, which is provided as part of the `sqlmesh init` command and defined as follows:

```sql linenums="1"
MODEL (
    name sqlmesh_example.incremental_model,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start '2020-01-01',
    cron '@daily',
    grain (id, ds)
);

SELECT
    id,
    item_id,
    ds,
FROM
    sqlmesh_example.seed_model
WHERE
    ds between @start_ds and @end_ds

```

As one may expect, we need to start by specifying what the input data are for `sqlmesh_example.seed_model`. The `create_test` command achieves this by executing a user-supplied query against the target warehouse of the SQLMesh project to produce the input rows of the aforementioned model.

Let's assume that we're only interested in specifying three input rows for `sqlmesh_example.seed_model`. One way to do that is by executing the following query:

```sql linenums="1"
SELECT * FROM sqlmesh_example.seed_model LIMIT 3
```

However, notice that `sqlmesh_example.incremental_model` also contains a filter which references the `@start_ds` and `@end_ds` [macro variables](macros/macro_variables.md). To ensure that the produced test will always pass, we modify the above query to constrain the value range of the `ds` column:

```sql linenums="1"
-- The dates '2020-01-01' and '2020-01-04' have been picked arbitrarily
SELECT * FROM sqlmesh_example.seed_model WHERE ds BETWEEN '2020-01-01' AND '2020-01-04' LIMIT 3
```

We will also define these variables in the test, so that the filter of `sqlmesh_example.incremental_model` matches that range after it's been rendered.

Finally, we don't have to specify the output `query` attribute, since we can compute its values given the input data produced by the above query.

The following command captures all of the above:

```bash
$ sqlmesh create_test sqlmesh_example.incremental_model --query sqlmesh_example.seed_model "select * from sqlmesh_example.seed_model where ds between '2020-01-01' and '2020-01-04' limit 3" --var start '2020-01-01' --var end '2020-01-04'
```

Running this command produces the following new test, which is located at `tests/test_incremental_model.yaml`:

```yaml linenums="1" hl_lines="16-22"
test_incremental_model:
  model: sqlmesh_example.incremental_model
  inputs:
    sqlmesh_example.seed_model:
    - id: 1
      item_id: 2
      ds: '2020-01-01'
    - id: 2
      item_id: 1
      ds: '2020-01-01'
    - id: 3
      item_id: 3
      ds: '2020-01-03'
  outputs:
    query:
    - id: 1
      item_id: 2
      ds: '2020-01-01'
    - id: 2
      item_id: 1
      ds: '2020-01-01'
    - id: 3
      item_id: 3
      ds: '2020-01-03'
  vars:
    start: '2020-01-01'
    end: '2020-01-04'
```

As shown below, we now have two passing tests:

```bash
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 2 tests in 0.024s

OK
```

Note: since the `sqlmesh create_test` command executes queries directly in the target warehouse, the tables of the involved models must be built first, otherwise the queries will fail.

## Running tests

### Automatic testing with plan

Tests run automatically every time a new [plan](plans.md) is created.

### Manual testing with the CLI

You can execute tests on demand using the `sqlmesh test` command as follows:
```bash
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 1 test in 0.005s

OK
```

The command returns a non-zero exit code if there are any failures, and reports them in the standard error stream:

```bash
$ sqlmesh test
F
======================================================================
FAIL: test_example_full_model (test/tests/test_full_model.yaml)
----------------------------------------------------------------------
AssertionError: Data differs (exp: expected, act: actual)

  num_orders
         exp  act
0        3.0  2.0

----------------------------------------------------------------------
Ran 1 test in 0.012s

FAILED (failures=1)
```

Note: when there are many differing columns, the corresponding DataFrame will be truncated by default, but it can be fully rendered using the `-v` option (verbose) of the `sqlmesh test` command.

### Testing for specific models

To run a specific model test, pass in the suite file name followed by `::` and the name of the test:

```
sqlmesh test tests/test_full_model.yaml::test_example_full_model
```

You can also run tests that match a pattern or substring using a glob pathname expansion syntax:

```
sqlmesh test tests/test_*
```
