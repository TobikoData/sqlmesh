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
  audits [assert_positive_order_ids],
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
