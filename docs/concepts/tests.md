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
    <macro_variable_name>: <macro_variable_value>
```

### Example

In this example, we'll use the `sqlmesh_example.example_full_model` model, which is provided as part of the `sqlmesh init` command and defined as follows:

```sql linenums="1"
MODEL (
  name sqlmesh_example.example_full_model,
  kind FULL,
  cron '@daily'
);

SELECT
  item_id,
  COUNT(distinct id) AS num_orders,
FROM
    sqlmesh_example.example_incremental_model
GROUP BY item_id
```

Notice how the query of the model definition above references one upstream model: `sqlmesh_example.example_incremental_model`.

The test definition for this model may look like following:

```yaml linenums="1"
test_example_full_model:
  model: sqlmesh_example.example_full_model
  inputs:
    sqlmesh_example.example_incremental_model:
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

### Testing CTEs

Individual CTEs within the model's query can also be tested. Let's slightly modify the query of the model used in the previous example:

```sql linenums="1"
WITH filtered_orders_cte AS (
    SELECT
      id,
      item_id
    FROM
        sqlmesh_example.example_incremental_model
    WHERE
        item_id = 1
)
SELECT
  item_id,
  COUNT(distinct id) AS num_orders,
FROM
    filtered_orders_cte
GROUP BY item_id
```

Below is the example of a test that verifies individual rows returned by the `filtered_orders_cte` CTE before aggregation takes place:

```yaml linenums="1" hl_lines="16-22"
test_example_full_model:
  model: sqlmesh_example.example_full_model
  inputs:
    sqlmesh_example.example_incremental_model:
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
FAIL: test_example_full_model (/Users/izeigerman/github/tmp/tests/test_suite.yaml:1)
----------------------------------------------------------------------
AssertionError: Data differs
- {'item_id': 1, 'num_orders': 3}
?                              ^

+ {'item_id': 1, 'num_orders': 2}
?                              ^


----------------------------------------------------------------------
Ran 1 test in 0.008s

FAILED (failures=1)
```

### Testing for specific models
To run a specific model test, pass in the suite file name followed by `::` and the name of the test:

```
sqlmesh test tests/test_suite.yaml::test_example_full_model
```

You can also run tests that match a pattern or substring using a glob pathname expansion syntax:

```
sqlmesh test tests/test_*
```
