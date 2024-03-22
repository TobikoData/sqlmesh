# Testing

Testing allows you to protect your project from regression by continuously verifying that the output of each model matches your expectations. Unlike [audits](audits.md), tests are executed either on demand (for example, as part of a CI/CD job) or every time a new [plan](plans.md) is created.

Similar to unit testing in software development, SQLMesh evaluates the model's logic against predefined inputs and then compares the output to expected outcomes provided as part of each test.

A comprehensive suite of tests can empower data practitioners to work with confidence, as it allows them to ensure models behave as expected after changes have been applied to them.

## Creating tests

Test suites are defined using the YAML format. Each suite is a file whose name must begin with `test`, end in either `.yaml` or `.yml`, and is stored within the `tests/` folder of your SQLMesh project.

Tests within a suite file contain the following attributes:

* The unique name of a test
* The name of the model targeted by this test
* Test inputs, which are defined per upstream model or external table referenced by the target model. Each test input consists of the following:
    * The name of an upstream model or external table
    * The list of rows defined as a mapping from a column name to a value associated with it
* Expected outputs, which are defined as follows:
    * The list of rows that are expected to be returned by the model's query defined as a mapping from a column name to a value associated with it
    * [Optional] The list of expected rows per each individual [Common Table Expression](glossary.md#cte) (CTE) defined in the model's query
* [Optional] The dictionary of values for macro variables that will be set during model testing
    * There are three special macros that can be overridden, `start`, `end`, and `execution_time`. Overriding each will allow you to override the date macros in your SQL queries. For example, setting execution_time: 2022-01-01 -> execution_ds in your queries.

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

The `rows` key is optional in the above format, so the following would also be valid:

```
<unique_test_name>:
  model: <target_model_name>
  inputs:
    <upstream_model_or_external_table_name>:
      - <column_name>: <column_value>
...
```

### Omitting Columns

Defining the complete inputs and outputs for wide tables, i.e. tables with many columns, can become cumbersome. Therefore, if certain columns can be safely ignored, they may be omitted from any row and their value will be treated as `NULL` for that row.

Additionally, it's possible to test only a subset of the output columns by setting `partial` to `true` for the rows of interest:

```yaml linenums="1"
  ...
  outputs:
    query:
      partial: true
      rows:
        - <column_name>: <column_value>
          ...
```

This is useful when we can't treat the missing columns as `NULL`, but still want to ignore them. When `partial` is set, the rows need to be defined as a mapping under the `rows` key and the tested columns are only those that are referenced in them.

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

The `ds` column is redundant in the above test, since it is not referenced in `full_model`, so it may be omitted.

If we were only interested in the `num_orders` column, we could only specify input values for the `id` column of `sqlmesh_example.incremental_model`, thus rewriting the above test more compactly as follows:

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

Since [omitted columns](#omitting-columns) are treated as `NULL`, this test also implicitly asserts that both the input and the output `item_id` column are `NULL`, which is correct.

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

Creating tests manually can be repetitive and error-prone, which is why SQLMesh also provides a way to automate this process using the [`create_test` command](../reference/cli.md#create_test).

This command can generate a complete test for a given model, as long as the tables of the upstream models it references exist in the project's data warehouse and are already populated with data.

### Example

In this example, we'll show how to generate a test for `sqlmesh_example.incremental_model`, which is another model provided as part of the `sqlmesh init` command and defined as follows:

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

Firstly, we need to specify the input data for the upstream model `sqlmesh_example.seed_model`. The `create_test` command starts by executing a user-supplied query against the project's data warehouse and uses the returned data for this purpose.

For instance, the following query will return three rows from the table corresponding to the model `sqlmesh_example.seed_model`:

```sql linenums="1"
SELECT * FROM sqlmesh_example.seed_model LIMIT 3
```

Next, notice that `sqlmesh_example.incremental_model` contains a filter which references the `@start_ds` and `@end_ds` [macro variables](macros/macro_variables.md).

To make the generated test deterministic and thus ensure that it will always succeed, we need to define these variables and modify the above query to constrain `ds` accordingly.

If we set `@start_ds` to `'20-01-01'` and `@end_ds` to `'2020-01-04'`, the query becomes:

```sql linenums="1"
SELECT * FROM sqlmesh_example.seed_model WHERE ds BETWEEN '2020-01-01' AND '2020-01-04' LIMIT 3
```

Finally, combining this query with the proper macro variable definitions, we can compute the expected output for the model's query in order to generate the complete test.

This can be achieved using the following command:

```bash
$ sqlmesh create_test sqlmesh_example.incremental_model --query sqlmesh_example.seed_model "select * from sqlmesh_example.seed_model where ds between '2020-01-01' and '2020-01-04' limit 3" --var start '2020-01-01' --var end '2020-01-04'
```

Running this creates the following new test, located at `tests/test_incremental_model.yaml`:

```yaml linenums="1"
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

As shown below, we now have two passing tests. Hooray!

```bash
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 2 tests in 0.024s

OK
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

Note: when there are many differing columns, the corresponding DataFrame will be truncated by default, but it can be fully rendered using the `-v` (verbose) option of the `sqlmesh test` command.

### Testing for specific models

To run a specific model test, pass in the suite file name followed by `::` and the name of the test:

```bash
$ sqlmesh test tests/test_full_model.yaml::test_example_full_model
```

You can also run tests that match a pattern or substring using a glob pathname expansion syntax:

```bash
$ sqlmesh test tests/test_*
```
