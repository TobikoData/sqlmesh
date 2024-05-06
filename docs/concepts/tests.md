# Testing

Testing allows you to protect your project from regression by continuously verifying that the output of each model matches your expectations. Unlike [audits](audits.md), tests are executed either on demand (for example, as part of a CI/CD job) or every time a new [plan](plans.md) is created.

Similar to unit testing in software development, SQLMesh evaluates the model's logic against predefined inputs and then compares the output to expected outcomes provided as part of each test.

A comprehensive suite of tests can empower data practitioners to work with confidence, as it allows them to ensure models behave as expected after changes have been applied to them.

## Creating tests

Test suites are defined using the YAML format. Each suite is a file whose name must begin with `test`, end in either `.yaml` or `.yml`, and is stored under the `tests/` folder of your SQLMesh project.

Tests within a suite file contain the following attributes:

* The unique name of a test
* The name of the model targeted by this test
* [Optional] The test's description
* [Optional] The gateway whose `test_connection` will be used to run this test
* Test inputs, which are defined per upstream model or external table referenced by the target model. Each test input consists of the following:
    * The name of an upstream model or external table
    * The list of rows defined as a mapping from a column name to a value associated with it
    * [Optional] The table's schema, defined as a mapping from a column name to its type, represented as a string. Any number of columns may be omitted from this mapping, in which case their types will be inferred by SQLMesh, when possible
* Expected outputs, which are defined as follows:
    * The list of rows that are expected to be returned by the model's query defined as a mapping from a column name to a value associated with it
    * [Optional] The list of expected rows per each individual [Common Table Expression](glossary.md#cte) (CTE) defined in the model's query
* [Optional] The dictionary of values for macro variables that will be set during model testing
    * There are three special macro variables: `start`, `end`, and `execution_time`. Setting these will allow you to override the date macros in your SQL queries. For example, `@execution_ds` will render to `2022-01-01` if `execution_time` is set to this value. Additionally, SQL expressions like `CURRENT_DATE` and `CURRENT_TIMESTAMP` will result in the same datetime value as `execution_time`, when it is set.

The YAML format is defined as follows:

```yaml linenums="1"
<unique_test_name>:
  model: <target_model_name>
  description: <description>  # Optional
  gateway: <gateway>  # Optional
  inputs:
    <upstream_model_or_external_table_name>:
      columns:  # Optional
        <column_name>: <column_type>
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

```yaml linenums="1"
<unique_test_name>:
  model: <target_model_name>
  inputs:
    <upstream_model_or_external_table_name>:
      - <column_name>: <column_value>
...
```

Note: the columns in each row of an expected output must appear in the same relative order as they are selected in the corresponding query.

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
        event_date: '2020-01-01'
      - id: 2
        item_id: 1
        event_date: '2020-01-02'
      - id: 3
        item_id: 2
        event_date: '2020-01-03'
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
```

The `event_date` column is not needed in the above test, since it is not referenced in `full_model`, so it may be omitted.

If we were only interested in testing the `num_orders` column, we could only specify input values for the `id` column of `sqlmesh_example.incremental_model`, thus rewriting the above test more compactly as follows:

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

Since [omitted columns](#omitting-columns) are treated as `NULL`, this test also implicitly asserts that both the input and the expected output `item_id` columns are `NULL`, which is correct.

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
  COUNT(DISTINCT id) AS num_orders,
FROM
    filtered_orders_cte
GROUP BY item_id
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
          event_date: '2020-01-01'
        - id: 2
          item_id: 1
          event_date: '2020-01-02'
        - id: 3
          item_id: 2
          event_date: '2020-01-03'
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

## Omitting columns

Defining the complete inputs and expected outputs for wide tables, i.e. tables with many columns, can become cumbersome. Therefore, if certain columns can be safely ignored they may be omitted from any row and their value will be treated as `NULL` for that row.

Additionally, it's possible to test only a subset of the expected output columns by setting `partial` to `true` for the rows of interest:

```yaml linenums="1"
  ...
  outputs:
    query:
      partial: true
      rows:
        - <column_name>: <column_value>
          ...
```

This is useful when we can't treat the missing columns as `NULL`, but still want to ignore them. In order to apply this setting to _all_ expected outputs, simply set it under the `outputs` key:

```yaml linenums="1"
  ...
  outputs:
    partial: true
    ...
```

When `partial` is set for a _specific_ expected output, its rows need to be defined as a mapping under the `rows` key and only the columns referenced in them will be tested.

## Freezing time

Some models may use SQL expressions that compute datetime values at a given point in time, such as `CURRENT_TIMESTAMP`. Since these expressions are non-deterministic, it's not enough to simply specify an expected output value in order to test them.

Setting the `execution_time` macro variable addresses this problem by mocking out the current time in the context of the test, thus making its value deterministic.

The following example demonstrates how `execution_time` can be used to test a column that is computed using `CURRENT_TIMESTAMP`. The model we're going to test is defined as:

```sql linenums="1"
MODEL (
    name colors,
    kind FULL
);

SELECT
    'Yellow' AS color,
    CURRENT_TIMESTAMP AS created_at
```

And the corresponding test is:

```yaml linenums="1"
test_colors:
  model: colors
  outputs:
    query:
      - color: "Yellow"
        created_at: "2023-01-01 12:05:03"
  vars:
    execution_time: "2023-01-01 12:05:03"
```

It's also possible to set a time zone for `execution_time`, by including it in the timestamp string.

If a time zone is provided, it is currently required that the test's _expected_ datetime values are timestamps without time zone, meaning that they need to be offset accordingly.

Here's how we would write the above test if we wanted to freeze the time to UTC+2:

```yaml linenums="1"
test_colors:
  model: colors
  outputs:
    query:
      - color: "Yellow"
        created_at: "2023-01-01 10:05:03"
  vars:
    execution_time: "2023-01-01 12:05:03+02:00"
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
        time_column event_date
    ),
    start '2020-01-01',
    cron '@daily',
    grain (id, event_date)
);

SELECT
    id,
    item_id,
    event_date,
FROM
    sqlmesh_example.seed_model
WHERE
    event_date BETWEEN @start_date AND @end_date
```

Firstly, we need to specify the input data for the upstream model `sqlmesh_example.seed_model`. The `create_test` command starts by executing a user-supplied query against the project's data warehouse and uses the returned data to produce the test's input rows.

For instance, the following query will return three rows from the table corresponding to the model `sqlmesh_example.seed_model`:

```sql linenums="1"
SELECT * FROM sqlmesh_example.seed_model LIMIT 3
```

Next, notice that `sqlmesh_example.incremental_model` contains a filter which references the `@start_date` and `@end_date` [macro variables](macros/macro_variables.md).

To make the generated test deterministic and thus ensure that it will always succeed, we need to define these variables and modify the above query to constrain `event_date` accordingly.

If we set `@start_date` to `'2020-01-01'` and `@end_date` to `'2020-01-04'`, the above query needs to be changed to:

```sql linenums="1"
SELECT * FROM sqlmesh_example.seed_model WHERE event_date BETWEEN '2020-01-01' AND '2020-01-04' LIMIT 3
```

Finally, combining this query with the proper macro variable definitions, we can compute the expected output for the model's query in order to generate the complete test.

This can be achieved using the following command:

```
$ sqlmesh create_test sqlmesh_example.incremental_model --query sqlmesh_example.seed_model "SELECT * FROM sqlmesh_example.seed_model WHERE event_date BETWEEN '2020-01-01' AND '2020-01-04' LIMIT 3" --var start '2020-01-01' --var end '2020-01-04'
```

Running this creates the following new test, located at `tests/test_incremental_model.yaml`:

```yaml linenums="1"
test_incremental_model:
  model: sqlmesh_example.incremental_model
  inputs:
    sqlmesh_example.seed_model:
    - id: 1
      item_id: 2
      event_date: 2020-01-01
    - id: 2
      item_id: 1
      event_date: 2020-01-01
    - id: 3
      item_id: 3
      event_date: 2020-01-03
  outputs:
    query:
    - id: 1
      item_id: 2
      event_date: 2020-01-01
    - id: 2
      item_id: 1
      event_date: 2020-01-01
    - id: 3
      item_id: 3
      event_date: 2020-01-03
  vars:
    start: '2020-01-01'
    end: '2020-01-04'
```

As shown below, we now have two passing tests. Hooray!

```
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 2 tests in 0.024s

OK
```

## Using a different testing connection

The testing connection can be changed for a given test. This may be useful when, e.g., the model being tested cannot be correctly transpiled to the dialect of the default testing engine.

The following example demonstrates this by modifying `test_example_full_model`, so that it runs against a single-threaded local Spark process, defined as the `test_connection` of the `spark_testing` gateway in the project's `config.yaml` file:

```yaml linenums="1"
gateways:
  local:
    connection:
      type: duckdb
      database: db.db
  spark_testing:
    test_connection:
      type: spark
      config:
        # Run Spark locally with one worker thread
        "spark.master": "local"

        # Move data under /tmp so that it is only temporarily persisted
        "spark.sql.warehouse.dir": "/tmp/data_dir"
        "spark.driver.extraJavaOptions": "-Dderby.system.home=/tmp/derby_dir"

default_gateway: local

model_defaults:
  dialect: duckdb
```

The test would then be updated as follows:

```yaml linenums="1"
test_example_full_model:
  gateway: spark_testing
  # ... the other test attributes remain the same
```

## Running tests

Tests run automatically every time a new [plan](plans.md) is created, but they can also be executed on demand as described in the following sections.

### Testing using the CLI

You can execute tests on demand using the `sqlmesh test` command as follows:

```
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 1 test in 0.005s

OK
```

The command returns a non-zero exit code if there are any failures, and reports them in the standard error stream:

```
$ sqlmesh test
F
======================================================================
FAIL: test_example_full_model (test/tests/test_full_model.yaml)
----------------------------------------------------------------------
AssertionError: Data mismatch (exp: expected, act: actual)

  num_orders
         exp  act
0        3.0  2.0

----------------------------------------------------------------------
Ran 1 test in 0.012s

FAILED (failures=1)
```

Note: when there are many differing columns, the corresponding DataFrame will be truncated by default. In order to fully display them, use the `-v` (verbose) option of the `sqlmesh test` command.

To run a specific model test, pass in the suite file name followed by `::` and the name of the test:

```
$ sqlmesh test tests/test_full_model.yaml::test_example_full_model
```

You can also run tests that match a pattern or substring using a glob pathname expansion syntax:

```
$ sqlmesh test tests/test_*
```

### Testing using notebooks

You can execute tests on demand using the `%run_test` notebook magic as follows:

```
# This import will register all needed notebook magics
In [1]: import sqlmesh
        %run_test

        ----------------------------------------------------------------------
        Ran 1 test in 0.018s

        OK
```

The `%run_test` magic supports the same options as the corresponding [CLI command](#testing-using-the-CLI).

## Troubleshooting Issues

When executing unit tests, SQLMesh creates input fixtures as views within the testing connection.

These fixtures are dropped by default after the execution completes, but it is possible to preserve them using the `--preserve-fixtures` option available in both the `sqlmesh test` CLI command and the `%run_test` notebook magic.

This can be helpful when debugging a test failure, because for example it's possible to query the fixture tables directly and verify that they are populated correctly.

### Type Mismatches

It's not always possible to correctly interpret certain column values in a unit test without additional context. For example, a YAML dictionary can be used to represent both a `STRUCT` and a `MAP` value.

To avoid this ambiguity, SQLMesh needs to know the column's type. This is possible either by relying on its type inference, which can be enhanced by `CAST`ing the model's columns, or by defining the model's schema:

- in the [`schema.yaml`](models/external_models.md#generating-an-external-models-schema-file) file
- using the [`columns`](models/overview.md#columns) model property
- using the [`columns`](#creating_tests) field in the unit test itself
