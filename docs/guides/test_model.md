# Test a model using a unit test

## Test changes to models

To run unit tests for your models, use the `sqlmesh test` command.

Running the `sqlmesh test` command produces the following example output:

```bash
.
----------------------------------------------------------------------
Ran 1 test in 0.138s

OK
```

As the unit tests run, SQLMesh will identify any that fail.

## Test changes to a specific model

To run a specific model test, pass in the module followed by `::` and the name of the test, such as: `example_full_model::test_example_full_model.`

## Run a subset of tests

To run a test that matches a pattern or substring, use the following syntax: `project.tests.test_order*`.
For example, this will match the following tests: `project.tests.test_orders`, `project.tests.test_order_items`.

For more information about tests, refer to [testing](../concepts/tests.md).
For more information about related data quality checks, or audits, refer to [auditing](../concepts/tests.md).
