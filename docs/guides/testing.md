# Testing guide

## Testing changes to models

To run unit tests for your models, run the `sqlmesh test` command as follows:

```bash
$ sqlmesh test
.
----------------------------------------------------------------------
Ran 1 test in 0.052s

OK
```

As the unit tests run, SQLMesh will identify any that fail.

For more information about tests, refer to [testing](../concepts/tests.md).

### Test changes to a specific model

To run a specific model test, pass in the module followed by `::` and the name of the test. 

For example: `example_full_model::test_example_full_model.`

### Run a subset of tests

To run a test that matches a pattern or substring, use the following syntax: `project.tests.test_order*`. 

For example, this will match the following tests: `project.tests.test_orders`, `project.tests.test_order_items`.

## Auditing changes to models

To audit your models, run the `sqlmesh audit` command as follows:

```bash
$ sqlmesh audit
Found 1 audit(s).
assert_positive_order_ids PASS.

Finished with 0 audit error(s).
Done.
```

**Note:** Ensure that you have already planned and applied your changes before running an audit.

By default, SQLMesh will halt the pipeline when an audit fails in order to prevent potentially invalid data from propagating further downstream. This behavior can be changed for individual audits by defining them as [non-blocking](../concepts/audits.md#non-blocking-audits).

For more information about audits, refer to [auditing](../concepts/audits.md).
