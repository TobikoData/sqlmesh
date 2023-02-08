# Audit a model

## Audit changes to models

To audit your models, use the `sqlmesh audit` command. 

Running the `sqlmesh audit` command produces the following example output:

```
Found 1 audit(s).
assert_positive_order_ids PASS.

Finished with 0 audit error(s).
Done.
```

**Note:** Ensure that you have already planned and applied your changes before running an audit.

By default, SQLMesh will halt the pipeline when an audit fails in order to prevent potentially invalid data from propagating further downstream. This behavior can be changed for individual audits by defining them as [non-blocking](/concepts/audits#non-blocking-audits).

For more information about audits, refer to [auditing](/concepts/audits).
For more information about testing with unit tests, refer to [testing](/concepts/tests).
