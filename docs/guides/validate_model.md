# Validate changes to a model

## Validate changes to a model

To validate a model, you can:

* test a model
* evaluate a model
* preview changes using the `plan` command 
* audit a model

### 1. Test a model

To test a model, use the `sqlmesh test` command. For more information about unit tests, refer to [testing](/concepts/tests).

Running `sqlmesh test` produces the following example output:

```
.
----------------------------------------------------------------------
Ran 1 test in 0.138s

OK
```

### 2. Evaluate a model

Refer to [evaluate a model](/guides/evaluate_model).

### 3. Preview changes using the `plan` command

To preview changes using `plan`, enter the `sqlmesh plan dev` command.

SQLMesh will then run the `plan` command on your `dev` environment and show you whether any downstream models are impacted by your changes. If so, SQLMesh will prompt you to classify the changes as [Breaking](/../concepts/plans#breaking-change) or [Non-Breaking](/../concepts/plans#non-breaking-change) before applying the changes.

### 4. Audit a model

To audit a model, use the `sqlmesh audit` command. For more information about audits, refer to [auditing](/concepts/audits).

**Note:** Ensure that you have already planned and applied your changes before running an audit.

Running `sqlmesh audit` produces the following example output:

```
Found 1 audit(s).
assert_positive_order_ids PASS.

Finished with 0 audit error(s).
Done.
```
