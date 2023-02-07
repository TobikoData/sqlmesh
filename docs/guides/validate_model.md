# Validate changes to a model

## Validate changes to a model

Validating a model ties together multiple features in order to ensure the quality and accuracy of your data.

To validate a model, perform each of the following steps:

1. Evaluate a model
2. Test a model
3. Audit a model
4. Preview changes using the `plan` command 

### 1. Evaluate a model

Refer to [evaluate a model](/guides/evaluate_model).

### 2. Test a model

Refer to [test a model](/guides/test_model).

### 3. Audit a model

Refer to [audit a model](/guides/audit_model).

### 4. Preview changes using the `plan` command

To preview changes using `plan`, enter the `sqlmesh plan dev` command.

SQLMesh will then run the `plan` command on your `dev` environment and show you whether any downstream models are impacted by your changes. If so, SQLMesh will prompt you to classify the changes as [Breaking](/../concepts/plans#breaking-change) or [Non-Breaking](/../concepts/plans#non-breaking-change) before applying the changes.

For more information, refer to [plans](/concepts/plans).