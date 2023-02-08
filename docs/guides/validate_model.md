# Validate changes to a model

## Validate changes to a model

When running the `plan` command, there are multiple ways that SQLMesh automatically validates your models in order to ensure the quality and accuracy of your data. You can also perform any of these techniques manually if you wish to directly validate a change:

* Evaluating a model
* Testing a model using unit tests
* Auditing a model
* Previewing changes using the `plan` command 

### Evaluating a model

Refer to [evaluate a model](/guides/evaluate_model).

### Testing a model using unit tests

Refer to [test a model](/guides/test_model).

### Auditing a model

Refer to [audit a model](/guides/audit_model).

### Previewing changes using the `plan` command

To preview changes using `plan`, enter the `sqlmesh plan dev` command.

SQLMesh will then run the `plan` command on your `dev` environment and show you whether any downstream models are impacted by your changes. If so, SQLMesh will prompt you to classify the changes as [Breaking](/../concepts/plans#breaking-change) or [Non-Breaking](/../concepts/plans#non-breaking-change) before applying the changes.

For more information, refer to [plans](/concepts/plans).
