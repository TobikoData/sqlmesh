# Validate changes to a model

## Automatic model validation

SQLMesh automatically validates your models in order to ensure the quality and accuracy of your data. This is done by:
* Running unit tests by default when you run the `plan` command. This ensures all changes to applied to any environment is logically validated.
* Running audits whenever data is loaded to a table (both backfill and loading on a cadence).  This way you know all data present in any table has passed defined audits.

# TODO: Add note about CI/CD bot when documentation is ready as another way SQLMesh provides automatic validation since it automatically creates the preview environment.

## Manual model validation

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

To preview changes using `plan`, enter the `sqlmesh plan <environment name>` command.

SQLMesh will then run the `plan` command on your `<environment name>` environment and show you whether any downstream models are impacted by your changes. If so, SQLMesh will prompt you to classify the changes as [Breaking](/../concepts/plans#breaking-change) or [Non-Breaking](/../concepts/plans#non-breaking-change) before applying the changes.

For more information, refer to [plans](/concepts/plans).
