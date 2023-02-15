# Revert a change

## Revert a change

---

Before reverting a change, ensure that you have already made a change, run `sqlmesh plan`, and applied your change.

---

To revert your change:

1. Open the model file you wish to edit in your preferred editor and undo the change you made earlier.
2. Run `sqlmesh plan` and apply your changes.
3. Observe that a logical update is made.

### Logical updates
Reverting to a previous version is a quick operation as no additional work is being done. For more information, refer to [plan application](../concepts/plans.md#plan-application) and [logical updates](../concepts/plans.md#logical-updates).

## TODO
Link to documentation on the Janitor when it is written, which will include information like how to determine the TTL for tables. This TTL determines how much time can pass before reverting is no longer possible.
