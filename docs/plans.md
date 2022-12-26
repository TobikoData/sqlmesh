# Plans

Planning allows you to understand and categorize potential changes to models in a given environment.
A plan can compare your local environment with a remote environment or two remote environments. Plans show
text diffs of directly modified changes while showing you possibly affected downstream models. Finally, planning
will also allow you to control what date ranges are missing and need to be backfilled.
Based on this infromation, it's up to you to choose whether changes are [breaking](#breaking-change), [non-breaking](#non-breaking-change), or [no change](#no-change).
These categorizations determine which tables are backfilled or reused.

# Change Categories
Categorizations are only prompted for new snapshots that have been directly modified. The categorization of indirectly modified downstream models is inferred based on upstream decisions. If an indirectly modified snapshot's upstream parents have conflicting decisions, it always inherits the most severe one (breaking).

## Breaking Change
If a directly modified model is categorized as a breaking-changing, then it will be backfilled along with all children. In general, this is the safest option to choose because it guarantees all downstream dependencies pick up any logical changes. However, it is the most expensive option because backfilling takes time and resources. Choose breaking when you've changed the logic of your models which needs to propogated downstream.

## Non-Breaking Change
A directly modified model that is classified as non-breaking will backfill itself but will not backfill any of its children. This is a common option for column additions that don't affect downstream models because they aren't being used yet.

## No Change
No change means don't backfill anything but use any logic changes that occured only an ongoing basis. If other environments use this particular snapshot, they will be affected ongoing as well. It is safe to use no change for metadata changes or things that don't affect the logic of a model at all. If there are indeed logic changes that would change historical runs, they will not be reflected unless a future backfill occurs. Use no change sparingly or only when necessary for logic changes.

# Backfills
After all changes have been categorized, SQLMesh prompts you with intervals that need to be backfilled, or if there are none, then a "Logical Update" button will appear. The dates to backfill can be adjusted, this is useful in development environments where you may only need a subset of data to validate your changes. A logical update means that deploying changes to an environment is cheap because only the pointers to the tables need to be swapped out.

# Apply
Changes and backfills only take place once a plan is applied. Once applied, a plan and the snapshots associated with it are immutable.
