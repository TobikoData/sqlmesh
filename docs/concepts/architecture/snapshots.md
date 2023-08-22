# Snapshots
A snapshot is a record of a model at a given time. Along with a copy of the model, a snapshot contains everything needed to evaluate the model and render its query. This allows SQLMesh to have a consistent view of your project's history and its data as the project and its models evolve and change. Since model queries can have macros, each snapshot stores a copy of all macro definitions and global variables at the time the snapshot is taken. Additionally, snapshots store the intervals of time for which they have data.

## Fingerprinting
Snapshots have unique fingerprints that are derived from their models. SQLMesh uses these fingerprints to determine when existing tables can be reused, or whether a backfill is needed as a model's query has changed.

Because SQLMesh can understand SQL with SQLGlot, it can generate fingerprints such that superficial changes to a model, such as applying formatting to its query, will not return a new fingerprint.

## Change categories
Refer to [change categories](../plans.md#change-categories).
